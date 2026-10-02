# Task timeouts and cancellation

## Timeouts

### What

`Task.timeout` (seconds, optional, set at creation -- the column has
existed since Phase 3) now actually does something: if set, a
handler that runs longer than that is stopped and the task is
transitioned to `TIMEOUT` instead of being left to run forever.

### Why

Without this, a handler that hangs -- a downstream HTTP call with no
timeout of its own, a deadlock, an infinite loop in a buggy task
type -- would occupy one of a worker's `WORKER_CONCURRENCY` slots
indefinitely. On a worker with a handful of slots, a few hung tasks
is enough to stop that worker from making any other progress at all,
with nothing in the system noticing or correcting it. A task-level
timeout is the system imposing its own upper bound on "how long is
too long," independent of whether the handler code itself is well
behaved.

### How

The actual `handler(payload)` call is wrapped in its own
`asyncio.Task` and awaited through `asyncio.wait_for(handler_task,
timeout=task.timeout)`. If the timeout fires, `asyncio.wait_for`
cancels `handler_task` for us and raises `TimeoutError`, caught in
`services/worker/consumer.py` and routed through
`_handle_terminal_failure` with `failure_status=TIMEOUT` -- the exact
same retry-or-dead-letter logic Phase 8 built for ordinary handler
exceptions (`domain/states/transitions.py` gives `TIMEOUT` and
`FAILED` identical outgoing edges, `-> RETRYING` or
`-> DEAD_LETTERED`, specifically so this sharing is correct and not
just convenient).

### Failure modes

- **The handler doesn't actually stop when cancelled**: cancelling an
  `asyncio.Task` only raises `asyncio.CancelledError` at its next
  `await` point. A handler stuck in tight synchronous CPU-bound code
  with no `await` in it (a true infinite loop, not an unbounded
  network wait) won't actually yield back until it finishes on its
  own -- `asyncio.wait_for` will have already raised `TimeoutError`
  to the caller, so the *task* is marked TIMEOUT and retried/dead-
  lettered, but the orphaned coroutine keeps burning CPU on this
  worker in the background, invisible to the registry's
  `active_task_count` (which was decremented already) and to
  anything else in this system. This project's task handlers are all
  well-behaved `async` code with real `await` points (`echo`,
  `sleep`, `always_fail`), so this gap is real but not exercised by
  anything shipped here; a production system would need a harder
  backstop (a subprocess per task, with a real OS-level kill) to
  close it completely.
- **The timeout fires right as the handler was about to succeed**:
  a genuine race with no clean answer -- the handler's result is
  simply discarded once cancelled, even if it was one `await` away
  from returning. This is why timeouts should be set generously
  relative to a task's expected duration, not tuned to the exact
  common case.

## Cancellation

### What

`POST /tasks/{task_id}/cancel`: ask for a task to stop. Works
differently depending on where the task currently is, because "stop
it" means something different before it's started than it does mid-
execution.

### Why PENDING/QUEUED and RUNNING are handled completely differently

A `PENDING` or `QUEUED` task has no process doing anything with it
yet -- cancelling it is just a Postgres write
(`TaskStateManager.transition(..., CANCELLED)`), done synchronously
by `TaskService.cancel_task()`, no coordination needed.

A `RUNNING` task is a different problem entirely: some worker process
-- not necessarily this API process, not even necessarily on the same
machine -- has a handler actually executing right now. The API has no
direct channel to that specific process; all it has is the
`worker_id` recorded on the `Task` row when it entered RUNNING. This
is exactly the problem `coordination/cancellation.py`'s Pub/Sub
channel exists to solve: broadcast the request to every worker, let
each one check "is this naming a task I'm currently running" (its own
local `MessageHandler.running_tasks`), and only the one that actually
owns it does anything.

### Why cancelling a RUNNING task doesn't update the database immediately

`TaskService.cancel_task()` could optimistically set the status to
`CANCELLED` the moment the request comes in, the same way it does for
PENDING/QUEUED. It deliberately doesn't. Doing so would mean Postgres
says `CANCELLED` while the handler might, for a little while longer,
still be genuinely running -- and if the cancellation request never
actually reaches a live worker (see the failure modes below), it
would say `CANCELLED` forever for a task that's actually still
executing, or that already finished with a real `SUCCESS`/`FAILED`
result the status would now be hiding. The status only changes once
the owning worker's handler task is actually cancelled and
`services/worker/consumer.py` transitions it itself -- the same
principle as Phase 9's DLQ consumer (`docs/dead-letter-queue.md`):
the task's recorded status should reflect what actually happened, not
what was merely requested.

### The queued-then-cancelled race

A task can be cancelled (PENDING/QUEUED -> CANCELLED) after its
message has already been published to RabbitMQ -- there's no way to
recall a message already sitting in a queue. When a worker eventually
delivers that message and tries `_transition(task_id, RUNNING, ...)`,
the task is no longer QUEUED (it's CANCELLED, a terminal status with
no outgoing edges), so the transition raises
`InvalidStateTransitionError`. `services/worker/consumer.py` catches
this specifically and just acks the message without running anything
-- the same "stale message, safe to discard" pattern used for DLQ
race handling in Phase 9, applied here to a different race with the
same shape.

### Failure modes

- **Cancellation request for a RUNNING task whose owning worker has
  already died**: nobody is listening for it (or rather, every
  remaining worker is listening, but none of them owns this
  `task_id`), so nothing happens. The task stays RUNNING in Postgres
  indefinitely from this mechanism's point of view -- a separate
  problem (detecting that a worker died mid-task and its tasks need
  to be reclaimed) that this phase does not solve; see
  `docs/graceful-shutdown.md`'s own failure-modes section for the
  related but distinct question of what happens to in-flight work
  when a worker *does* shut down.
- **Cancellation request arrives after the handler already
  finished**: `MessageHandler.cancel_task()` checks
  `running_tasks.get(task_id)` and finds nothing (or a task that's
  already `.done()`), so it's a no-op. The task already reached
  whatever real terminal status it reached (`SUCCESS`, `FAILED`,
  etc.) and the cancellation request is simply too late -- not an
  error, just a lost race, consistent with this being explicitly
  fire-and-forget (`coordination/cancellation.py`'s module
  docstring).
- **Calling `/cancel` on a task that's already terminal** (`SUCCESS`,
  `FAILED`, `CANCELLED`, `TIMEOUT`, `DEAD_LETTERED`) or mid-retry
  (`RETRYING`): `TaskService.cancel_task()` raises `ValueError`,
  which the router turns into `409 Conflict` -- there's nothing left
  to cancel, and pretending otherwise would be misleading.
