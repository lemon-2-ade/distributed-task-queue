# Idempotency

Two different problems share the word "idempotency" in this project,
and they're solved in two different places, because they're actually
about two different kinds of duplication.

## Problem 1: the client retries the *request*

A client calls `POST /tasks`, the connection drops before the
response arrives, and the client -- reasonably, not knowing whether
the task was created or not -- retries the exact same request. Without
anything to detect this, that's two tasks created from one intended
submission.

### How

`Task.idempotency_key` (an optional, client-supplied string, unique
in the database since Phase 3) is the client's way of saying "this
request and any retry of it are the same logical submission."
`TaskService.create_task()` looks the key up before creating
anything; if a task with that key already exists, it's returned
as-is -- no new row, no new publish to RabbitMQ. `POST /tasks` reports
this to the caller by returning `200 OK` with the existing task
instead of `201 Created` with a new one, so a client watching status
codes (not just comparing bodies) can tell "this is the task you
already made" from "this is a brand new task."

### The race this has to handle

A lookup-then-create has an obvious race: two concurrent requests
with the same `idempotency_key` can both pass the "does this already
exist" check before either has committed its insert. Relying on the
lookup alone would let both through. The actual safety net is the
database's own unique constraint on `idempotency_key` -- one of the
two inserts wins, the other fails with an integrity-constraint
violation. `create_task()` catches exactly that, rolls back its own
half-finished insert, and re-fetches by `idempotency_key` to hand
back whichever row actually won -- so both concurrent callers end up
with the same task either way, which is the whole point of an
idempotency key. The upfront lookup isn't redundant with this: it's
what makes the *common* case (a genuine retry, not a true race) avoid
even attempting a doomed insert.

### Scope

This only protects task *creation*. It says nothing about whether the
task's handler runs once or more than once once it's QUEUED -- that's
Problem 2.

## Problem 2: RabbitMQ redelivers a *message*

`docs/rabbitmq.md`'s "ACK timing" section lays out the mechanism: a
worker can crash after it's done real work (or even after writing a
terminal status to Postgres) but before its `ack()` reaches RabbitMQ.
The broker, having never heard "done," redelivers the message to
another consumer (or the same one, if it restarts). That's
at-least-once delivery working as designed -- the cost of "a crash
never loses a task" is "a task might run more than once."

### How this project narrows the risk (not eliminates it)

`services/worker/consumer.py` already asks the state machine
`_transition(task_id, RUNNING, ...)` before running anything, and
`domain/states/transitions.py` only allows `QUEUED -> RUNNING` (and
`PENDING -> RUNNING` isn't even defined -- only via QUEUED). This
means a redelivered message for a task that's already reached any
*other* status gets caught as an `InvalidStateTransitionError`, which
splits into exactly two cases:

- **The task reached a real terminal status** (`SUCCESS`, `FAILED`
  with retries exhausted, `CANCELLED`, `TIMEOUT` exhausted,
  `DEAD_LETTERED`): the transition attempt fails, the redelivered
  message is acked without running the handler again, and nothing is
  lost -- the *first* attempt's result already stands. This covers
  the specific crash-after-SUCCESS-before-ack scenario the "ACK
  timing" doc section describes as the primary risk, and closes it
  completely: a handler that already finished successfully never
  runs again because of a redelivery.
- **The task is already RUNNING** (`exc.from_status ==
  TaskStatus.RUNNING`): genuinely ambiguous, and this project is
  honest about not fully solving it. A redelivery arriving while the
  task is still RUNNING could mean the original attempt is still
  alive and about to finish (re-running now would be true double
  execution), or that the original worker actually died mid-task with
  no automatic reclaim (a real gap, also noted in
  `docs/graceful-shutdown.md`'s failure modes) and this redelivery is
  the *only* remaining chance to make progress on it. This project has
  no fencing token, lease, or heartbeat-per-task mechanism to tell
  those two situations apart -- that's real distributed-systems
  machinery this portfolio project doesn't build. Rather than guess,
  it takes the side that can't silently duplicate side effects: the
  redelivery is discarded (acked, not re-run), and a
  `DUPLICATE_DELIVERY_DETECTED` event is recorded
  (`persistence/state_manager.py`'s `record_duplicate_delivery()`) so
  it's visible in `GET /tasks/{id}/events` rather than silently
  disappearing. If the original attempt really did die, the task is
  left stuck at RUNNING -- visibly so, and recoverable through the
  `POST /tasks/{id}/cancel` endpoint Phase 12 already built, which is
  explicitly allowed from RUNNING. That's a manual recovery path, not
  an automatic one; a production system would want the automatic
  version (worker heartbeats per in-flight task, not just per worker,
  feeding a reclaim process), which is out of scope here.

### Why task handlers should still be written idempotently

Even with the above, this project's retry logic (Phase 8) itself
*intentionally* re-runs a handler on transient failure -- that's
retries working as designed, not a bug. Combined with the redelivery
risk above, the practical guidance for any task handler in this
system (see `task_handlers/`) is the same advice real queue systems
give: a handler whose side effects aren't safe to repeat (charging a
payment twice, sending a duplicate email) needs its own
application-level dedup (e.g. a request ID the downstream system
itself deduplicates on), because no amount of queue-level engineering
makes "exactly once" a honest guarantee over a network that can fail
at any point between "did the work" and "recorded that fact."
