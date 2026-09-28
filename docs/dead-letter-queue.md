# Dead-letter queue

## What

When a task's failures exhaust its retry budget (or a handler raises
`PermanentTaskError`, meaning no amount of retrying will help), the
task's message stops being requeued and instead lands in a separate
queue -- `dead_letter.queue` -- reserved for exactly this: work the
system gave up on automatically, parked somewhere a human can look at
it, and optionally decide to retry manually.

`Task.status` becomes `DEAD_LETTERED`. It is treated as terminal for
the automatic system (no automatic transition leaves it), with one
deliberate exception: an administrator can move it back to `QUEUED`
via `POST /tasks/{id}/retry`.

## Why

Without a DLQ, an unretryable task's message would either requeue
forever (a poison-pill message that a broken handler can never
process, looping indefinitely and starving other work) or simply
vanish on nack with no trace. Neither is acceptable: the first wastes
capacity, the second loses information about what failed and why.
Routing to a dedicated queue, backed by a `Task` row whose full
`task_events` history already records every attempt, gives a
durable, inspectable record and a deliberate, explicit path back into
the system instead of silent data loss or an infinite loop.

## How it works

1. Phase 4's topology already declares this: every priority queue
   has `x-dead-letter-exchange` / `x-dead-letter-routing-key`
   arguments pointing at `dead_letter.exchange` / `dead_letter`, and
   `dead_letter.queue` is bound to that exchange. This is RabbitMQ's
   own dead-lettering mechanism, not application code -- any message
   nacked with `requeue=False` on a priority queue is automatically
   routed there by the broker.
2. Phase 8's `_handle_failure` (in `services/worker/consumer.py`)
   nacks with `requeue=False` when a failure is permanent or retries
   are exhausted. At that point the task's `Task.status` is `FAILED`
   -- nothing has told Postgres the message is now in the DLQ yet.
3. Phase 9 adds `make_dlq_handler()`, a consumer every worker replica
   runs against `dead_letter.queue` alongside its three priority-queue
   consumers (see `services/worker/main.py`). This is the *only*
   thing that transitions a task to `DEAD_LETTERED`.

## Why a separate consumer instead of marking DEAD_LETTERED at nack time

`_handle_failure` could mark the task `DEAD_LETTERED` itself, at the
moment it decides to nack. That would be assuming the nack succeeds
in routing the message to the DLQ without ever checking. Consuming
the real `dead_letter.queue` instead means `Task.status` reflects
where the message actually ended up in RabbitMQ, not just what one
worker intended to happen to it.

## The eventual-consistency window

Between "the worker nacked the message" and "the DLQ consumer has
processed it," the task sits at `FAILED` even though the message is
already unrecoverable via the normal retry path. This window is
usually milliseconds, but it is real: a `GET /tasks/{id}` in that
window shows `FAILED`, not `DEAD_LETTERED`, even though no worker
will ever pick this task back up. `GET /tasks/dead-lettered` will not
list it until the DLQ consumer catches up.

## The manual-retry race

`POST /tasks/{id}/retry` requires the task to currently be
`DEAD_LETTERED` (state_manager enforces this: `DEAD_LETTERED ->
QUEUED` is the only outgoing edge). Because of the eventual-consistency
window above, it's possible for a stale DLQ message to arrive *after*
an administrator has already retried the task and it has moved on
(back to `QUEUED`, then `RUNNING`, maybe even `SUCCESS`). When that
happens, `make_dlq_handler` calls `state_manager.transition(...,
DEAD_LETTERED)` against a task that is no longer `FAILED`, which
raises `InvalidStateTransitionError`. The handler catches this (along
with `ValueError` for a missing task, which shouldn't happen but
isn't worth crashing over) and simply acks the stale message without
changing anything. The task's real current status is correct without
it; the DLQ message describing a since-superseded failure is discarded.

## Manual retry semantics

`POST /tasks/{id}/retry`:
- 404 if the task doesn't exist.
- 409 if the task exists but isn't currently `DEAD_LETTERED`.
- On success: `retry_count` resets to 0 (a fresh manual attempt gets
  the full retry budget again, not whatever was left when it was
  dead-lettered), the task transitions `DEAD_LETTERED -> QUEUED`, and
  a new message is published to the task exchange -- same
  publish-after-commit ordering as every other transition in this
  codebase.

`task_handlers/always_fail.py` exists purely so this can be
demonstrated live: it always raises, so a task submitted with
`task_type="always_fail"` and a small `max_retries` reliably cycles
through `RETRYING` and lands in `DEAD_LETTERED` without needing to
contrive a real failure.
