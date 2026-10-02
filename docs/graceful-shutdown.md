# Graceful shutdown

## What

`services/worker/main.py`'s response to `SIGTERM`/`SIGINT`, from
Phase 12 on:

1. Cancel every AMQP consumer tag (`queue.cancel(...)`) -- RabbitMQ
   stops delivering *new* messages to this worker immediately.
2. Call `MessageHandler.wait_for_drain(WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS)`
   and wait for whatever's currently in flight to finish on its own.
3. Only then: stop the heartbeat loop and cancellation listener,
   deregister from the worker registry, close the RabbitMQ and Redis
   connections.

## Why this ordering, and why it matters

Before Phase 12, shutdown cancelled the consumers and closed the
RabbitMQ connection back-to-back, with no step in between. That's a
real bug, not just an incompleteness: if a handler was still
executing when the connection closed, its eventual `ack()`/`nack()`
call would fail (or never even be reached) against a connection
that's already gone. RabbitMQ's own behavior when a connection drops
with unacked messages outstanding is to redeliver them to another
consumer -- which sounds like a safety net, but means a task that
*did* complete successfully right as the connection closed could get
re-delivered and re-run anyway, because the ack that would have told
RabbitMQ "this one's done" never arrived. Waiting for in-flight work
to finish *before* touching the connection is what avoids both the
silent-loss and the spurious-redelivery versions of this problem.

## Why there's a grace period instead of waiting forever

An orchestrator (Docker, Kubernetes, `docker compose stop`) that
sends `SIGTERM` will, after its own timeout, send `SIGKILL` if the
process hasn't exited -- a signal no process can catch or delay.
Waiting indefinitely for in-flight work doesn't actually guarantee a
clean finish; it just means the *eventual* kill is an un-caught,
un-cleaned-up `SIGKILL` instead of a `SIGTERM` this process had any
chance to react to. A bounded grace period
(`WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS`, default 30s) is this
process choosing to give up on its own terms, at a known point, with
a log line saying so -- rather than being surprised by the
orchestrator's kill.

## What happens to work still running after the grace period

Deliberately **not** force-cancelled. Cancelling a handler
mid-execution at this point would abandon whatever side effects it
had already started, with no chance to record even a `FAILED` or
`TIMEOUT` status -- the very connections needed to write that status
are about to be closed anyway. Instead, `main.py` just logs how many
handlers were still running and proceeds with shutdown. Their
messages stay unacked; RabbitMQ's redelivery-on-connection-loss
behavior is what eventually gets them picked up again (by this
worker if it restarts, or by another replica) -- not a graceful
resolution, exactly, but a safe one: "redelivered and possibly
re-run" is a known, already-documented consequence of this project's
at-least-once delivery model (`docs/rabbitmq.md`), not a new failure
mode introduced here. A task whose handler has non-idempotent side
effects could run twice because of this -- the same caveat idempotency
work (Phase 13) exists to close, not something this phase claims to
solve.

## Why draining is based on the *outer* handle() task, not just the handler

`MessageHandler.in_flight` tracks every `handle()` invocation from
the moment it starts (before even acquiring the concurrency
semaphore) to the moment it returns -- not just the narrower window
where the actual task handler coroutine is running
(`running_tasks`, used for cancellation instead). Draining on the
narrower window would let shutdown proceed while a handler had
already finished but the wrap-up work (the final state transition,
the `ack()`/`nack()` call) was still in progress -- exactly the kind
of "connection closes mid-write" gap this feature exists to close in
the first place. Using the outer task means shutdown genuinely waits
for every message this worker has touched to reach a real, final
disposition.

## Failure modes

- **The process is killed outright (`SIGKILL`, OOM-kill, `docker
  compose kill`)**: none of this runs at all -- there's no signal to
  catch. Whatever was in flight is abandoned exactly as described
  above, immediately rather than after a grace period. This is
  exactly the scenario Phase 10's worker registry TTL exists to
  detect from the outside (`docs/worker-registry.md`): a hard-killed
  worker's registry entry simply expires, which is a different
  signal than a task the worker was actually running -- those two
  facts aren't yet connected to each other (no component currently
  reclaims a dead worker's RUNNING tasks); that remains a known gap
  this project doesn't close until something explicitly does.
- **Grace period is set shorter than realistic task durations**:
  shutdown will routinely "succeed" while abandoning work, which
  looks the same operationally as a crash even though it technically
  went through this orderly path. The default (30s) is a guess, not
  a measured value -- a real deployment would tune this against
  actual task-duration data.
