# Retries, exponential backoff, and jitter

## Configurable retries

Every task carries its own `max_retries` (default 3, set at
submission time). `retry_count` starts at 0 and is incremented by
`TaskStateManager` each time a transient failure is retried. A task
gets `max_retries + 1` total attempts: the first try, plus up to
`max_retries` retries.

## Exponential backoff

`domain/retry_policy.py`:

```
attempt 1 -> base_delay * 2^0 = base_delay
attempt 2 -> base_delay * 2^1
attempt 3 -> base_delay * 2^2
attempt 4 -> base_delay * 2^3
```

capped at `RETRY_MAX_DELAY_SECONDS`, so a task with a generous
`max_retries` doesn't end up waiting hours between attempts. The
idea: if something failed, waiting briefly and trying again often
succeeds (a transient network blip, a downstream service mid-restart)
-- and waiting *longer* on each subsequent failure gives a struggling
dependency more room to recover instead of hammering it at a fixed
interval.

## Jitter, and why retry storms happen without it

Take pure exponential backoff without randomization: if a downstream
dependency goes down and 500 in-flight tasks all fail at
approximately the same moment, they'd all also *retry* at
approximately the same moment (same formula, same inputs, same
delay). That's a **retry storm**: the retry traffic itself arrives as
a synchronized spike, which can be worse than the original load --
especially right as the dependency is coming back up, where a
thundering herd can knock it back down.

`RETRY_JITTER_FRACTION` (default 0.2, i.e. +/-20%) randomizes the
computed delay so 500 simultaneously-failed tasks spread their
retries across a window instead of a single instant. This is the
same principle AWS's and Google's SRE literature call "jittered
backoff," and it's why this project doesn't treat jitter as optional
polish -- without it, backoff's exponential growth only delays the
thundering herd, it doesn't prevent one.

## Transient vs. permanent failures

A task handler can raise two different kinds of thing:

- A **`PermanentTaskError`**: "this will never succeed, no matter how
  many times you retry it" -- a malformed payload, a business rule
  that rejects the request outright. Retrying a permanent failure
  wastes time and (per the backoff schedule) can wait quite a while
  before finding out it still doesn't work. The worker treats this
  as non-retryable immediately, regardless of remaining
  `max_retries`.
- **Any other exception**: assumed transient (a timeout, a dropped
  connection, a dependency that's temporarily unavailable) and
  retried up to `max_retries`.

This is an opt-in distinction, not a guess: task handlers decide
which bucket a failure belongs in by choosing which exception to
raise. A handler that never raises `PermanentTaskError` is saying
"every failure I produce is worth retrying," which is a reasonable
default but not always correct -- handler authors are expected to
raise `PermanentTaskError` for cases they know are hopeless.

## What actually happens on a transient failure

```
RUNNING --(handler raises)--> FAILED --(retryable)--> RETRYING --> QUEUED
                                 |
                                 +--(not retryable)--> stays FAILED,
                                                        message -> DLQ
```

Concretely, in `services/worker/consumer.py`:
1. `RUNNING -> FAILED`: records the failed attempt (closes its
   `task_attempts` row, appends a `TASK_FAILED` event).
2. If retryable: `FAILED -> RETRYING` (increments `retry_count`,
   appends `TASK_RETRYING`), then this worker `asyncio.sleep()`s for
   the jittered backoff delay, then `RETRYING -> QUEUED` and a *new*
   message is published for the next attempt. Only then is the
   *original* message acked.
3. If not retryable (permanent error, or `retry_count >=
   max_retries`): stays `FAILED`, original message is nacked with
   `requeue=False` -- which, via the dead-letter-exchange argument
   on every priority queue (Phase 4), routes it to the DLQ.
   Nothing marks the task `DEAD_LETTERED` yet; that's Phase 9.

## A deliberate tradeoff: where the backoff wait happens

The `asyncio.sleep()` for backoff happens *inside* the worker's
message handler, holding both the Phase 6 concurrency semaphore and
the original (unacked) message for the whole wait. A retrying task
therefore occupies one of the worker's `WORKER_CONCURRENCY` slots
until its backoff elapses, rather than freeing that capacity up
immediately for other work.

The alternative -- a broker-side delay, e.g. publishing the retry to
a holding queue with a per-message TTL and a dead-letter-exchange
that routes expired messages back to `task.exchange` -- avoids
tying up worker capacity, but core RabbitMQ (without the optional
delayed-message-exchange plugin) only expires messages from a
queue's *head*, so a queue holding retries with different TTLs
(different attempt numbers, different jittered delays) doesn't
expire them in the order their TTLs would suggest -- a message with
a long TTL sitting at the head can delay a shorter-TTL message
behind it. That's a real, easy-to-get-subtly-wrong mechanism.

This project takes the simpler option -- an in-process sleep -- and
names the cost explicitly rather than building (and half-explaining)
a delay-queue mechanism. It's a legitimate choice for a system where
retries are the exception, not the common case; a production system
handling much higher retry volume would likely need the broker-side
approach, or the delayed-message-exchange plugin, or a dedicated
scheduler-style component (similar to Phase 16's scheduler) polling
for tasks whose backoff has elapsed.
