
# Circuit breaker (Phase 20)

## What problem this solves that retries (Phase 8) don't

Retries and this circuit breaker both react to failure, but at
different scopes:

- **Retries** (docs/retries.md, domain/retry_policy.py) are about
  *one task*: this specific attempt failed, wait a backoff interval,
  try this same task again, up to its own retry budget. They know
  nothing about any other task.
- **A circuit breaker** is about *one task_type, across every task of
  it this worker has run*: if the last several `send_email` attempts
  all failed, that is evidence the email provider itself is down --
  not that each task independently hit a transient blip. Once that
  evidence crosses a threshold, there's no point spending a worker
  slot, a handler-timeout window, and a full retry cycle on the
  *next* `send_email` task either; it's overwhelmingly likely to fail
  for the exact same systemic reason.

Without a circuit breaker, a downstream outage shows up as every task
of the affected type queuing up for its own full retry-and-backoff
cycle, one at a time -- slow to notice, wasteful of worker capacity
that other, unrelated task_types could have used instead, and (worse)
exactly the retry-storm shape docs/retries.md already worries about,
just reproduced at the task_type level instead of the single-task
level.

## The three states

`domain/circuit_breaker.py`'s `CircuitBreaker` is a small, pure state
machine (no I/O, no framework imports -- same reasoning as
`domain/retry_policy.py` for keeping it trivially unit-testable):

- **CLOSED**: normal operation. Every call is allowed through, and
  consecutive failures are counted; `failure_threshold` consecutive
  failures opens the circuit. Any success resets the streak -- this
  counts *consecutive* failures, not failures in a time window.
- **OPEN**: calls are rejected immediately, before the handler ever
  runs, for `open_duration_seconds`. This is the entire point of the
  breaker -- failing fast instead of waiting out a timeout or a retry
  backoff for a call that's overwhelmingly likely to fail anyway.
- **HALF_OPEN**: once the cooldown elapses, the next call is let
  through as a single trial, not a flood of them. Letting every
  queued task of that type through the instant the breaker reopens
  would recreate the exact thundering-herd problem this breaker
  exists to prevent, against a dependency that may have only just
  started recovering. A trial success closes the circuit; a trial
  failure reopens it for a fresh full cooldown.

## Where this sits in the worker

`services/worker/consumer.py`'s `MessageHandler` owns one
`CircuitBreaker` per task_type, created lazily on first use. In
`_handle()`, the breaker is checked right after the no_handler check
(nothing to protect a circuit for if there's no handler at all) but
*before* `asyncio.ensure_future(handler(payload))` -- a rejection
never invokes the handler, never starts the timeout clock, and never
occupies the semaphore slot for longer than it takes to record the
rejection.

A rejection is fed through the exact same `_handle_terminal_failure`
retry-or-dead-letter logic a real handler exception or timeout would
trigger (via a `CircuitBreakerOpenError`), rather than a separate
code path: from the retry policy's point of view, "it raised," "it
ran too long," and "it never got to run because the circuit was
open" are the same kind of transient problem, deserving the same
backoff-and-retry treatment up to the same retry budget. The one
difference: a circuit-breaker rejection passes `duration_seconds=None`
to skip the `task_duration_seconds` histogram, since recording a
0-second duration for a handler that never actually ran would
quietly inflate the fast end of that histogram with executions that
never happened.

Every real success and failure that *does* reach the handler feeds
back into the breaker via `record_success()`/`record_failure()` --
except a task cancelled by an administrator
(`asyncio.CancelledError`), which is a deliberate stop, not evidence
of anything being broken, and correctly doesn't count against the
breaker, the same way it doesn't trigger a retry either.

## Why this project keeps circuit state local to one worker process, not shared in Redis

Each worker process's breakers are its own -- there is no cluster-
wide circuit state in Redis alongside
`coordination/worker_registry.py`'s heartbeats. That means two
workers can genuinely disagree about whether `send_email` is
"currently open": one whose last few attempts happened to fail
opens its own breaker, while another that simply hasn't drawn that
task_type yet stays closed and tries it anyway. A centralized
breaker would make every worker agree the instant any one of them
trips it -- at the cost of a shared-state dependency and
coordination logic (locking, or accepting eventual consistency on
the shared counter) this phase deliberately doesn't add. Each
worker's breaker still protects *that worker's* capacity, which is
the problem this phase sets out to solve, even though detection and
recovery happen independently, worker by worker, rather than
instantly across the whole fleet. Documented here as a conscious
tradeoff, consistent with how this project documents every other
scope-of-coordination decision (see docs/load-balancing.md,
docs/worker-registry.md) rather than silently picking one.

## Observability

Phase 18's metrics gain two additions (`services/worker/metrics.py`):
`worker_circuit_breaker_state` (a gauge per task_type: 0=closed,
1=open, 2=half_open -- reusing `CircuitState`'s own declaration
order as the numeric value) and `circuit_breaker_rejections_total`
(a counter per task_type, incremented only on a fast rejection, never
on a real handler failure). Together they answer "is this task_type's
circuit open right now" and "how many calls has that saved us from
running" without needing to infer either from `tasks_processed_total`
alone.

## Verifying this phase

The `always_fail` demo handler (`task_handlers/always_fail.py`,
already present since Phase 8/9 to exercise retries and the DLQ) is
also what makes this phase observable live: submit several
`always_fail` tasks in a row and, after
`CIRCUIT_BREAKER_FAILURE_THRESHOLD` consecutive failures, watch
`worker_circuit_breaker_state{task_type="always_fail"}` flip to `1`
on `/metrics` and `circuit_breaker_rejections_total` start climbing
on the next submissions, instead of each one running the handler and
waiting out its own retry backoff.
