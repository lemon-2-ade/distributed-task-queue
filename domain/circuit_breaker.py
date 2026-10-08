"""
Per-task_type circuit breaker -- pure state machine, no I/O, no
framework imports, same reasoning as domain/retry_policy.py for why
this lives here and is trivially unit-testable in isolation.

## What problem this solves that retries (Phase 8) don't

Retries and this circuit breaker both react to failure, but at
different scopes, and this project builds both because neither
covers the other:

- **Retries** (domain/retry_policy.py) are about *one task*: this
  specific attempt failed, so wait a backoff interval and try this
  same task again, up to its own retry budget. They know nothing
  about any other task.
- **A circuit breaker** is about *one task_type, across every task of
  it this worker has run*: if the last several attempts at
  `send_email` all failed, that's evidence the email provider itself
  is down -- not that each individual task happened to hit a
  transient blip. Once that evidence crosses a threshold, there's no
  point burning a worker slot, a handler-timeout window, and a retry
  cycle on the *next* `send_email` task either: it's extremely likely
  to fail too, for the same systemic reason. The circuit breaker lets
  the worker reject it immediately (see services/worker/consumer.py)
  instead of discovering that the slow way, task by task.

Without this, a downstream outage turns into every `send_email` task
queuing up for its own full retry-and-backoff cycle one at a time,
which is slow to notice, wastes worker capacity that other, unrelated
task_types could have used, and (worse) is exactly the retry-storm
shape docs/retries.md already worries about, just at the task_type
level instead of the single-task level.

## The three states, and why half-open exists at all

- **CLOSED**: normal operation. Every call is allowed through;
  consecutive failures are counted, and `failure_threshold`
  consecutive failures opens the circuit.
- **OPEN**: calls are rejected immediately (no handler execution,
  see `allow_request()`) for `open_duration_seconds`. This is the
  entire point -- failing fast instead of waiting out a timeout or a
  retry backoff for a call that's overwhelmingly likely to fail
  anyway.
- **HALF_OPEN**: once `open_duration_seconds` has elapsed, the next
  call is let through as a single trial, *not* a flood of them --
  letting every queued task_type call through at once the instant
  the breaker reopens would just recreate the thundering-herd problem
  this breaker exists to prevent, against a dependency that may have
  only just barely started recovering. A trial success closes the
  circuit (back to normal); a trial failure reopens it for another
  full `open_duration_seconds`. `half_open_max_trial_calls` (almost
  always 1 in this project) bounds how many concurrent trials are
  allowed before the breaker commits to a verdict, for callers with
  enough concurrency that more than one call could arrive while
  still deciding.

## Why this project keeps the circuit's state local to one worker process

Each worker process owns its own `CircuitBreaker` per task_type (see
`services/worker/consumer.py`'s `MessageHandler`) -- there is no
shared, cluster-wide circuit state in Redis. That means two workers
can genuinely disagree about whether `send_email` is currently
"open": one worker whose last few attempts happened to fail opens
its local breaker while another worker, who simply hasn't drawn that
task_type yet, stays closed and tries it anyway. A centralized
breaker (state in Redis, like coordination/worker_registry.py's
heartbeats) would make every worker agree immediately -- at the cost
of a shared-state dependency and coordination logic this project
chooses not to add for what Phase 20 is meant to teach. This is a
deliberate, documented tradeoff (see docs/circuit-breaker.md), not an
oversight: each worker's breaker is still protecting that worker's
own capacity, which is the problem this phase actually sets out to
solve, even though it means recovery detection and failure detection
both happen independently, worker by worker, rather than instantly
cluster-wide.
"""

import time
from enum import Enum


class CircuitBreakerOpenError(Exception):
    """
    Raised (by services/worker/consumer.py, not by this module --
    CircuitBreaker itself never raises, see allow_request()'s plain
    bool return) to feed a circuit-breaker rejection through the same
    _handle_terminal_failure retry/dead-letter machinery a real
    handler exception would. Not a PermanentTaskError: the circuit
    being open right now says nothing about whether *this specific*
    task is fundamentally unrunnable, only that now is a bad time --
    so it should still consume a retry attempt and eventually dead-
    letter through the normal budget, exactly like a transient
    failure, rather than skipping retries altogether.
    """


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int,
        open_duration_seconds: float,
        half_open_max_trial_calls: int = 1,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if open_duration_seconds <= 0:
            raise ValueError("open_duration_seconds must be > 0")
        if half_open_max_trial_calls < 1:
            raise ValueError("half_open_max_trial_calls must be >= 1")

        self._failure_threshold = failure_threshold
        self._open_duration_seconds = open_duration_seconds
        self._half_open_max_trial_calls = half_open_max_trial_calls

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_trials_in_flight = 0

    @property
    def state(self) -> CircuitState:
        """
        Read-only snapshot of the *current* state, resolving an
        OPEN breaker whose cooldown has already elapsed into
        HALF_OPEN as a side effect of being asked -- there's no
        background timer driving this transition (one more process
        this project doesn't need), just a check made the moment
        something actually wants to know, inside allow_request()
        below or here.
        """
        if self._state is CircuitState.OPEN and self._cooldown_elapsed():
            self._state = CircuitState.HALF_OPEN
            self._half_open_trials_in_flight = 0
        return self._state

    def _cooldown_elapsed(self) -> bool:
        assert self._opened_at is not None
        return (time.monotonic() - self._opened_at) >= self._open_duration_seconds

    def allow_request(self) -> bool:
        """
        Call before running the handler. True means proceed (and the
        caller must later call record_success()/record_failure() with
        the outcome); False means reject immediately without running
        anything -- see services/worker/consumer.py for what the
        worker does with a rejection (the same FAILED/retry machinery
        a real handler failure would trigger, just skipping the
        handler call itself).
        """
        current = self.state  # resolves OPEN -> HALF_OPEN if the cooldown passed
        if current is CircuitState.CLOSED:
            return True
        if current is CircuitState.OPEN:
            return False
        # HALF_OPEN: let through up to half_open_max_trial_calls
        # concurrent trials, not an unbounded flood.
        if self._half_open_trials_in_flight < self._half_open_max_trial_calls:
            self._half_open_trials_in_flight += 1
            return True
        return False

    def record_success(self) -> None:
        """
        A HALF_OPEN trial succeeding is what actually closes the
        circuit -- CLOSED calls succeeding just reset the failure
        streak, since they were never in danger of tripping anything.
        """
        self._consecutive_failures = 0
        if self._state is CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._opened_at = None
            self._half_open_trials_in_flight = 0

    def record_failure(self) -> None:
        """
        A HALF_OPEN trial failing re-opens the circuit for a fresh
        full cooldown -- the dependency clearly hasn't recovered yet,
        so there's no reason to let the *next* call retry sooner than
        a CLOSED breaker's first failure would have. A CLOSED breaker
        only opens once `_consecutive_failures` reaches
        `failure_threshold`; any success in between resets the streak
        (see record_success()), so this counts *consecutive*
        failures, not failures-per-window.
        """
        if self._state is CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._half_open_trials_in_flight = 0
            return

        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
