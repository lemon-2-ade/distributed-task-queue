"""
Worker-process metrics (Phase 18). See observability/metrics.py's
module docstring for the overall design and why this doesn't live in
a shared metrics module.

`tasks_processed_total`'s `outcome` label is the one piece of design
worth calling out: it's deliberately *not* the same vocabulary as
domain/states.py's TaskStatus. A worker's single handling of one
message ends in one of a fixed set of *outcomes* from this worker's
point of view -- "ran to completion successfully," "failed and was
requeued for another attempt," "failed and is now headed to the
DLQ," "was cancelled mid-flight," "a redelivery for a task already
RUNNING, discarded," "no handler registered for this task_type" --
which only partially overlaps with Task.status (a single "retried"
outcome here spans the FAILED -> RETRYING -> QUEUED sequence
services/worker/consumer.py's _handle_terminal_failure walks through
as one worker-side event). Reusing TaskStatus values directly as the
label would conflate "this is what status the row is in" with "this
is what this worker did with this message," which are related but
not the same thing -- see persistence/models.py's Task/TaskEvent for
where the former is the system of record; this metric is purely an
operational signal about worker behavior, not a second copy of state.
"""

from prometheus_client import Counter, Gauge, Histogram

from observability.metrics import TASK_DURATION_BUCKETS

tasks_processed_total = Counter(
    "tasks_processed_total",
    "Messages this worker finished handling, by how handling ended",
    ["task_type", "outcome"],
    # outcome: success | retried | dead_lettered | cancelled |
    #          duplicate_discarded | no_handler
)
task_duration_seconds = Histogram(
    "task_duration_seconds",
    "Wall-clock time a handler actually ran for, start to finish/timeout/cancel "
    "(excludes time a message spent only queued, and excludes retry backoff sleeps)",
    ["task_type"],
    buckets=TASK_DURATION_BUCKETS,
)
worker_active_tasks = Gauge(
    "worker_active_tasks",
    "Handlers this worker process is currently executing",
)

# Phase 20: circuit breaker visibility. `worker_circuit_breaker_state`
# is a per-task_type gauge, not a Counter, because a breaker's state
# at any instant is exactly what an operator wants to see on a
# dashboard (closed=0 right now, or open=1 right now) -- the
# CircuitState enum's own 0/1/2 ordering (domain/circuit_breaker.py)
# is reused directly as the numeric value rather than inventing a
# separate mapping. `circuit_breaker_rejections_total` is the
# corresponding Counter: how many calls this worker fast-rejected
# instead of running the handler at all, which is the thing this
# whole phase exists to make happen instead of a slow, per-task
# retry-and-backoff discovery of the same outage -- see
# docs/circuit-breaker.md.
worker_circuit_breaker_state = Gauge(
    "worker_circuit_breaker_state",
    "Current circuit breaker state for this task_type on this worker "
    "(0=closed, 1=open, 2=half_open)",
    ["task_type"],
)
circuit_breaker_rejections_total = Counter(
    "circuit_breaker_rejections_total",
    "Task executions this worker rejected immediately because the "
    "task_type's circuit breaker was open, instead of running the handler",
    ["task_type"],
)
