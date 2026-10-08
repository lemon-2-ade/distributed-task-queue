"""Scheduler-process metrics (Phase 18). See observability/metrics.py's
module docstring for the overall design."""

from prometheus_client import Counter, Histogram

scheduler_dispatched_total = Counter(
    "scheduler_dispatched_total",
    "PENDING tasks this scheduler replica has claimed and dispatched (QUEUED + outbox row)",
)
scheduler_poll_duration_seconds = Histogram(
    "scheduler_poll_duration_seconds",
    "Time one claim_and_dispatch_due_tasks() poll cycle took, including its Postgres transaction",
)
