"""Outbox-relay-process metrics (Phase 18). See observability/metrics.py's
module docstring for the overall design."""

from prometheus_client import Counter, Histogram

outbox_relayed_total = Counter(
    "outbox_relayed_total",
    "Outbox rows this relay replica has published to RabbitMQ and marked published",
)
outbox_relay_poll_duration_seconds = Histogram(
    "outbox_relay_poll_duration_seconds",
    "Time one relay_once() poll cycle took, including its publish calls and Postgres transaction "
    "(this one holds its transaction open across RabbitMQ I/O -- see relay.py's docstring, so this "
    "histogram runs noticeably longer than scheduler_poll_duration_seconds for the same reason)",
)
