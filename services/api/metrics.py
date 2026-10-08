"""
API-process metrics (Phase 18). See observability/metrics.py's
module docstring for why these live here rather than in a shared
metrics file, and for the overall Prometheus design.

Three different "shapes" of metric live in this one module, each
updated a different way -- worth being explicit about, since
conflating them is an easy mistake:

1. **Request-scoped counters/histograms** (`http_requests_total`,
   `http_request_duration_seconds`, `tasks_created_total`,
   `tasks_rejected_total`) -- updated synchronously, inline, at the
   moment the thing they count happens (a request completing, a task
   being accepted or rejected). See the metrics middleware and
   services/api/routers/tasks.py.
2. **Gauges reflecting this process's own request handling**: none
   of this module's gauges are this shape -- see the note on (3).
3. **Gauges reflecting state that lives *outside* this process**
   (`rabbitmq_queue_depth`, `outbox_unpublished_rows`,
   `dead_lettered_tasks_current`) -- these can't be updated
   inline the way (1) is, because nothing about "how many messages
   are sitting in a RabbitMQ queue right now" happens *at* a request;
   it's ambient state that changes independently of API traffic
   (other publishers, other workers, the scheduler, the outbox
   relay). A `prometheus_client` custom Collector could compute these
   at scrape time instead, but that function has to be synchronous,
   and every one of these numbers requires an async RabbitMQ or
   Postgres round trip -- so this module instead runs a background
   polling loop (`poll_external_gauges`, started from
   services/api/main.py's lifespan) that refreshes these gauges every
   `settings.metrics_poll_interval_seconds` regardless of whether
   anyone is scraping at that exact moment. The tradeoff, made
   explicit: a reader of /metrics sees a value that's at most one
   poll interval stale, not a live read -- acceptable here because
   these are dashboard/alerting numbers (trending over minutes), not
   values anything in this codebase makes a decision based on.
"""

import asyncio
import logging

from prometheus_client import Counter, Gauge, Histogram

from observability.metrics import TASK_DURATION_BUCKETS
from persistence.database import AsyncSessionLocal
from persistence.repositories.outbox_repository import OutboxRepository
from persistence.repositories.task_repository import TaskRepository

logger = logging.getLogger(__name__)

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled by the API",
    ["method", "path", "status"],
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
)

tasks_created_total = Counter(
    "tasks_created_total",
    "Tasks newly accepted via POST /tasks (excludes idempotent replays -- see task_service.create_task)",
    ["task_type", "priority"],
)
tasks_rejected_total = Counter(
    "tasks_rejected_total",
    "Requests to POST /tasks rejected before a task was created",
    ["reason"],  # "rate_limited" | "backpressure"
)

rabbitmq_queue_depth = Gauge(
    "rabbitmq_queue_depth",
    "Ready messages currently waiting in a priority queue (passive declare, see messaging/backpressure.py)",
    ["priority"],
)
outbox_unpublished_rows = Gauge(
    "outbox_unpublished_rows",
    "Rows in outbox_messages not yet published -- outbox-relay lag",
)
dead_lettered_tasks_current = Gauge(
    "dead_lettered_tasks_current",
    "Tasks currently sitting in DEAD_LETTERED status",
)


async def poll_external_gauges(app, interval: float, stop_event: asyncio.Event) -> None:
    """
    Started as a background asyncio task from services/api/main.py's
    lifespan, cancelled (via stop_event) on shutdown alongside every
    other background loop this project runs (compare
    coordination/heartbeat.py's run_heartbeat_loop, which this
    deliberately mirrors: poll, sleep up to `interval` or until
    stop_event fires, repeat). Each iteration is independently
    try/excepted so one failed poll (a transient RabbitMQ hiccup, a
    momentarily unreachable Postgres) never kills the loop -- a gauge
    just keeps its last-known value until the next successful poll,
    which is a more honest failure mode for a dashboard number than
    the whole metrics-refresh loop dying silently.
    """
    from domain.states import TaskStatus
    from messaging.queues import QUEUE_BY_PRIORITY

    while not stop_event.is_set():
        try:
            channel = app.state.rabbitmq.channel
            if channel is not None:
                for priority, queue_name in QUEUE_BY_PRIORITY.items():
                    queue = await channel.declare_queue(queue_name, passive=True)
                    rabbitmq_queue_depth.labels(priority=priority.value).set(
                        queue.declaration_result.message_count
                    )
        except Exception:
            logger.exception("metrics: failed to poll RabbitMQ queue depth")

        try:
            async with AsyncSessionLocal() as session:
                outbox_unpublished_rows.set(await OutboxRepository(session).count_unpublished())
                dead_lettered_tasks_current.set(
                    await TaskRepository(session).count_by_status(TaskStatus.DEAD_LETTERED)
                )
        except Exception:
            logger.exception("metrics: failed to poll Postgres-backed gauges")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
