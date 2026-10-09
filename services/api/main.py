"""
FastAPI application factory.

Routes stay thin (see routers/health.py). As real endpoints are
added from Phase 3 onward (POST /tasks, etc.), the pattern is:
route handler -> service-layer function -> repository. The route's
only job is translating HTTP <-> Pydantic schemas; the service layer
holds the actual business logic (state transitions, idempotency
checks, publishing to RabbitMQ, ...). This keeps the business logic
testable without spinning up an HTTP client, and keeps a single use
case (e.g. "cancel a task") reachable from both the API and, later,
the CLI.

create_app() is a factory (rather than a bare module-level `app`)
so that tests can construct an isolated app instance with different
settings/dependency overrides instead of importing a singleton that
already has a real config baked in.

The lifespan context manager owns anything with real connection
setup/teardown (currently: RabbitMQ, Redis). It runs once per
process, around the whole app's lifetime -- not per-request -- which
is the same reasoning as persistence/database.py's single
module-level engine: a connection is expensive to establish and
meant to be reused, not opened and closed per call.

Phase 10 adds a WorkerRegistry, read-only from the API's side (only
workers themselves register/heartbeat/deregister -- see
services/worker/main.py) so GET /workers and /ready can both use it.

Phase 11 adds the load balancing strategy instances backing
GET /workers/select. RoundRobinStrategy is constructed once here
(not per-request) specifically because it's stateful -- see
coordination/load_balancer.py.

Phase 12 adds a CancellationBroadcaster, used by
POST /tasks/{id}/cancel to ask a RUNNING task's owning worker to stop
it -- see coordination/cancellation.py and
services/api/services/task_service.py's cancel_task().

Phase 14 adds a RateLimiter, checked (along with queue-depth
backpressure, messaging/backpressure.py) at the top of
POST /tasks -- see services/api/routers/tasks.py and
docs/rate-limiting-and-backpressure.md.

Phase 17 removes TaskPublisher from here: TaskService no longer
publishes directly (see its module docstring) -- it writes to the
transactional outbox instead, which services/outbox_relay/ drains.
The API still owns a RabbitMQConnection (not a TaskPublisher) purely
for /ready's connectivity check and POST /tasks's backpressure check
(messaging/backpressure.py's queue-depth lookup), neither of which
involves publishing a message.

Phase 19 instruments this app with FastAPIInstrumentor, the one piece
of OpenTelemetry *auto*-instrumentation this project uses (see
observability/tracing.py's module docstring for why asyncpg/aio-pika
aren't also auto-instrumented): it gives every inbound HTTP request
its own span for free, which is what lets
services/api/services/task_service.py capture a meaningful trace
context onto the outbox row without this module having to open a
span by hand for every route.
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import make_asgi_app

from config import get_settings
from coordination.cancellation import CancellationBroadcaster
from coordination.load_balancer import LeastLoadedStrategy, RoundRobinStrategy
from coordination.rate_limiter import RateLimiter
from coordination.worker_registry import WorkerRegistry
from messaging.connection import RabbitMQConnection
from observability.tracing import setup_tracing
from services.api.auth import require_api_key
from services.api.middleware import MaxBodySizeMiddleware
from services.api.metrics import (
    http_request_duration_seconds,
    http_requests_total,
    poll_external_gauges,
)
from services.api.routers import health, tasks, workers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    app.state.rabbitmq = rabbitmq

    app.state.worker_registry = WorkerRegistry()
    app.state.cancellation_broadcaster = CancellationBroadcaster()
    app.state.rate_limiter = RateLimiter()
    # RoundRobinStrategy is stateful (an internal counter) and must
    # be a single long-lived instance shared across requests, or
    # every call would reset to "pick the first worker" -- see
    # coordination/load_balancer.py. LeastLoadedStrategy is stateless
    # but kept here too, for one consistent lookup table.
    app.state.load_balancer_strategies = {
        "round_robin": RoundRobinStrategy(),
        "least_loaded": LeastLoadedStrategy(),
    }

    # Phase 18: refreshes the gauges that reflect state living
    # outside this process (queue depth, outbox lag, dead-lettered
    # count) on a timer -- see services/api/metrics.py's
    # poll_external_gauges docstring for why this can't just be
    # computed inline at scrape time.
    settings = get_settings()
    metrics_stop_event = asyncio.Event()
    metrics_poll_task = asyncio.create_task(
        poll_external_gauges(app, settings.metrics_poll_interval_seconds, metrics_stop_event)
    )

    try:
        yield
    finally:
        metrics_stop_event.set()
        await metrics_poll_task
        await rabbitmq.close()
        await app.state.worker_registry.close()
        await app.state.cancellation_broadcaster.close()
        await app.state.rate_limiter.close()


def create_app() -> FastAPI:
    settings = get_settings()

    # Phase 23: the one check this phase adds that isn't a FastAPI
    # dependency -- a loud, process-startup warning (not a hard
    # failure; see docs/security.md for why this stays a warning
    # rather than refusing to start) when a non-development
    # environment is still running the placeholder key from
    # .env.example, which is as good as no authentication at all
    # since anyone can read that file on GitHub.
    if settings.environment != "development" and settings.api_key == "change-me":
        logger.warning(
            "API_KEY is still the default placeholder value in a %r environment -- "
            "every request to /tasks and /workers is effectively unauthenticated. "
            "Set a real secret via the API_KEY environment variable.",
            settings.environment,
        )

    setup_tracing(
        "api",
        otel_exporter_otlp_endpoint=settings.otel_exporter_otlp_endpoint,
        otel_traces_enabled=settings.otel_traces_enabled,
    )

    app = FastAPI(
        title="Distributed Task Queue API",
        version="0.1.0",
        description=(
            "API gateway for a from-scratch distributed task queue "
            "and job execution platform."
        ),
        lifespan=lifespan,
    )

    app.state.settings = settings
    FastAPIInstrumentor.instrument_app(app)

    @app.middleware("http")
    async def _record_http_metrics(request: Request, call_next):
        """
        `request.url.path` (not `request.scope["route"].path`) is
        used as the label on purpose, despite the well-known
        cardinality risk of raw paths (a real UUID per task_id would
        blow up the metric's label cardinality over time) --
        deliberately accepted here rather than solved, because
        solving it means resolving the *matched route template*
        (`/tasks/{task_id}`) before the label is recorded, which
        needs Starlette internals this middleware form doesn't have
        easy access to. A real production deployment would want that
        fix; this project documents the gap instead of leaving it
        silently unaddressed. See docs/metrics.md.
        """
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start
        path = request.url.path
        http_requests_total.labels(
            method=request.method, path=path, status=response.status_code
        ).inc()
        http_request_duration_seconds.labels(method=request.method, path=path).observe(duration)
        return response

    # Phase 23: Starlette builds its middleware stack in
    # reverse-of-registration order (the *last* middleware
    # registered ends up *outermost*, wrapping every middleware
    # registered before it) -- registering this after
    # _record_http_metrics above, rather than before, is what makes
    # the body-size guard the outermost thing an oversized request
    # hits, ahead of the metrics middleware and FastAPIInstrumentor,
    # instead of (uselessly) behind them.
    app.add_middleware(MaxBodySizeMiddleware, max_body_bytes=settings.max_request_body_bytes)

    # Mounted, not a route: prometheus_client's generate_latest()
    # output isn't a Pydantic model FastAPI would know how to
    # serialize, and this ASGI app already handles content-type and
    # encoding correctly on its own -- a hand-written route would
    # just be re-implementing what make_asgi_app() already does.
    app.mount("/metrics", make_asgi_app())

    app.include_router(health.router)
    # Phase 23: /tasks and /workers require a valid X-API-Key header
    # (services/api/auth.py); /health, /ready, and the mounted
    # /metrics app above stay open -- see auth.py's module docstring
    # for why.
    app.include_router(tasks.router, dependencies=[Depends(require_api_key)])
    app.include_router(workers.router, dependencies=[Depends(require_api_key)])

    return app


app = create_app()
