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
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import get_settings
from coordination.worker_registry import WorkerRegistry
from messaging.connection import RabbitMQConnection
from messaging.publisher import TaskPublisher
from services.api.routers import health, tasks, workers


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    app.state.rabbitmq = rabbitmq
    app.state.publisher = TaskPublisher(rabbitmq.task_exchange)

    app.state.worker_registry = WorkerRegistry()
    try:
        yield
    finally:
        await rabbitmq.close()
        await app.state.worker_registry.close()


def create_app() -> FastAPI:
    settings = get_settings()

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

    app.include_router(health.router)
    app.include_router(tasks.router)
    app.include_router(workers.router)

    return app


app = create_app()
