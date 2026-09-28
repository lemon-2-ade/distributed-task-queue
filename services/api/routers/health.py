"""
Liveness and readiness endpoints.

These two endpoints answer different questions, and conflating them
is a common source of bad Kubernetes/Docker healthcheck behavior:

- /health (liveness): "is this process alive and able to respond at
  all?" It should do the absolute minimum amount of work. If it ever
  starts checking downstream dependencies, a temporary Postgres blip
  would cause the orchestrator to kill and restart a perfectly
  healthy API process -- which does nothing to fix the database and
  just adds churn.

- /ready (readiness): "is this process able to serve real traffic
  right now?" This is allowed to check dependencies, because the
  correct response to "Postgres is unreachable" is "stop routing
  traffic here until it recovers," not "kill the process."

No business logic belongs in a router -- see services/api/main.py's
module docstring for why. These handlers stay this small on purpose.
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> dict[str, str]:
    # TODO(phase 3+): check PostgreSQL connectivity once the
    #   persistence layer exists.
    # TODO(phase 4+): check RabbitMQ connectivity once the
    #   messaging layer exists.
    # TODO(phase 10+): check Redis connectivity once the
    #   coordination layer exists.
    # Until those dependencies exist, readiness and liveness are the
    # same thing: the process is up.
    return {"status": "ready"}
