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

from fastapi import APIRouter, Request, Response, status
from sqlalchemy import text

from persistence.database import engine

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request, response: Response) -> dict[str, str]:
    # TODO(phase 10+): check Redis connectivity once the
    #   coordination layer exists.
    problems = []

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        problems.append("database unavailable")

    rabbitmq = getattr(request.app.state, "rabbitmq", None)
    if rabbitmq is None or not rabbitmq.is_connected:
        problems.append("rabbitmq unavailable")

    if problems:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not ready", "detail": ", ".join(problems)}
    return {"status": "ready"}
