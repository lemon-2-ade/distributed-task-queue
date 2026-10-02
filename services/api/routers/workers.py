"""
Read-only view onto the worker registry (Phase 10) and the
application-level load balancing strategies built on top of it
(Phase 11). No business logic here beyond translating registry rows
and strategy output into response models -- see services/api/main.py's
module docstring for why routers stay this thin.
"""

from fastapi import APIRouter, HTTPException, Query, Request, status

from services.api.schemas import WorkerResponse

router = APIRouter(prefix="/workers", tags=["workers"])


def _to_response(w: dict) -> WorkerResponse:
    return WorkerResponse(
        worker_id=w["worker_id"],
        queues=w.get("queues", "").split(",") if w.get("queues") else [],
        concurrency=int(w["concurrency"]) if "concurrency" in w else 0,
        started_at=float(w["started_at"]) if "started_at" in w else 0.0,
        last_heartbeat_at=float(w["last_heartbeat_at"]) if "last_heartbeat_at" in w else 0.0,
        active_task_count=int(w["active_task_count"]) if "active_task_count" in w else 0,
    )


@router.get("", response_model=list[WorkerResponse])
async def list_workers(request: Request) -> list[WorkerResponse]:
    registry = request.app.state.worker_registry
    workers = await registry.list_active_workers()
    return [_to_response(w) for w in workers]


@router.get("/select", response_model=WorkerResponse)
async def select_worker(
    request: Request,
    strategy: str = Query(
        "least_loaded",
        description="Which load balancing strategy to use: 'round_robin' or 'least_loaded'.",
    ),
) -> WorkerResponse:
    """
    Diagnostic/admin endpoint: which worker would this strategy pick
    right now, given the current registry snapshot? See
    coordination/load_balancer.py's module docstring for why this
    isn't (yet) wired into actual task dispatch -- RabbitMQ still
    owns that. This exists so the strategies can be inspected and
    exercised on their own.
    """
    strategies = request.app.state.load_balancer_strategies
    if strategy not in strategies:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown strategy {strategy!r}; choose one of {sorted(strategies)}",
        )

    registry = request.app.state.worker_registry
    workers = await registry.list_active_workers()
    chosen = strategies[strategy].select(workers)
    if chosen is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="no active workers to select from",
        )
    return _to_response(chosen)
