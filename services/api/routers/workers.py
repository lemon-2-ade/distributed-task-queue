"""
Read-only view onto the worker registry (Phase 10). No business
logic here beyond translating registry rows into a response model --
see services/api/main.py's module docstring for why routers stay
this thin.
"""

from fastapi import APIRouter, Request

from services.api.schemas import WorkerResponse

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=list[WorkerResponse])
async def list_workers(request: Request) -> list[WorkerResponse]:
    registry = request.app.state.worker_registry
    workers = await registry.list_active_workers()
    return [
        WorkerResponse(
            worker_id=w["worker_id"],
            queues=w.get("queues", "").split(",") if w.get("queues") else [],
            concurrency=int(w["concurrency"]) if "concurrency" in w else 0,
            started_at=float(w["started_at"]) if "started_at" in w else 0.0,
            last_heartbeat_at=float(w["last_heartbeat_at"]) if "last_heartbeat_at" in w else 0.0,
        )
        for w in workers
    ]
