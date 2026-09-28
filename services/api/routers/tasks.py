"""
Only two endpoints this phase: create and read-by-id. Cancel,
retry, listing, dead-lettered, and events all belong to phases that
haven't landed yet (cancellation, DLQ, event history) -- adding
their routes now would mean routes that accept requests but don't
actually do the thing they claim to.
"""

import uuid

from fastapi import APIRouter, HTTPException, Request, status

from services.api.schemas import TaskCreateRequest, TaskResponse
from services.api.services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _get_task_service(request: Request) -> TaskService:
    return TaskService(request.app.state.publisher)


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(body: TaskCreateRequest, request: Request) -> TaskResponse:
    service = _get_task_service(request)
    task = await service.create_task(
        task_type=body.task_type,
        payload=body.payload,
        priority=body.priority,
        max_retries=body.max_retries,
        timeout=body.timeout,
        idempotency_key=body.idempotency_key,
    )
    return TaskResponse.model_validate(task)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, request: Request) -> TaskResponse:
    service = _get_task_service(request)
    task = await service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return TaskResponse.model_validate(task)
