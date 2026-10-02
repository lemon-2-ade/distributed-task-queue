"""
Cancel and full listing still belong to later phases (cancellation,
general task listing). Dead-lettered listing and manual retry land
here (Phase 9) since they're what actually exercises the DLQ this
phase builds.

Route order matters: GET /tasks/dead-lettered is registered before
GET /tasks/{task_id}, because FastAPI matches routes in declaration
order and "dead-lettered" would otherwise be parsed as a (invalid)
UUID path parameter for the dynamic route and 422 instead of hitting
this one.
"""

import uuid

from fastapi import APIRouter, HTTPException, Request, Response, status

from services.api.schemas import TaskCreateRequest, TaskEventResponse, TaskResponse
from services.api.services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _get_task_service(request: Request) -> TaskService:
    return TaskService(request.app.state.publisher, request.app.state.cancellation_broadcaster)


@router.post("", response_model=TaskResponse)
async def create_task(body: TaskCreateRequest, request: Request, response: Response) -> TaskResponse:
    """
    status_code is set dynamically rather than fixed at 201: a
    request whose idempotency_key matches an existing task (Phase 13)
    returns 200 with that existing task instead of creating a new
    one, so a client can tell "this is the task you already made" (200)
    apart from "this is a brand new task" (201) -- both correct
    responses to the same request body, depending on whether this was
    a first attempt or a safe retry. See
    services/api/services/task_service.py's create_task() docstring.
    """
    service = _get_task_service(request)
    task, was_created = await service.create_task(
        task_type=body.task_type,
        payload=body.payload,
        priority=body.priority,
        max_retries=body.max_retries,
        timeout=body.timeout,
        idempotency_key=body.idempotency_key,
    )
    response.status_code = status.HTTP_201_CREATED if was_created else status.HTTP_200_OK
    return TaskResponse.model_validate(task)


@router.get("/dead-lettered", response_model=list[TaskResponse])
async def list_dead_lettered_tasks(
    request: Request, limit: int = 50, offset: int = 0
) -> list[TaskResponse]:
    service = _get_task_service(request)
    tasks = await service.list_dead_lettered(limit=limit, offset=offset)
    return [TaskResponse.model_validate(t) for t in tasks]


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, request: Request) -> TaskResponse:
    service = _get_task_service(request)
    task = await service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return TaskResponse.model_validate(task)


@router.get("/{task_id}/events", response_model=list[TaskEventResponse])
async def get_task_events(task_id: uuid.UUID, request: Request) -> list[TaskEventResponse]:
    service = _get_task_service(request)
    events = await service.get_events(task_id)
    if events is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return [TaskEventResponse.model_validate(e) for e in events]


@router.post("/{task_id}/retry", response_model=TaskResponse)
async def retry_task(task_id: uuid.UUID, request: Request) -> TaskResponse:
    service = _get_task_service(request)
    try:
        task = await service.retry_dead_lettered_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return TaskResponse.model_validate(task)


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(task_id: uuid.UUID, request: Request) -> TaskResponse:
    """
    For a PENDING/QUEUED task, the response already reflects
    status="CANCELLED" -- it happened synchronously. For a RUNNING
    task, the response still shows status="RUNNING": a cancellation
    request has been sent to the owning worker, but this call does
    not wait for (or guarantee) it actually taking effect. Poll
    GET /tasks/{task_id} to see when it actually lands. See
    TaskService.cancel_task()'s docstring and
    docs/timeouts-and-cancellation.md for why this isn't synchronous
    for a RUNNING task.
    """
    service = _get_task_service(request)
    try:
        task = await service.cancel_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return TaskResponse.model_validate(task)
