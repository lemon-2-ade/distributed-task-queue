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

from messaging.backpressure import get_total_queue_depth
from services.api.schemas import TaskCreateRequest, TaskEventResponse, TaskResponse
from services.api.services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _get_task_service(request: Request) -> TaskService:
    return TaskService(request.app.state.publisher, request.app.state.cancellation_broadcaster)


async def _check_rate_limit(request: Request) -> None:
    """Checked first -- it's the cheap, single-Redis-call guard.
    Backpressure is checked second because it costs an extra
    RabbitMQ round trip (a passive queue declare per priority queue),
    not worth paying if the request was already going to be rejected
    for a simpler reason. See docs/rate-limiting-and-backpressure.md."""
    allowed, retry_after = await request.app.state.rate_limiter.check()
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )


async def _check_backpressure(request: Request) -> None:
    settings = request.app.state.settings
    channel = request.app.state.rabbitmq.channel
    depth = await get_total_queue_depth(channel)
    if depth >= settings.backpressure_max_queue_depth:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"queue backlog ({depth}) at or above the configured limit "
                f"({settings.backpressure_max_queue_depth}) -- try again shortly"
            ),
            headers={"Retry-After": "5"},
        )


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
    await _check_rate_limit(request)
    await _check_backpressure(request)
    service = _get_task_service(request)
    task, was_created = await service.create_task(
        task_type=body.task_type,
        payload=body.payload,
        priority=body.priority,
        max_retries=body.max_retries,
        timeout=body.timeout,
        idempotency_key=body.idempotency_key,
        scheduled_at=body.scheduled_at,
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
