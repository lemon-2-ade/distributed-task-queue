"""Pydantic request/response schemas for the API -- kept separate
from persistence/models.py so the wire format and the storage format
can evolve independently."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from domain.states import TaskPriority


class TaskCreateRequest(BaseModel):
    task_type: str
    payload: dict = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3
    timeout: int | None = None
    idempotency_key: str | None = None


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    task_id: uuid.UUID
    task_type: str
    payload: dict
    priority: str
    status: str
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    worker_id: str | None
    retry_count: int
    max_retries: int
    timeout: int | None
    result: dict | None
    error: str | None
    idempotency_key: str | None


class TaskEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: uuid.UUID
    event_type: str
    timestamp: datetime
    event_metadata: dict | None


class WorkerResponse(BaseModel):
    worker_id: str
    queues: list[str]
    concurrency: int
    started_at: float
    last_heartbeat_at: float
