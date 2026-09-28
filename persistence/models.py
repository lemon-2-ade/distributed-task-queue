"""
SQLAlchemy ORM models -- the durable system of record.

Only the `tasks` table is defined in this phase. `task_attempts`,
`task_events`, `workers`, and a dedicated `idempotency_keys` table
arrive in the phases that actually need them (task attempts/event
history, worker registry, idempotency), rather than being
speculatively created now with nothing populating them.
"""

import uuid
from datetime import datetime

from sqlalchemy import Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from domain.states import TaskPriority, TaskStatus


class Base(DeclarativeBase):
    pass


class Task(Base):
    __tablename__ = "tasks"

    task_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    task_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    priority: Mapped[str] = mapped_column(
        String(16), nullable=False, default=TaskPriority.NORMAL.value
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=TaskStatus.PENDING.value
    )

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)

    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout: Mapped[int | None] = mapped_column(Integer, nullable=True)

    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    idempotency_key: Mapped[str | None] = mapped_column(
        String(255), nullable=True, unique=True
    )

    __table_args__ = (
        # The queue/scheduler's core query is "give me due, claimable
        # tasks of a given status ordered by priority" -- this
        # composite index is shaped for exactly that lookup rather
        # than for filtering on any one column alone.
        Index("ix_tasks_status_priority_scheduled_at", "status", "priority", "scheduled_at"),
        # Dashboards and DLQ views filter by status alone.
        Index("ix_tasks_status", "status"),
        # "show me all attempts of this task_type" / handler routing.
        Index("ix_tasks_task_type", "task_type"),
    )
