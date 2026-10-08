"""
SQLAlchemy ORM models -- the durable system of record.

`tasks`, `task_attempts`, and `task_events` are defined in this
phase. A `workers` table and a dedicated `idempotency_keys` table
arrive in the phases that actually need them (worker registry,
request-level idempotency), rather than being speculatively created
now with nothing populating them.

task_attempts vs task_events: an *attempt* is one execution try (one
row per RUNNING episode -- retry N creates attempt N+1), used to
answer "how many times has this run, and what happened each time."
An *event* is a finer-grained, append-only log entry for every
state-machine transition (see domain/states/transitions.py), used to
answer "show me everything that happened to this task, in order" --
GET /tasks/{id}/events. Both are written by
persistence.state_manager.TaskStateManager alongside every status
change, never directly by a route or the worker.

Phase 17 adds `outbox_messages`: the transactional outbox. See
docs/outbox.md for the full design -- in short, every place that
used to write a Task status change *and then separately* call
TaskPublisher.publish_task() (two operations, not atomic, the
"dual-write problem" flagged throughout this codebase since Phase 5)
now writes the status change *and* an OutboxMessage row in the exact
same Postgres transaction instead. A separate process,
services/outbox_relay/, is the only thing left that actually talks to
RabbitMQ to publish -- it polls this table for unpublished rows and
relays them.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, func, text
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
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

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


class TaskAttempt(Base):
    __tablename__ = "task_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("tasks.task_id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # "list every attempt for this task, in order" is the only
        # query shape this table needs to serve right now.
        Index("ix_task_attempts_task_id", "task_id"),
    )


class OutboxMessage(Base):
    """
    One row per "this task needs to be published to RabbitMQ."
    Written in the same transaction as whatever Task status change
    made that true (QUEUED, whether from immediate creation, a
    scheduler claim, a manual retry, or an automatic retry), which is
    the entire point: a transaction either commits both the status
    change and the intent-to-publish together, or neither -- there is
    no window where Postgres says QUEUED but nothing durable records
    that a message still needs to reach RabbitMQ.

    `published_at` is null until services/outbox_relay/relay.py
    successfully hands this to RabbitMQ and marks it. It is never
    deleted on success (kept as an audit trail of what was actually
    relayed and when) -- this table is expected to be pruned by
    retention tooling in a real deployment, not by application code,
    the same stance this project takes on task_events never being
    deleted either.
    """

    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("tasks.task_id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[str] = mapped_column(String(16), nullable=False)

    # Phase 19: the W3C traceparent/tracestate pair captured from
    # whatever span was active when this row was written (a request
    # handler, a retry, or a scheduler dispatch -- see
    # observability/tracing.py's inject_trace_context()). Nullable
    # because rows written before this column existed, and rows
    # written while tracing is disabled, legitimately have none --
    # extract_trace_context() treats that the same as an empty dict,
    # so the outbox relay and worker fall back to starting a fresh
    # trace rather than erroring out. This is what lets a trace begun
    # in the API/scheduler survive the gap between "written to
    # Postgres" and "picked up by the outbox relay," where no Python
    # call stack -- and so no contextvars-based propagation -- exists.
    trace_context: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # The relay's entire query shape is "give me unpublished rows,
        # oldest first" -- a partial index (only indexing the
        # published_at IS NULL rows) keeps this index small and fast
        # regardless of how many already-published rows have piled up
        # over the system's lifetime, since those never need to be
        # found by this query again.
        Index(
            "ix_outbox_messages_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class TaskEvent(Base):
    __tablename__ = "task_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("tasks.task_id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # Mapped attribute is `event_metadata`, not `metadata`: SQLAlchemy's
    # DeclarativeBase already reserves `metadata` as the class-level
    # schema registry, so a column attribute can't reuse that name.
    # The database column itself is still named `metadata`.
    event_metadata: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)

    __table_args__ = (
        # GET /tasks/{id}/events reads this table ordered by time
        # for one task -- shape the index for exactly that.
        Index("ix_task_events_task_id_timestamp", "task_id", "timestamp"),
    )
