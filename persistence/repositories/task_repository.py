"""
Task repository -- the only place in the codebase that issues SQL
for the `tasks` table.

Why a repository at all, instead of calling the ORM session directly
from the service layer: it gives every other layer (API routes,
worker, scheduler, CLI) one place to depend on for "how do I read/
write a task," so a later change (e.g. adding row-level locking to
claim_due_scheduled_tasks for the scheduler in Phase 16) touches one
file instead of every call site.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.states import TaskPriority, TaskStatus
from persistence.models import Task


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        task_type: str,
        payload: dict,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        timeout: int | None = None,
        scheduled_at: datetime | None = None,
        idempotency_key: str | None = None,
    ) -> Task:
        task = Task(
            task_id=uuid.uuid4(),
            task_type=task_type,
            payload=payload,
            priority=priority.value,
            status=TaskStatus.PENDING.value,
            max_retries=max_retries,
            timeout=timeout,
            scheduled_at=scheduled_at,
            idempotency_key=idempotency_key,
        )
        self._session.add(task)
        await self._session.flush()
        return task

    async def get_by_id(self, task_id: uuid.UUID) -> Task | None:
        return await self._session.get(Task, task_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Task | None:
        stmt = select(Task).where(Task.idempotency_key == idempotency_key)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list(
        self,
        *,
        status: TaskStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Task]:
        stmt = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
        if status is not None:
            stmt = stmt.where(Task.status == status.value)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def claim_due_scheduled_tasks(self, *, now: datetime, limit: int = 50) -> list[Task]:
        """
        Phase 16: the core of safe multi-replica scheduling. Selects
        PENDING tasks whose scheduled_at has arrived, *locking* each
        matching row (`FOR UPDATE`) for the rest of this transaction
        -- and `SKIP LOCKED` means a second scheduler replica running
        this exact same query concurrently doesn't block waiting for
        those locks, it just silently excludes whatever the first
        replica already has locked and returns whatever's left. Two
        schedulers can run this query at the same instant against an
        overlapping set of due tasks and are *guaranteed* to walk away
        with disjoint result sets -- neither has to know the other
        exists, there's no separate lock/lease to coordinate, and
        nothing here can double-claim a task. This is the standard
        Postgres pattern for "many workers competing to claim rows
        from one table" (the same shape of problem RabbitMQ's own
        consumer dispatch solves at the message-broker level -- this
        is what solves it at the row level).

        Returns the matching Task ORM objects, still locked, within
        the caller's existing session/transaction. Nothing is marked
        QUEUED here -- see services/scheduler/dispatcher.py, which
        transitions each one (via TaskStateManager, for the usual
        event/attempt bookkeeping) and commits, which is what actually
        releases these locks.
        """
        stmt = (
            select(Task)
            .where(
                Task.status == TaskStatus.PENDING.value,
                Task.scheduled_at.is_not(None),
                Task.scheduled_at <= now,
            )
            .order_by(Task.scheduled_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def update_status(
        self,
        task_id: uuid.UUID,
        status: TaskStatus,
        **fields: object,
    ) -> Task | None:
        """
        Low-level, unconditional status write: no state-machine
        validation, no TaskEvent, no TaskAttempt bookkeeping. Kept
        for callers that genuinely don't need those (tests, one-off
        scripts). Anything that's part of a task's real lifecycle
        (the worker, the API) should go through
        persistence.state_manager.TaskStateManager instead, which
        wraps this same kind of write with the transition validation
        and audit trail that make /tasks/{id}/events meaningful.
        """
        task = await self.get_by_id(task_id)
        if task is None:
            return None
        task.status = status.value
        for key, value in fields.items():
            setattr(task, key, value)
        await self._session.flush()
        return task
