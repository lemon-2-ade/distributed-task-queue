"""
Repository for `outbox_messages` -- see persistence/models.py's
OutboxMessage docstring and docs/outbox.md for the design this
supports.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.states import TaskPriority
from persistence.models import OutboxMessage


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(
        self,
        *,
        task_id: uuid.UUID,
        task_type: str,
        payload: dict,
        priority: TaskPriority,
    ) -> OutboxMessage:
        """
        Call this *inside* the same session/transaction as whatever
        Task status change made this task need publishing, before
        that transaction commits -- never on its own, separately
        committed transaction. That's the entire mechanism: the
        status change and this row either both land or neither does.
        """
        message = OutboxMessage(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
            priority=priority.value,
        )
        self._session.add(message)
        await self._session.flush()
        return message

    async def claim_unpublished(self, *, limit: int = 100) -> list[OutboxMessage]:
        """
        Same FOR UPDATE SKIP LOCKED pattern as
        TaskRepository.claim_due_scheduled_tasks() (Phase 16), for
        the identical reason: it's what makes running multiple
        services/outbox_relay/ replicas safe with zero coordination
        between them -- two replicas polling at the same instant walk
        away with disjoint, non-overlapping sets of rows to relay.
        """
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_published(self, message: OutboxMessage) -> None:
        message.published_at = datetime.now(timezone.utc)
        await self._session.flush()
