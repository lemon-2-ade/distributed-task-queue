"""
Repository for `outbox_messages` -- see persistence/models.py's
OutboxMessage docstring and docs/outbox.md for the design this
supports.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
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
        trace_context: dict | None = None,
    ) -> OutboxMessage:
        """
        Call this *inside* the same session/transaction as whatever
        Task status change made this task need publishing, before
        that transaction commits -- never on its own, separately
        committed transaction. That's the entire mechanism: the
        status change and this row either both land or neither does.

        `trace_context` (Phase 19) is whatever
        observability.tracing.inject_trace_context() captured from
        the caller's currently active span -- the API's request
        span, the scheduler's per-dispatch span, or (for a retry) the
        *original* task's span, so the retry stays part of the same
        trace rather than starting a new one. Defaults to None for
        callers with no active span (tracing disabled, or genuinely
        no parent), which the outbox relay treats as "start a fresh
        trace" rather than an error.
        """
        message = OutboxMessage(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
            priority=priority.value,
            trace_context=trace_context,
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

    async def count_unpublished(self) -> int:
        """
        Phase 18: backs the `outbox_unpublished_rows` gauge
        (services/api/metrics.py) -- a cheap, indexed COUNT(*) (the
        partial index on `published_at IS NULL`, see
        persistence/models.py's OutboxMessage, makes this fast even
        as the table grows) used purely to watch outbox-relay lag
        from the outside: a number that's consistently near zero
        means the relay is keeping up; one that keeps climbing means
        it's falling behind or down.
        """
        stmt = select(func.count()).select_from(OutboxMessage).where(
            OutboxMessage.published_at.is_(None)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one()
