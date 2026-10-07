"""
The claim-and-enqueue use case, separated from services/scheduler/
main.py's process/polling-loop concerns the same way
services/api/services/task_service.py separates business logic from
HTTP routing -- this function is independently testable without a
RabbitMQ connection or a running process around it.

Phase 17: this no longer publishes to RabbitMQ at all. Claiming a due
task and writing its outbox row both happen in the one Postgres
transaction that claim_due_scheduled_tasks()'s row locks are held
for -- see persistence/repositories/task_repository.py's docstring
for why that matters (committing is what releases the locks for the
next scheduler replica). services/outbox_relay/ is now the only
process that ever talks to RabbitMQ to publish; this process is
Postgres-only.
"""

from datetime import datetime, timezone

from domain.states import TaskPriority, TaskStatus
from persistence.database import AsyncSessionLocal
from persistence.repositories.outbox_repository import OutboxRepository
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager


async def claim_and_dispatch_due_tasks(*, limit: int) -> int:
    """
    One poll cycle: claim whatever PENDING tasks are due right now
    (up to `limit`), mark them QUEUED, and enqueue an outbox row for
    each -- all within one Postgres transaction. Returns how many
    were dispatched, purely so the caller can log something useful --
    0 (every poll finds nothing due) is the normal, common case, not
    a special one.
    """
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        state_manager = TaskStateManager(session)
        outbox_repo = OutboxRepository(session)

        due_tasks = await repo.claim_due_scheduled_tasks(now=now, limit=limit)

        claimed = []
        for task in due_tasks:
            queued_task = await state_manager.transition(
                task.task_id,
                TaskStatus.QUEUED,
                event_metadata={
                    "reason": "scheduled_at_due",
                    "scheduled_at": task.scheduled_at.isoformat(),
                },
            )
            await outbox_repo.enqueue(
                task_id=queued_task.task_id,
                task_type=queued_task.task_type,
                payload=queued_task.payload,
                priority=TaskPriority(queued_task.priority),
            )
            claimed.append(queued_task)

        # Commits the QUEUED status, events, and outbox rows for
        # every claimed task in one transaction, and -- just as
        # importantly -- releases every row lock this poll cycle took
        # out, so the *next* scheduler replica's poll (this one or
        # another) can see and claim whatever's left.
        await session.commit()

    return len(claimed)
