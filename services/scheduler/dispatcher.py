"""
The claim-and-publish use case, separated from services/scheduler/
main.py's process/polling-loop concerns the same way
services/api/services/task_service.py separates business logic from
HTTP routing -- this function is independently testable without a
RabbitMQ connection or a running process around it.
"""

from datetime import datetime, timezone

from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager


async def claim_and_dispatch_due_tasks(publisher: TaskPublisher, *, limit: int) -> int:
    """
    One poll cycle: claim whatever PENDING tasks are due right now
    (up to `limit`), mark them QUEUED, and publish each one. Returns
    how many were dispatched, purely so the caller can log something
    useful -- 0 (every poll finds nothing due) is the normal, common
    case, not a special one.

    Two-phase, same shape as every other write-then-publish path in
    this codebase (services/api/services/task_service.py's
    create_task, retry_dead_lettered_task): claim + transition +
    commit in one Postgres transaction first (this is what actually
    releases the FOR UPDATE SKIP LOCKED row locks from
    claim_due_scheduled_tasks -- see that method's docstring), *then*
    publish to RabbitMQ as a separate step once the commit has
    already happened. A crash between those two steps leaves a task
    QUEUED in Postgres forever without ever reaching RabbitMQ -- the
    same dual-write problem documented everywhere else in this
    project pre-Outbox (Phase 17), not a new failure mode invented
    here.
    """
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        state_manager = TaskStateManager(session)

        due_tasks = await repo.claim_due_scheduled_tasks(now=now, limit=limit)

        claimed = []
        for task in due_tasks:
            queued_task = await state_manager.transition(
                task.task_id,
                TaskStatus.QUEUED,
                event_metadata={"reason": "scheduled_at_due", "scheduled_at": task.scheduled_at.isoformat()},
            )
            claimed.append(queued_task)

        # Commits the QUEUED status + events for every claimed task
        # in one transaction, and -- just as importantly -- releases
        # every row lock this poll cycle took out, so the *next*
        # scheduler replica's poll (this one or another) can see and
        # claim whatever's left.
        await session.commit()

    for task in claimed:
        await publisher.publish_task(
            task_id=task.task_id,
            task_type=task.task_type,
            payload=task.payload,
            priority=TaskPriority(task.priority),
        )

    return len(claimed)
