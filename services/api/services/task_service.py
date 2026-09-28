"""
Task creation/read use cases -- the layer routers call into instead
of touching the repository or messaging layer directly (see
services/api/main.py's module docstring for why).

Known, deliberate gap in this phase: create_task() writes the task
row to PostgreSQL, commits, and *then* publishes to RabbitMQ as a
second, separate operation. If this process dies between those two
steps, the task exists in the database as PENDING forever but is
never enqueued -- nothing will ever pick it up. This is the "dual-
write problem" documented in docs/architecture.md. The correct fix
is the Outbox Pattern (Phase 17): write the task *and* an outbox
row in one transaction, and let a separate publisher process drain
the outbox. That doesn't exist yet -- this version is left honestly
imperfect so the Outbox phase is a visible fix, not a silent one.
"""

import uuid

from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.models import Task
from persistence.repositories.task_repository import TaskRepository


class TaskService:
    def __init__(self, publisher: TaskPublisher) -> None:
        self._publisher = publisher

    async def create_task(
        self,
        *,
        task_type: str,
        payload: dict,
        priority: TaskPriority,
        max_retries: int,
        timeout: int | None,
        idempotency_key: str | None,
    ) -> Task:
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            task = await repo.create(
                task_type=task_type,
                payload=payload,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                idempotency_key=idempotency_key,
            )
            await session.commit()

        # See module docstring: this publish is not part of the
        # transaction above. A crash right here is the dual-write
        # problem in action.
        await self._publisher.publish_task(
            task_id=task.task_id,
            task_type=task.task_type,
            payload=task.payload,
            priority=priority,
        )

        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            queued_task = await repo.update_status(task.task_id, TaskStatus.QUEUED)
            await session.commit()
        return queued_task

    async def get_task(self, task_id: uuid.UUID) -> Task | None:
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            return await repo.get_by_id(task_id)
