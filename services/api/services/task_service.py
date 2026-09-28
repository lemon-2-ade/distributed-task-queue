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

from sqlalchemy import select

from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.models import Task, TaskEvent
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager


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
            state_manager = TaskStateManager(session)
            task = await repo.create(
                task_type=task_type,
                payload=payload,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                idempotency_key=idempotency_key,
            )
            await state_manager.record_creation(task)
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
            state_manager = TaskStateManager(session)
            queued_task = await state_manager.transition(task.task_id, TaskStatus.QUEUED)
            await session.commit()
        return queued_task

    async def get_task(self, task_id: uuid.UUID) -> Task | None:
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            return await repo.get_by_id(task_id)

    async def list_dead_lettered(self, *, limit: int = 50, offset: int = 0) -> list[Task]:
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            return await repo.list(status=TaskStatus.DEAD_LETTERED, limit=limit, offset=offset)

    async def retry_dead_lettered_task(self, task_id: uuid.UUID) -> Task | None:
        """
        Administrative manual retry (POST /tasks/{id}/retry): only
        valid for a task that is currently DEAD_LETTERED -- see
        domain/states/transitions.py for why that's the one allowed
        exception to "terminal means terminal." Resets retry_count to
        0, since this is a deliberate fresh start, not a continuation
        of the automatic retry sequence that already gave up.
        """
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            task = await repo.get_by_id(task_id)
            if task is None:
                return None
            if task.status != TaskStatus.DEAD_LETTERED.value:
                raise ValueError(
                    f"task {task_id} is {task.status}, not DEAD_LETTERED -- only "
                    "dead-lettered tasks can be manually retried"
                )
            state_manager = TaskStateManager(session)
            task = await state_manager.transition(
                task_id,
                TaskStatus.QUEUED,
                retry_count=0,
                event_metadata={"reason": "manual_admin_retry"},
            )
            await session.commit()

        await self._publisher.publish_task(
            task_id=task.task_id,
            task_type=task.task_type,
            payload=task.payload,
            priority=TaskPriority(task.priority),
        )
        return task

    async def get_events(self, task_id: uuid.UUID) -> list[TaskEvent] | None:
        """Returns None if the task itself doesn't exist (so the
        router can 404), or the task's events in chronological order
        otherwise (an empty list is a valid, real answer -- a task
        that exists but somehow has no recorded events yet)."""
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            task = await repo.get_by_id(task_id)
            if task is None:
                return None

            stmt = (
                select(TaskEvent)
                .where(TaskEvent.task_id == task_id)
                .order_by(TaskEvent.timestamp.asc())
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())
