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

from coordination.cancellation import CancellationBroadcaster
from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.models import Task, TaskEvent
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager

# Cancellable only from these three -- the same set
# domain/states/transitions.py allows an outgoing CANCELLED edge
# from. Checked here too (not just left to TaskStateManager to
# reject) so cancel_task() can give a clear 409 instead of letting an
# InvalidStateTransitionError leak up as a generic error.
_CANCELLABLE_STATUSES = {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING}


class TaskService:
    def __init__(
        self, publisher: TaskPublisher, cancellation_broadcaster: CancellationBroadcaster
    ) -> None:
        self._publisher = publisher
        self._cancellation_broadcaster = cancellation_broadcaster

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

    async def cancel_task(self, task_id: uuid.UUID) -> Task | None:
        """
        PENDING/QUEUED: nothing is executing yet, so this can
        transition straight to CANCELLED here and now -- no worker
        involvement needed. The message already published to
        RabbitMQ (if any) can't be recalled, but
        services/worker/consumer.py's RUNNING-transition handling
        catches exactly this case (InvalidStateTransitionError,
        since the task is no longer QUEUED by the time that delivery
        is processed) and just acks it without running anything.

        RUNNING: a worker somewhere already has this task's handler
        executing. This process has no direct line to that worker --
        only its worker_id, recorded on the Task row when it entered
        RUNNING. A cancellation request is published on
        coordination/cancellation.py's shared channel instead, and
        *that* worker (if it's still up and still running this task)
        cancels its handler task and transitions to CANCELLED itself.
        This method does NOT transition the status here -- doing so
        would mean Postgres says CANCELLED while the handler might
        still be running for real, which is worse than the honest
        answer: the status stays RUNNING until the owning worker
        confirms the cancellation actually happened. See
        docs/timeouts-and-cancellation.md for the races this implies
        (the worker might already be done, or gone, by the time the
        request arrives -- this is deliberately fire-and-forget).

        Anything else (SUCCESS, FAILED, RETRYING, CANCELLED,
        TIMEOUT, DEAD_LETTERED): raises ValueError for the router to
        turn into a 409 -- there's nothing left to cancel.
        """
        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            task = await repo.get_by_id(task_id)
            if task is None:
                return None

            status = TaskStatus(task.status)
            if status not in _CANCELLABLE_STATUSES:
                raise ValueError(
                    f"task {task_id} is {task.status} -- only PENDING, QUEUED, or "
                    "RUNNING tasks can be cancelled"
                )

            if status in (TaskStatus.PENDING, TaskStatus.QUEUED):
                state_manager = TaskStateManager(session)
                task = await state_manager.transition(
                    task_id,
                    TaskStatus.CANCELLED,
                    event_metadata={"reason": "admin_cancel_before_running"},
                )
                await session.commit()
                return task

        # status == RUNNING: fire-and-forget request to whichever
        # worker owns it, outside the session above (no DB write of
        # our own to make here).
        await self._cancellation_broadcaster.request_cancel(task_id, worker_id=task.worker_id)
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
