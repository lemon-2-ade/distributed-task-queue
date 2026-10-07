"""
Task creation/read use cases -- the layer routers call into instead
of touching the repository or messaging layer directly (see
services/api/main.py's module docstring for why).

Phase 17 (Outbox Pattern) removes this service's direct dependency on
TaskPublisher/RabbitMQ entirely. Through Phase 16, create_task() and
retry_dead_lettered_task() each wrote a Task status change to
Postgres, committed, and *then* called TaskPublisher.publish_task()
as a second, separate operation -- the "dual-write problem"
documented since Phase 5: a crash between those two steps left a task
QUEUED in Postgres forever without ever reaching RabbitMQ, and
nothing would ever pick it up.

Both methods now write the status change *and* an OutboxMessage row
(persistence/models.py) in the exact same Postgres transaction
instead -- either both land, or neither does, so there is no window
where Postgres says QUEUED but nothing durable records that a
message still needs to reach RabbitMQ. A separate process,
services/outbox_relay/, is the only thing that actually talks to
RabbitMQ to publish now; this service doesn't need a TaskPublisher at
all anymore. See docs/outbox.md for the full design and the new
failure mode this trades the old one for (a possible duplicate
publish, never a lost one -- handled by the idempotency machinery
already in place since Phase 13).
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from coordination.cancellation import CancellationBroadcaster
from domain.states import TaskPriority, TaskStatus
from persistence.database import AsyncSessionLocal
from persistence.models import Task, TaskEvent
from persistence.repositories.outbox_repository import OutboxRepository
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager

# Cancellable only from these three -- the same set
# domain/states/transitions.py allows an outgoing CANCELLED edge
# from. Checked here too (not just left to TaskStateManager to
# reject) so cancel_task() can give a clear 409 instead of letting an
# InvalidStateTransitionError leak up as a generic error.
_CANCELLABLE_STATUSES = {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING}


class TaskService:
    def __init__(self, cancellation_broadcaster: CancellationBroadcaster) -> None:
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
        scheduled_at: datetime | None = None,
    ) -> tuple[Task, bool]:
        """
        Returns (task, was_created). was_created=False means an
        idempotency_key collision returned an existing task instead
        of creating a new one -- see docs/idempotency.md for why this
        exists (safe client-side retry of the POST /tasks request
        itself, distinct from the worker-side redelivery dedup in
        services/worker/consumer.py) and the two distinct races this
        method has to handle.

        Phase 16: a `scheduled_at` in the future means this task is
        deliberately left PENDING and *not* enqueued to the outbox
        here -- it's services/scheduler/main.py's job to notice (by
        polling Postgres) once that time arrives, claim it, and
        enqueue it to the outbox then. A missing or already-past
        scheduled_at is "run now," unchanged from every phase before
        Phase 16, and takes the same immediate-QUEUED path it always
        has -- just via the outbox (Phase 17) rather than a direct
        publish. See docs/scheduling.md and docs/outbox.md.
        """
        if scheduled_at is not None and scheduled_at.tzinfo is None:
            # A client can send an ISO timestamp with no UTC offset
            # (Pydantic parses that as a naive datetime). Rather than
            # reject it or silently compare naive-vs-aware below
            # (which raises TypeError), treat an offset-less
            # scheduled_at as already being UTC -- the same
            # convention every other timestamp in this system uses
            # (see persistence/state_manager.py's datetime.utcnow()
            # calls, and Task.created_at's server_default=func.now()).
            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

        if idempotency_key is not None:
            async with AsyncSessionLocal() as session:
                repo = TaskRepository(session)
                existing = await repo.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False

        async with AsyncSessionLocal() as session:
            repo = TaskRepository(session)
            state_manager = TaskStateManager(session)
            task = await repo.create(
                task_type=task_type,
                payload=payload,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                scheduled_at=scheduled_at,
                idempotency_key=idempotency_key,
            )
            await state_manager.record_creation(task)
            try:
                await session.commit()
            except IntegrityError:
                # Lost the race: between the lookup above and this
                # commit, a concurrent request with the *same*
                # idempotency_key already inserted its row first, and
                # the unique constraint on Task.idempotency_key
                # (Phase 3) rejected this one. This isn't a real
                # error from the caller's point of view -- it's the
                # same "this idempotency_key already has a task"
                # outcome the lookup above was trying to catch, just
                # discovered a few milliseconds later than it could
                # have been. Roll back this half-finished insert and
                # hand back whichever row actually won the race.
                await session.rollback()
                existing = await repo.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False
                raise  # genuinely unexpected: re-raise rather than hide it

        if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
            # Deferred: stays PENDING, nothing enqueued yet. The
            # scheduler's claim_due_scheduled_tasks() query is what
            # eventually picks this row up -- see
            # persistence/repositories/task_repository.py.
            return task, True

        # Run now (no scheduled_at, or one already in the past): the
        # QUEUED transition and the outbox row are written together,
        # in one transaction -- see this module's docstring.
        async with AsyncSessionLocal() as session:
            state_manager = TaskStateManager(session)
            outbox_repo = OutboxRepository(session)
            queued_task = await state_manager.transition(task.task_id, TaskStatus.QUEUED)
            await outbox_repo.enqueue(
                task_id=queued_task.task_id,
                task_type=queued_task.task_type,
                payload=queued_task.payload,
                priority=priority,
            )
            await session.commit()
        return queued_task, True

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
        of the automatic retry sequence that already gave up. The
        QUEUED transition and the outbox row are written in the same
        transaction, same as create_task() above.
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
            outbox_repo = OutboxRepository(session)
            task = await state_manager.transition(
                task_id,
                TaskStatus.QUEUED,
                retry_count=0,
                event_metadata={"reason": "manual_admin_retry"},
            )
            await outbox_repo.enqueue(
                task_id=task.task_id,
                task_type=task.task_type,
                payload=task.payload,
                priority=TaskPriority(task.priority),
            )
            await session.commit()
        return task

    async def cancel_task(self, task_id: uuid.UUID) -> Task | None:
        """
        PENDING/QUEUED: nothing is executing yet, so this can
        transition straight to CANCELLED here and now -- no worker
        involvement needed. A message already published to RabbitMQ
        (if any) can't be recalled, but services/worker/consumer.py's
        RUNNING-transition handling catches exactly this case
        (InvalidStateTransitionError, since the task is no longer
        QUEUED by the time that delivery is processed) and just acks
        it without running anything.

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
