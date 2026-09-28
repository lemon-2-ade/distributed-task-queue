"""
The single place that changes a Task's status.

Four things happen atomically (one flush, same transaction as
whatever the caller commits) every time a task's status changes:
1. Validate the transition against domain.states.transitions --
   an illegal transition raises InvalidStateTransitionError instead
   of silently corrupting the state machine.
2. Update the Task row's status (and whatever else the caller
   passed: worker_id, error, result, ...).
3. Append a TaskEvent row -- the audit log GET /tasks/{id}/events
   reads.
4. On entering RUNNING, open a new TaskAttempt row; on leaving
   RUNNING, close the current one.

Why centralize this instead of letting the worker and the API each
update Task.status directly (as they did through Phase 6): every
call site that changes status needs all four of the above to stay
consistent, and a call site that updates status but forgets the
event (or gets the attempt bookkeeping wrong) produces a task whose
history is silently incomplete -- a bug that's invisible until
someone goes looking at /tasks/{id}/events and finds gaps. One
class owning all four means that bug class can't happen per-call-site.
"""

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.exceptions import InvalidStateTransitionError
from domain.states import TaskEventType, TaskStatus, is_valid_transition
from persistence.models import Task, TaskAttempt, TaskEvent

# What event to record when a task *reaches* a given status. Kept as
# its own mapping (not reusing TaskStatus values directly) because
# the event vocabulary is meant to read as a log of what happened
# ("it started retrying") rather than a raw status dump, even though
# they correspond 1:1 today.
_EVENT_TYPE_BY_STATUS: dict[TaskStatus, TaskEventType] = {
    TaskStatus.QUEUED: TaskEventType.TASK_QUEUED,
    TaskStatus.RUNNING: TaskEventType.TASK_STARTED,
    TaskStatus.SUCCESS: TaskEventType.TASK_COMPLETED,
    TaskStatus.FAILED: TaskEventType.TASK_FAILED,
    TaskStatus.RETRYING: TaskEventType.TASK_RETRYING,
    TaskStatus.CANCELLED: TaskEventType.TASK_CANCELLED,
    TaskStatus.TIMEOUT: TaskEventType.TASK_TIMEOUT,
    TaskStatus.DEAD_LETTERED: TaskEventType.TASK_DEAD_LETTERED,
}


class TaskStateManager:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record_creation(self, task: Task) -> None:
        """PENDING isn't reached via transition() -- there's no
        "from" status before a task exists -- so task creation gets
        its own explicit event-recording call."""
        self._session.add(
            TaskEvent(task_id=task.task_id, event_type=TaskEventType.TASK_CREATED.value)
        )
        await self._session.flush()

    async def transition(
        self,
        task_id: uuid.UUID,
        to_status: TaskStatus,
        *,
        worker_id: str | None = None,
        error: str | None = None,
        result: dict | None = None,
        retry_count: int | None = None,
        event_metadata: dict | None = None,
    ) -> Task:
        task = await self._session.get(Task, task_id)
        if task is None:
            raise ValueError(f"no task with id {task_id}")

        from_status = TaskStatus(task.status)
        if not is_valid_transition(from_status, to_status):
            raise InvalidStateTransitionError(task_id, from_status, to_status)

        task.status = to_status.value
        if worker_id is not None:
            task.worker_id = worker_id
        if error is not None:
            task.error = error
        if result is not None:
            task.result = result
        if retry_count is not None:
            task.retry_count = retry_count
        if to_status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}:
            # Note: this tracks the *current attempt's* completion
            # time, not "when the task was done for good" -- a
            # FAILED task that goes on to RETRYING will overwrite
            # this again on its next attempt. task_attempts holds
            # the authoritative per-attempt started_at/completed_at
            # history if every attempt's timing matters, not just
            # the most recent one.
            task.completed_at = datetime.utcnow()
        if to_status == TaskStatus.RUNNING:
            task.started_at = datetime.utcnow()

        self._session.add(
            TaskEvent(
                task_id=task_id,
                event_type=_EVENT_TYPE_BY_STATUS[to_status].value,
                event_metadata=event_metadata,
            )
        )

        if to_status == TaskStatus.RUNNING:
            await self._start_attempt(task, worker_id)
        elif from_status == TaskStatus.RUNNING:
            await self._finish_current_attempt(task, to_status, error)

        await self._session.flush()
        return task

    async def _start_attempt(self, task: Task, worker_id: str | None) -> None:
        attempt_number = task.retry_count + 1
        self._session.add(
            TaskAttempt(
                task_id=task.task_id,
                attempt_number=attempt_number,
                worker_id=worker_id,
                started_at=datetime.utcnow(),
                status=TaskStatus.RUNNING.value,
            )
        )

    async def _finish_current_attempt(
        self, task: Task, to_status: TaskStatus, error: str | None
    ) -> None:
        stmt = (
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task.task_id)
            .order_by(TaskAttempt.attempt_number.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        attempt = result.scalar_one_or_none()
        if attempt is None:
            # Shouldn't happen in practice (RUNNING is only entered
            # via _start_attempt), but a missing attempt row is not
            # a reason to fail the whole transition.
            return
        attempt.completed_at = datetime.utcnow()
        attempt.status = to_status.value
        attempt.error = error
