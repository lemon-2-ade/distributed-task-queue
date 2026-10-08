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

Phase 19: a scheduled task has no inbound HTTP request to inherit a
trace from (unlike services/api/services/task_service.py's
create_task()), so this is one of this system's two trace roots (the
other being a client's POST /tasks call) -- each claimed due task
gets its own fresh span here, named per task_type, whose context is
what gets captured onto that task's outbox row.
"""

import time
from datetime import datetime, timezone

from domain.states import TaskPriority, TaskStatus
from observability.tracing import get_tracer, inject_trace_context
from persistence.database import AsyncSessionLocal
from persistence.repositories.outbox_repository import OutboxRepository
from persistence.repositories.task_repository import TaskRepository
from persistence.state_manager import TaskStateManager
from services.scheduler.metrics import scheduler_dispatched_total, scheduler_poll_duration_seconds

_tracer = get_tracer(__name__)


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
    started_at = time.perf_counter()

    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        state_manager = TaskStateManager(session)
        outbox_repo = OutboxRepository(session)

        due_tasks = await repo.claim_due_scheduled_tasks(now=now, limit=limit)

        claimed = []
        for task in due_tasks:
            # Fresh trace root per task -- see this module's
            # docstring. Span name carries the task_type (not the
            # task_id, which would be unbounded cardinality if this
            # were a metric label -- but Jaeger spans aren't scraped
            # like Prometheus series, so a per-task_id *attribute*
            # below is fine and is exactly what makes a single task's
            # trace findable in the Jaeger UI).
            with _tracer.start_as_current_span(f"scheduler.dispatch_task.{task.task_type}") as span:
                span.set_attribute("task.id", str(task.task_id))
                span.set_attribute("task.type", task.task_type)
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
                    trace_context=inject_trace_context(),
                )
            claimed.append(queued_task)

        # Commits the QUEUED status, events, and outbox rows for
        # every claimed task in one transaction, and -- just as
        # importantly -- releases every row lock this poll cycle took
        # out, so the *next* scheduler replica's poll (this one or
        # another) can see and claim whatever's left.
        await session.commit()

    scheduler_poll_duration_seconds.observe(time.perf_counter() - started_at)
    if claimed:
        scheduler_dispatched_total.inc(len(claimed))

    return len(claimed)
