"""
Turns a raw RabbitMQ message into: look up the handler, run it,
persist the result through the task state machine, then ack --
never the other order. See docs/rabbitmq.md ("why ACK timing
matters"): acking before the terminal state is durably written would
let a worker crash between those two steps silently lose the task,
because RabbitMQ would already consider it delivered-and-done.

Every status change goes through persistence.state_manager.
TaskStateManager (Phase 7) rather than writing Task.status directly,
so every transition is validated against the state machine and
recorded to task_events/task_attempts automatically.

Known, deliberate gap in this phase: every failure path here --
an unrecognized task_type, or the handler raising -- nacks with
requeue=False, which (via the dead-letter-exchange argument on every
priority queue, see messaging/topology.py) sends the message
straight to the DLQ. There is no retry yet. Phase 8 (retry manager)
and Phase 9 (DLQ policy) are what turn "any failure -> DLQ" into
"transient failure -> retry with backoff, exhausted retries ->
DLQ" -- this phase's worker only distinguishes success from failure,
on purpose, so that distinction is easy to see land later rather
than being tangled in from the start.

Concurrency (Phase 6): aio-pika already invokes this callback as its
own asyncio task per delivered message, so multiple messages are
"in flight" (received, not yet acked) simultaneously whenever
prefetch_count > 1 -- see services/worker/main.py. The semaphore
below is a second, explicit, application-level bound on how many
handler bodies may actually be *executing* at once, kept
deliberately separate from prefetch (see this module's earlier
history / docs/concurrency.md for the full reasoning).
"""

import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from domain.states import TaskStatus
from persistence.database import AsyncSessionLocal
from persistence.state_manager import TaskStateManager
from task_handlers import TASK_HANDLERS


def make_message_handler(worker_id: str, concurrency: int):
    """
    Returns a message callback bound to this worker's id (so
    `TaskStateManager.transition(..., worker_id=...)` records which
    worker actually ran a given attempt) and to a concurrency limit
    of at most `concurrency` task handlers running at once in this
    process.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def handle_message(message: AbstractIncomingMessage) -> None:
        async with semaphore:
            try:
                body = json.loads(message.body)
                task_id = uuid.UUID(body["task_id"])
                task_type = body["task_type"]
                payload = body["payload"]
            except Exception:
                # Can't even parse this message -- definitely not
                # something a retry would fix.
                await message.nack(requeue=False)
                return

            try:
                await _transition(task_id, TaskStatus.RUNNING, worker_id=worker_id)
            except ValueError:
                # No matching task row: a stray or duplicate-delivered
                # message referencing a task we don't know about.
                # Nothing to run, nothing worth retrying.
                await message.nack(requeue=False)
                return

            handler = TASK_HANDLERS.get(task_type)
            if handler is None:
                await _transition(
                    task_id,
                    TaskStatus.FAILED,
                    error=f"no handler registered for task_type={task_type!r}",
                )
                await message.nack(requeue=False)
                return

            try:
                result = await handler(payload)
            except Exception as exc:  # a task handler's own error is expected, arbitrary application failure
                await _transition(task_id, TaskStatus.FAILED, error=str(exc))
                await message.nack(requeue=False)
                return

            await _transition(task_id, TaskStatus.SUCCESS, result=result)
            await message.ack()

    return handle_message


async def _transition(task_id: uuid.UUID, to_status: TaskStatus, **kwargs: object):
    async with AsyncSessionLocal() as session:
        state_manager = TaskStateManager(session)
        task = await state_manager.transition(task_id, to_status, **kwargs)
        await session.commit()
        return task
