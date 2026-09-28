"""
Turns a raw RabbitMQ message into: look up the handler, run it,
persist the result through the task state machine, then ack --
never the other order. See docs/rabbitmq.md ("why ACK timing
matters"): acking before the terminal state is durably written would
let a worker crash between those two steps silently lose the task,
because RabbitMQ would already consider it delivered-and-done.

Every status change goes through persistence.state_manager.
TaskStateManager rather than writing Task.status directly, so every
transition is validated against the state machine and recorded to
task_events/task_attempts automatically.

Retries (Phase 8): a handler failure is one of two things --
1. A `PermanentTaskError`: the handler is telling us this will never
   succeed (bad payload, a rejected business rule). No retry,
   regardless of remaining budget.
2. Any other exception: assumed transient. If `retry_count <
   max_retries`, the task goes FAILED -> RETRYING (recording the
   attempt and incrementing retry_count), this worker sleeps for a
   jittered exponential backoff (domain/retry_policy.py), then
   RETRYING -> QUEUED and a *new* message is published for the next
   attempt -- only then is the *original* message acked. If retries
   are exhausted, the task stays FAILED and the original message is
   nacked with requeue=False, which (via the dead-letter-exchange
   argument on every priority queue) sends it to the DLQ. Nothing
   marks it DEAD_LETTERED yet -- Phase 9 adds a consumer that
   watches the DLQ and does that; this phase only gets the message
   there.

Deliberate tradeoff, not an oversight: sleeping for the backoff delay
happens *inside* this handler, while holding both the semaphore slot
(Phase 6) and the original message unacked. That means a retrying
task occupies one of this worker's WORKER_CONCURRENCY slots for the
whole backoff window instead of freeing it up immediately -- the
alternative (a broker-side delay queue using per-message TTL + a
dead-letter-exchange trick, since core RabbitMQ has no native
"deliver this message in N seconds") adds real complexity and its
own gotcha (RabbitMQ only inspects a queue's *head* for TTL expiry,
so variable per-message TTLs in one queue don't expire strictly in
TTL order). This project takes the simpler, fully-in-application-code
option and documents the cost rather than building a delay-queue
mechanism whose edge cases would need their own explanation. See
docs/retries.md.

Concurrency (Phase 6): aio-pika already invokes this callback as its
own asyncio task per delivered message, so multiple messages are
"in flight" simultaneously whenever prefetch_count > 1 -- see
services/worker/main.py. The semaphore below is a second, explicit,
application-level bound on how many handler bodies may actually be
executing at once.
"""

import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from config import get_settings
from domain.exceptions import PermanentTaskError
from domain.retry_policy import compute_backoff_seconds
from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.models import Task
from persistence.state_manager import TaskStateManager
from task_handlers import TASK_HANDLERS


def make_message_handler(worker_id: str, concurrency: int, publisher: TaskPublisher):
    """
    Returns a message callback bound to this worker's id (recorded
    on every attempt), a concurrency limit of at most `concurrency`
    handlers running at once, and a `publisher` used to republish a
    task for its next attempt after a transient failure.
    """
    semaphore = asyncio.Semaphore(concurrency)
    settings = get_settings()

    async def handle_message(message: AbstractIncomingMessage) -> None:
        async with semaphore:
            try:
                body = json.loads(message.body)
                task_id = uuid.UUID(body["task_id"])
                task_type = body["task_type"]
                payload = body["payload"]
                priority = TaskPriority(body["priority"])
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
            except Exception as exc:
                await _handle_failure(
                    message,
                    task_id=task_id,
                    task_type=task_type,
                    payload=payload,
                    priority=priority,
                    error=exc,
                    publisher=publisher,
                    settings=settings,
                )
                return

            await _transition(task_id, TaskStatus.SUCCESS, result=result)
            await message.ack()

    return handle_message


async def _handle_failure(
    message: AbstractIncomingMessage,
    *,
    task_id: uuid.UUID,
    task_type: str,
    payload: dict,
    priority: TaskPriority,
    error: Exception,
    publisher: TaskPublisher,
    settings,
) -> None:
    task = await _transition(task_id, TaskStatus.FAILED, error=str(error))

    is_permanent = isinstance(error, PermanentTaskError)
    retries_remaining = task.retry_count < task.max_retries

    if is_permanent or not retries_remaining:
        # Exhausted or unretryable: leave it FAILED, send the
        # message to the DLQ. See this module's docstring for why
        # nothing marks it DEAD_LETTERED yet.
        await message.nack(requeue=False)
        return

    next_attempt = task.retry_count + 1
    delay_seconds = compute_backoff_seconds(
        next_attempt,
        base_delay=settings.retry_base_delay_seconds,
        max_delay=settings.retry_max_delay_seconds,
        jitter_fraction=settings.retry_jitter_fraction,
    )

    await _transition(task_id, TaskStatus.RETRYING, retry_count=next_attempt)

    await asyncio.sleep(delay_seconds)

    await _transition(task_id, TaskStatus.QUEUED)
    await publisher.publish_task(
        task_id=task_id, task_type=task_type, payload=payload, priority=priority
    )
    # Only now: the original message's work (recording the failure
    # and publishing its replacement) is durably done, so it's safe
    # to ack it. Acking earlier and then crashing mid-backoff-sleep
    # would silently lose the retry -- the same "why ACK timing
    # matters" reasoning as the success path.
    await message.ack()


async def _transition(task_id: uuid.UUID, to_status: TaskStatus, **kwargs: object) -> Task:
    async with AsyncSessionLocal() as session:
        state_manager = TaskStateManager(session)
        task = await state_manager.transition(task_id, to_status, **kwargs)
        await session.commit()
        return task
