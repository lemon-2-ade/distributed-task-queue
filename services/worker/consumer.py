"""
Turns a raw RabbitMQ message into: look up the handler, run it,
persist the result, then ack -- never the other order. See
docs/rabbitmq.md ("why ACK timing matters"): acking before the
terminal state is durably written would let a worker crash between
those two steps silently lose the task, because RabbitMQ would
already consider it delivered-and-done.

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
deliberately separate from prefetch: prefetch is an AMQP flow-control
setting (how many unacked messages the broker will hand this
channel), while the semaphore is what this process treats as its own
concurrency budget regardless of how messages were delivered. In this
phase the two numbers are set equal (both from WORKER_CONCURRENCY),
so in practice the semaphore rarely blocks -- it's here so the
concurrency limit is enforced by code this project owns, not only by
an AMQP setting whose interaction with prefetch is easy to get wrong.
"""

import asyncio
import json
import uuid
from datetime import datetime

from aio_pika.abc import AbstractIncomingMessage

from domain.states import TaskStatus
from persistence.database import AsyncSessionLocal
from persistence.repositories.task_repository import TaskRepository
from task_handlers import TASK_HANDLERS


def make_message_handler(worker_id: str, concurrency: int):
    """
    Returns a message callback bound to this worker's id (so
    `TaskRepository.update_status(..., worker_id=...)` records which
    worker actually ran a given attempt) and to a concurrency limit
    of at most `concurrency` task handlers running at once in this
    process.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def handle_message(message: AbstractIncomingMessage) -> None:
        async with semaphore:
            body = json.loads(message.body)
            task_id = uuid.UUID(body["task_id"])
            task_type = body["task_type"]
            payload = body["payload"]

            await _mark_running(task_id, worker_id)

            handler = TASK_HANDLERS.get(task_type)
            if handler is None:
                await _mark_failed(task_id, f"no handler registered for task_type={task_type!r}")
                await message.nack(requeue=False)
                return

            try:
                result = await handler(payload)
            except Exception as exc:  # a task handler's own error is expected, arbitrary application failure
                await _mark_failed(task_id, str(exc))
                await message.nack(requeue=False)
                return

            await _mark_success(task_id, result)
            await message.ack()

    return handle_message


async def _mark_running(task_id: uuid.UUID, worker_id: str) -> None:
    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        await repo.update_status(
            task_id, TaskStatus.RUNNING, started_at=datetime.utcnow(), worker_id=worker_id
        )
        await session.commit()


async def _mark_success(task_id: uuid.UUID, result: dict | None) -> None:
    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        await repo.update_status(
            task_id, TaskStatus.SUCCESS, result=result, completed_at=datetime.utcnow()
        )
        await session.commit()


async def _mark_failed(task_id: uuid.UUID, error: str) -> None:
    async with AsyncSessionLocal() as session:
        repo = TaskRepository(session)
        await repo.update_status(
            task_id, TaskStatus.FAILED, error=error, completed_at=datetime.utcnow()
        )
        await session.commit()
