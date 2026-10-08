"""
The claim-publish-mark use case -- separated from
services/outbox_relay/main.py's process/polling-loop concerns, same
reasoning as services/scheduler/dispatcher.py.

This is the one place left in the entire codebase that calls
TaskPublisher.publish_task(). Every write path that used to publish
directly (task creation, manual DLQ retry, automatic retry
republishing, scheduled-task dispatch) now only ever writes an
OutboxMessage row -- see persistence/models.py's OutboxMessage
docstring and docs/outbox.md. This function is what actually drains
that table into RabbitMQ.

Phase 19: this is also the one place that bridges trace context from
"stored as data on a Postgres row" back to "live on an AMQP message."
Each row's trace_context (captured by whoever wrote it -- see
persistence/models.py's OutboxMessage and
observability/tracing.py's module docstring) is extracted into a new
child span here, and that span's *own* context is what actually gets
injected into the outgoing message's headers -- not the row's
original context verbatim -- so the resulting trace shows this relay
step as a real hop (its own span, with its own duration) between
"written to Postgres" and "received by a worker," rather than
silently disappearing.
"""

import time

from domain.states import TaskPriority
from messaging.publisher import TaskPublisher
from observability.tracing import get_tracer, inject_trace_context, start_span_from_carrier
from persistence.database import AsyncSessionLocal
from persistence.repositories.outbox_repository import OutboxRepository
from services.outbox_relay.metrics import outbox_relay_poll_duration_seconds, outbox_relayed_total

_tracer = get_tracer(__name__)


async def relay_once(publisher: TaskPublisher, *, limit: int) -> int:
    """
    One poll cycle: claim up to `limit` unpublished outbox rows
    (FOR UPDATE SKIP LOCKED, same mechanism and reasoning as
    TaskRepository.claim_due_scheduled_tasks -- see
    OutboxRepository.claim_unpublished's docstring), publish each one
    to RabbitMQ, mark it published, and commit. Returns how many were
    relayed.

    Deliberate choice, worth contrasting with
    services/scheduler/dispatcher.py: that function claims rows and
    commits *before* doing anything that reaches outside Postgres,
    specifically so it never holds row locks across I/O to another
    system. This function can't make that same choice -- publishing
    to RabbitMQ *is* its entire job, there's no further place to
    defer that network call to. So the claimed rows' locks stay held
    for the duration of this batch's publish calls, and the whole
    batch is marked published and committed together. The accepted
    cost: if this process crashes partway through a batch (say, after
    successfully publishing message 3 of 5 but before the commit),
    the whole transaction rolls back -- *every* row in the batch,
    including the ones already genuinely delivered to RabbitMQ,
    reverts to unpublished and gets relayed again next cycle. That
    means a message can be published more than once (a duplicate),
    but never zero times once its outbox row was durably committed by
    the writer -- exactly the guarantee the Outbox Pattern exists to
    provide, and exactly why this system has carried idempotency
    handling (Phase 13, docs/idempotency.md) since well before this
    phase: duplicates were already a fact of life under RabbitMQ's
    own at-least-once delivery (docs/rabbitmq.md), and this just adds
    one more legitimate source of them rather than a new category of
    problem.
    """
    started_at = time.perf_counter()

    async with AsyncSessionLocal() as session:
        repo = OutboxRepository(session)
        pending = await repo.claim_unpublished(limit=limit)

        for message in pending:
            with start_span_from_carrier(
                _tracer, f"outbox_relay.publish.{message.task_type}", message.trace_context
            ) as span:
                span.set_attribute("task.id", str(message.task_id))
                span.set_attribute("task.type", message.task_type)
                await publisher.publish_task(
                    task_id=message.task_id,
                    task_type=message.task_type,
                    payload=message.payload,
                    priority=TaskPriority(message.priority),
                    headers=inject_trace_context(),
                )
            await repo.mark_published(message)

        await session.commit()

    outbox_relay_poll_duration_seconds.observe(time.perf_counter() - started_at)
    if pending:
        outbox_relayed_total.inc(len(pending))

    return len(pending)
