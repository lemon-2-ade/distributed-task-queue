"""
Publishes task messages onto task.exchange.

Message body is JSON, not pickle: pickle would let a message decide
what Python code runs to deserialize it, which is a code-execution
risk the moment anything untrusted can reach the queue (and a
message queue, by nature, is a shared, longer-lived surface than a
single request). JSON also means the message is human-readable in
RabbitMQ's management UI while debugging.

delivery_mode=PERSISTENT tells RabbitMQ to write the message to disk,
not just hold it in memory. Combined with a durable queue (see
topology.py), this is what "the message survives a broker restart"
actually means -- a durable queue with a non-persistent message
still loses that message on restart, which is a common mistake.

`headers` (Phase 19) carries the W3C trace context the outbox relay
read back out of the OutboxMessage row and wants attached to this
specific AMQP message, so the worker on the other end can extract it
and continue the same trace -- see observability/tracing.py and
services/outbox_relay/relay.py. aio_pika.Message's own `headers` kwarg
is exactly RabbitMQ's basic.properties headers table, which survives
the hop to the consumer unmodified, so no project-specific envelope
is needed around it.
"""

import json
import uuid
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractExchange

from domain.states import TaskPriority
from messaging.queues import ROUTING_KEY_BY_PRIORITY


class TaskPublisher:
    def __init__(self, task_exchange: AbstractExchange) -> None:
        self._task_exchange = task_exchange

    async def publish_task(
        self,
        *,
        task_id: uuid.UUID,
        task_type: str,
        payload: dict,
        priority: TaskPriority,
        headers: dict | None = None,
    ) -> None:
        body = {
            "task_id": str(task_id),
            "task_type": task_type,
            "payload": payload,
            "priority": priority.value,
            "published_at": datetime.utcnow().isoformat() + "Z",
        }
        message = aio_pika.Message(
            body=json.dumps(body).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(task_id),
            headers=headers or {},
        )
        routing_key = ROUTING_KEY_BY_PRIORITY[priority]
        await self._task_exchange.publish(message, routing_key=routing_key)
