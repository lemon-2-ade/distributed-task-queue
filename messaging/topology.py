"""
Declares the exchange/queue/binding topology.

    task.exchange (direct)
        |
        +--routing key "high"---> high_priority.queue
        |
        +--routing key "normal"-> normal_priority.queue
        |
        +--routing key "low"----> low_priority.queue

    dead_letter.exchange (direct)
        |
        +--routing key "dead_letter"--> dead_letter.queue

Every priority queue is declared with x-dead-letter-exchange pointed
at dead_letter.exchange, so RabbitMQ itself -- not application code --
re-routes a message there the moment a consumer nacks it without
requeue (basic.reject/basic.nack with requeue=False) or a per-message
TTL expires. Phase 9 builds the *policy* of when that should happen
(retry exhaustion); this phase only builds the *plumbing* that makes
it possible.

Declaring exchanges/queues is idempotent: declaring something that
already exists with the same parameters is a no-op, which is why
it's safe to call declare_topology() from every service's startup
(API, and later worker/scheduler) rather than needing a single
"owner" service to run it once.

`durable=True` on every exchange and queue means their definitions
survive a RabbitMQ restart (they're written to disk, not held only
in memory) -- without it, restarting the broker would silently drop
the entire topology and messages, not just the connections to it.
"""

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange

from messaging.exchanges import DEAD_LETTER_EXCHANGE, TASK_EXCHANGE
from messaging.queues import (
    DEAD_LETTER_QUEUE,
    DEAD_LETTER_ROUTING_KEY,
    QUEUE_BY_PRIORITY,
    ROUTING_KEY_BY_PRIORITY,
)


async def declare_topology(channel: AbstractChannel) -> AbstractExchange:
    """
    Declares every exchange, queue, and binding this system depends
    on, and returns the task exchange (what publishers need).
    """
    dead_letter_exchange = await channel.declare_exchange(
        DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dead_letter_queue = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    await dead_letter_queue.bind(dead_letter_exchange, routing_key=DEAD_LETTER_ROUTING_KEY)

    task_exchange = await channel.declare_exchange(
        TASK_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )

    for priority, queue_name in QUEUE_BY_PRIORITY.items():
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
            },
        )
        await queue.bind(task_exchange, routing_key=ROUTING_KEY_BY_PRIORITY[priority])

    return task_exchange
