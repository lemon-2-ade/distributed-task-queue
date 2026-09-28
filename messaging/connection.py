"""
RabbitMQ connection lifecycle.

Uses aio_pika.connect_robust rather than plain connect(): a "robust"
connection transparently reconnects (and re-declares any exchanges/
queues/consumers registered through it) after the broker restarts or
a network blip drops the TCP connection. Without it, a RabbitMQ
restart would permanently kill every long-lived consumer/publisher
in this system until the process itself was restarted -- which is
exactly the kind of failure this project exists to handle gracefully
rather than paper over.

One RabbitMQConnection per process (API, worker, scheduler each get
their own), created at startup and closed at shutdown -- not one
connection dialed per request. A connection has real cost (TCP +
AMQP handshake) and RabbitMQ expects long-lived connections with
many lightweight channels multiplexed over them, not one connection
per operation.
"""

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection

from config import get_settings
from messaging.topology import declare_topology


class RabbitMQConnection:
    def __init__(self) -> None:
        self.connection: AbstractRobustConnection | None = None
        self.channel: AbstractChannel | None = None
        self.task_exchange: AbstractExchange | None = None

    async def connect(self) -> None:
        settings = get_settings()
        self.connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        # publisher_confirms=True (aio-pika's default) means every
        # `exchange.publish(...)` call doesn't return until the
        # broker has acknowledged it actually has the message --
        # without that, "the await returned" would only mean "the
        # bytes left this process," not "RabbitMQ has them." See
        # docs/rabbitmq.md.
        self.channel = await self.connection.channel(publisher_confirms=True)
        self.task_exchange = await declare_topology(self.channel)

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()

    @property
    def is_connected(self) -> bool:
        return self.connection is not None and not self.connection.is_closed
