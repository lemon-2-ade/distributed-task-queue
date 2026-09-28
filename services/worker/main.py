"""
Worker process entrypoint.

This is a standalone, independently deployable process -- not a
thread inside the API. That's what makes horizontal scaling (Phase
6: `docker compose up --scale worker=5`) meaningful: each replica is
its own OS process with its own RabbitMQ connection and channel,
competing for messages the same way any other AMQP consumer would.

Phase 5 scope, deliberately narrow:
- `prefetch_count=1`: this worker holds at most one unacknowledged
  message at a time, i.e. it processes one task fully before
  RabbitMQ will hand it another. Real concurrency (many in-flight
  tasks per process via asyncio) is Phase 6.
- Consumes from all three priority queues with no ordering policy
  between them yet -- RabbitMQ just delivers from whichever queue
  has a ready message and a free consumer slot. The
  priority-drain-order policy (and its starvation tradeoff, see
  docs/rabbitmq.md) is also Phase 6.
- Shutdown here cancels the consumers and closes the connection --
  it does not yet drain in-flight work, update a worker registry, or
  distinguish SIGTERM from a crash. Full graceful shutdown is Phase
  12.
"""

import asyncio
import signal
import uuid

from messaging.connection import RabbitMQConnection
from messaging.queues import QUEUE_BY_PRIORITY
from services.worker.consumer import make_message_handler

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def main() -> None:
    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    channel = rabbitmq.channel
    assert channel is not None

    await channel.set_qos(prefetch_count=1)

    handler = make_message_handler(WORKER_ID)

    consumers = []
    for queue_name in QUEUE_BY_PRIORITY.values():
        queue = await channel.get_queue(queue_name)
        consumer_tag = await queue.consume(handler)
        consumers.append((queue, consumer_tag))

    print(f"[{WORKER_ID}] consuming from {list(QUEUE_BY_PRIORITY.values())}", flush=True)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    print(f"[{WORKER_ID}] shutdown signal received", flush=True)

    for queue, consumer_tag in consumers:
        await queue.cancel(consumer_tag)
    await rabbitmq.close()
    print(f"[{WORKER_ID}] stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
