"""
Worker process entrypoint.

This is a standalone, independently deployable process -- not a
thread inside the API. That's what makes horizontal scaling (Phase
6: `docker compose up --scale worker=5`) meaningful: each replica is
its own OS process with its own RabbitMQ connection and channel,
competing for messages the same way any other AMQP consumer would.

Phase 6 adds real concurrency and horizontal scaling on top of
Phase 5's single-task-at-a-time worker:
- `prefetch_count=settings.worker_concurrency`: this worker may now
  hold up to WORKER_CONCURRENCY unacknowledged messages at once,
  which is what lets aio-pika run that many message handlers as
  concurrent asyncio tasks (see consumer.py's module docstring for
  the prefetch-vs-semaphore distinction).
- Horizontal scaling is `docker compose up --scale worker=N`: each
  replica is a separate OS process with its own WORKER_ID, its own
  RabbitMQ connection, and its own asyncio event loop, all competing
  as independent consumers on the same three queues. That's *real*
  parallelism (separate processes, separate GIL each), as opposed to
  the *concurrency* WORKER_CONCURRENCY gives within one process --
  see docs/concurrency.md for why those are not the same thing and
  why this project doesn't claim otherwise.
- Consumes from all three priority queues with no ordering policy
  between them yet -- RabbitMQ just delivers from whichever queue
  has a ready message and a free consumer slot. The
  priority-drain-order policy (and its starvation tradeoff, see
  docs/rabbitmq.md) is not yet implemented.
- Retries (Phase 8) republish through this same worker's own
  TaskPublisher, on the same connection/channel it consumes with --
  see services/worker/consumer.py for the retry flow itself.
- Shutdown here cancels the consumers and closes the connection --
  it does not yet drain in-flight work, update a worker registry, or
  distinguish SIGTERM from a crash. Full graceful shutdown is Phase
  12.

Phase 10 adds the worker registry (coordination/worker_registry.py):
this worker registers itself in Redis on startup, refreshes that
entry on a timer (coordination/heartbeat.py) independent of whatever
message traffic it's handling, and explicitly deregisters on a clean
shutdown. See coordination/worker_registry.py's module docstring for
why Redis (TTL-based liveness) rather than Postgres.

Phase 11 adds load reporting: the registry's active_task_count field
is kept current by services/worker/consumer.py (incremented/
decremented around each handler invocation), which is what makes
coordination/load_balancer.py's LeastLoadedStrategy meaningful rather
than reading a field nothing ever updates. GET /workers/select on the
API side (services/api/routers/workers.py) is the only thing reading
these strategies right now -- still nothing in the dispatch path
itself uses them, same as Phase 10's "knowing who's alive isn't the
same as using it" note, now extended to "knowing how loaded they are
isn't either."
"""

import asyncio
import signal
import uuid

from config import get_settings
from coordination.heartbeat import run_heartbeat_loop
from coordination.worker_registry import WorkerRegistry
from messaging.connection import RabbitMQConnection
from messaging.publisher import TaskPublisher
from messaging.queues import DEAD_LETTER_QUEUE, QUEUE_BY_PRIORITY
from services.worker.consumer import make_dlq_handler, make_message_handler

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def main() -> None:
    settings = get_settings()

    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    channel = rabbitmq.channel
    assert channel is not None

    await channel.set_qos(prefetch_count=settings.worker_concurrency)

    registry = WorkerRegistry()

    publisher = TaskPublisher(rabbitmq.task_exchange)
    handler = make_message_handler(WORKER_ID, settings.worker_concurrency, publisher, registry)

    consumers = []
    queue_names = list(QUEUE_BY_PRIORITY.values())
    for queue_name in queue_names:
        queue = await channel.get_queue(queue_name)
        consumer_tag = await queue.consume(handler)
        consumers.append((queue, consumer_tag))

    # Every worker also consumes the DLQ (Phase 9) -- there's no
    # separate "DLQ watcher" process. With multiple worker replicas
    # (Phase 6), RabbitMQ round-robins dead_letter.queue deliveries
    # across all of them the same way it does the priority queues;
    # whichever worker gets a given DLQ message is the one that
    # marks that task DEAD_LETTERED.
    dlq_handler = make_dlq_handler()
    dlq_queue = await channel.get_queue(DEAD_LETTER_QUEUE)
    dlq_consumer_tag = await dlq_queue.consume(dlq_handler)
    consumers.append((dlq_queue, dlq_consumer_tag))

    await registry.register(
        WORKER_ID, queues=[*queue_names, DEAD_LETTER_QUEUE], concurrency=settings.worker_concurrency
    )

    stop_event = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        run_heartbeat_loop(
            registry, WORKER_ID, settings.worker_heartbeat_interval_seconds, stop_event
        )
    )

    print(
        f"[{WORKER_ID}] consuming from {queue_names} and {DEAD_LETTER_QUEUE}",
        flush=True,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    print(f"[{WORKER_ID}] shutdown signal received", flush=True)

    await heartbeat_task

    for queue, consumer_tag in consumers:
        await queue.cancel(consumer_tag)
    await registry.deregister(WORKER_ID)
    await registry.close()
    await rabbitmq.close()
    print(f"[{WORKER_ID}] stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
