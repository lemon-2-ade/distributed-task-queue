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

Phase 12 adds three things on top of all of the above, all powered
by the same MessageHandler object (services/worker/consumer.py):
- **Timeouts**: handled entirely inside consumer.py's handle() --
  nothing here.
- **Cancellation**: this process runs a background task
  (`_run_cancellation_listener`) subscribed to coordination/
  cancellation.py's Pub/Sub channel. A cancellation request naming a
  task_id this worker is currently running (checked via
  `message_handler.running_tasks`) gets `cancel_task()` called on it;
  requests for tasks this worker doesn't own are silently ignored,
  since every worker sees every request on the shared channel.
- **Graceful shutdown**: SIGTERM now cancels the AMQP consumers
  (stops *new* deliveries) before anything else, then calls
  `message_handler.wait_for_drain()` to give in-flight handlers up to
  WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS to finish **on their own**
  before closing any connections. Closing the RabbitMQ connection out
  from under a still-running handler would drop that handler's
  eventual ack/nack entirely, which is exactly the kind of silent
  task loss this whole project exists to avoid. See
  docs/graceful-shutdown.md for what happens to whatever is still
  running after the grace period elapses.
"""

import asyncio
import signal
import uuid

from config import get_settings
from coordination.cancellation import CancellationBroadcaster
from coordination.heartbeat import run_heartbeat_loop
from coordination.worker_registry import WorkerRegistry
from messaging.connection import RabbitMQConnection
from messaging.publisher import TaskPublisher
from messaging.queues import DEAD_LETTER_QUEUE, QUEUE_BY_PRIORITY
from services.worker.consumer import MessageHandler, make_dlq_handler, make_message_handler

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def _run_cancellation_listener(
    broadcaster: CancellationBroadcaster, message_handler: MessageHandler, worker_id: str
) -> None:
    async for task_id, target_worker_id in broadcaster.listen():
        if target_worker_id != worker_id:
            # Every worker sees every request on the shared channel
            # (coordination/cancellation.py) -- this is the filter
            # that makes that safe. Not an error, not logged loudly:
            # this is the expected common case on any worker that
            # isn't the one running the named task.
            continue
        message_handler.cancel_task(task_id)


async def main() -> None:
    settings = get_settings()

    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    channel = rabbitmq.channel
    assert channel is not None

    await channel.set_qos(prefetch_count=settings.worker_concurrency)

    registry = WorkerRegistry()

    publisher = TaskPublisher(rabbitmq.task_exchange)
    message_handler = make_message_handler(
        WORKER_ID, settings.worker_concurrency, publisher, registry
    )

    consumers = []
    queue_names = list(QUEUE_BY_PRIORITY.values())
    for queue_name in queue_names:
        queue = await channel.get_queue(queue_name)
        consumer_tag = await queue.consume(message_handler.handle)
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

    cancellation_broadcaster = CancellationBroadcaster()
    cancellation_listener_task = asyncio.create_task(
        _run_cancellation_listener(cancellation_broadcaster, message_handler, WORKER_ID)
    )

    print(
        f"[{WORKER_ID}] consuming from {queue_names} and {DEAD_LETTER_QUEUE}",
        flush=True,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    print(f"[{WORKER_ID}] shutdown signal received, stopping consumers", flush=True)

    # Stop accepting *new* deliveries first -- everything after this
    # point only concerns work already in flight.
    for queue, consumer_tag in consumers:
        await queue.cancel(consumer_tag)

    still_running = await message_handler.wait_for_drain(
        settings.worker_shutdown_grace_period_seconds
    )
    if still_running:
        # The grace period elapsed with handlers still going. They
        # are *not* force-cancelled here: cancelling them now would
        # mean their in-flight work (a partially-run handler,
        # possibly non-idempotent side effects) gets abandoned
        # mid-execution with no chance to even record a FAILED/
        # TIMEOUT/CANCELLED status, since the Postgres/RabbitMQ
        # connections this process needs to do that are about to be
        # closed anyway. The honest tradeoff here: a worker process
        # that's killed (not just asked to stop) after this point
        # will lose track of these tasks until their messages'
        # absence of an ack eventually triggers RabbitMQ's own
        # redelivery-on-connection-loss behavior -- see
        # docs/graceful-shutdown.md.
        print(
            f"[{WORKER_ID}] grace period elapsed with {len(still_running)} handler(s) "
            "still running -- proceeding with shutdown anyway",
            flush=True,
        )
    else:
        print(f"[{WORKER_ID}] all in-flight work drained cleanly", flush=True)

    # heartbeat_task already respects stop_event on its own (see
    # coordination/heartbeat.py) and is already exiting by this
    # point -- just await it rather than cancelling. The
    # cancellation listener, on the other hand, is an unconditional
    # `async for` over a Pub/Sub stream with no stop_event awareness
    # of its own, so it genuinely needs to be cancelled to end.
    await heartbeat_task
    cancellation_listener_task.cancel()
    try:
        await cancellation_listener_task
    except asyncio.CancelledError:
        pass

    await cancellation_broadcaster.close()
    await registry.deregister(WORKER_ID)
    await registry.close()
    await rabbitmq.close()
    print(f"[{WORKER_ID}] stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
