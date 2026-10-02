"""
Worker process entrypoint.

This is a standalone, independently deployable process -- not a
thread inside the API. That's what makes horizontal scaling (Phase
6: `docker compose up --scale worker=5`) meaningful: each replica is
its own OS process with its own RabbitMQ connection and channel,
competing for messages the same way any other AMQP consumer would.

Phase 6 adds real concurrency and horizontal scaling on top of
Phase 5's single-task-at-a-time worker:
- Horizontal scaling is `docker compose up --scale worker=N`: each
  replica is a separate OS process with its own WORKER_ID, its own
  RabbitMQ connection, and its own asyncio event loop, all competing
  as independent consumers on the same three queues. That's *real*
  parallelism (separate processes, separate GIL each), as opposed to
  the *concurrency* WORKER_CONCURRENCY gives within one process --
  see docs/concurrency.md for why those are not the same thing and
  why this project doesn't claim otherwise.
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
than reading a field nothing ever updates.

Phase 12 adds timeouts, cancellation (a background Pub/Sub listener,
`_run_cancellation_listener`), and graceful shutdown (drain in-flight
work before closing connections) -- see services/worker/consumer.py
and docs/timeouts-and-cancellation.md / docs/graceful-shutdown.md.

Phase 15 replaces how this worker consumes the three priority queues
entirely. Through Phase 14, each priority queue had its own
automatic, broker-pushed consumer
(`queue.consume(message_handler.handle)`) -- RabbitMQ decided which
of the three delivered a message whenever this worker had a free
prefetch slot, with no actual priority ordering enforced (this
module's own docstring flagged this gap from Phase 6 onward).
`_run_priority_dispatch_loop` replaces that with an explicit *pull*
loop: on every free concurrency slot, it asks
`scheduling/priority_scheduler.py`'s `WeightedQueueSelector` which
queue to check first, falls through to the others (by weight) if
that one's empty, and only then dispatches a handler. This is what
actually makes "high priority before normal before low, without
starving normal/low" true, instead of aspirational -- see
docs/priority-fairness.md.

The DLQ consumer is deliberately NOT part of this -- it keeps its
original push-based `queue.consume()` subscription (Phase 9). DLQ
traffic is exceptional, not part of the normal three-way priority
split this phase is about, and giving it its own independent
consumer keeps it simple and unaffected by this change.
"""

import asyncio
import signal
import uuid

from config import get_settings
from coordination.cancellation import CancellationBroadcaster
from coordination.heartbeat import run_heartbeat_loop
from coordination.worker_registry import WorkerRegistry
from domain.states import TaskPriority
from messaging.connection import RabbitMQConnection
from messaging.publisher import TaskPublisher
from messaging.queues import DEAD_LETTER_QUEUE, QUEUE_BY_PRIORITY
from scheduling.priority_scheduler import WeightedQueueSelector
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


async def _run_priority_dispatch_loop(
    queues: dict[TaskPriority, object],
    selector: WeightedQueueSelector,
    message_handler: MessageHandler,
    dispatch_semaphore: asyncio.Semaphore,
    idle_sleep_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    """
    One iteration = one decision of "what should this worker start
    on next." Gated by `dispatch_semaphore` (acquired *before*
    pulling a message, released once that message's `handle()` call
    finishes): this is the pull-model's replacement for what AMQP
    `prefetch_count` gave the old push-based consumers -- a hard
    ceiling on how many messages this worker will ever hold
    unacknowledged at once, so it can't out-pace its own processing
    capacity and buffer an unbounded number of fetched-but-unstarted
    messages in memory. `MessageHandler` has its own internal
    semaphore too (services/worker/consumer.py) -- that one still
    bounds "how many handler bodies execute concurrently." The two
    are deliberately redundant at the same concurrency number rather
    than unified into one: this loop's semaphore is about *pull
    rate*, the other is about *execution rate*, and keeping them
    separate means neither implementation has to know about the
    other's existence.
    """
    while not stop_event.is_set():
        await dispatch_semaphore.acquire()
        if stop_event.is_set():
            dispatch_semaphore.release()
            return

        preferred = selector.next_priority()
        message = await queues[preferred].get(no_ack=False, fail=False)
        if message is None:
            for fallback in selector.fallback_order(preferred):
                message = await queues[fallback].get(no_ack=False, fail=False)
                if message is not None:
                    break

        if message is None:
            # Every queue was empty on this turn. Release the slot
            # we never used and back off briefly rather than
            # immediately looping back into another all-empty round
            # -- an idle worker polling three empty queues as fast
            # as the event loop allows would otherwise burn CPU and
            # spam RabbitMQ with no benefit.
            dispatch_semaphore.release()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=idle_sleep_seconds)
            except asyncio.TimeoutError:
                pass
            continue

        async def _run_and_release(msg=message) -> None:
            try:
                await message_handler.handle(msg)
            finally:
                dispatch_semaphore.release()

        asyncio.ensure_future(_run_and_release())


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

    queue_names = list(QUEUE_BY_PRIORITY.values())
    priority_queues = {
        priority: await channel.get_queue(name) for priority, name in QUEUE_BY_PRIORITY.items()
    }

    selector = WeightedQueueSelector(
        {
            TaskPriority.HIGH: settings.priority_weight_high,
            TaskPriority.NORMAL: settings.priority_weight_normal,
            TaskPriority.LOW: settings.priority_weight_low,
        }
    )
    dispatch_semaphore = asyncio.Semaphore(settings.worker_concurrency)

    # Every worker also consumes the DLQ (Phase 9) -- there's no
    # separate "DLQ watcher" process. With multiple worker replicas
    # (Phase 6), RabbitMQ round-robins dead_letter.queue deliveries
    # across all of them the same way it does the priority queues;
    # whichever worker gets a given DLQ message is the one that
    # marks that task DEAD_LETTERED. Still push-based, unlike the
    # priority queues as of Phase 15 -- see this module's docstring.
    dlq_handler = make_dlq_handler()
    dlq_queue = await channel.get_queue(DEAD_LETTER_QUEUE)
    dlq_consumer_tag = await dlq_queue.consume(dlq_handler)

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

    dispatch_loop_task = asyncio.create_task(
        _run_priority_dispatch_loop(
            priority_queues,
            selector,
            message_handler,
            dispatch_semaphore,
            settings.priority_poll_idle_sleep_seconds,
            stop_event,
        )
    )

    print(
        f"[{WORKER_ID}] consuming from {queue_names} (weighted priority dispatch) "
        f"and {DEAD_LETTER_QUEUE}",
        flush=True,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    print(f"[{WORKER_ID}] shutdown signal received, stopping dispatch", flush=True)

    # Stop accepting *new* deliveries first -- everything after this
    # point only concerns work already in flight. The dispatch loop
    # is cancelled outright rather than awaited cooperatively: if
    # every concurrency slot is currently busy, the loop is sitting
    # inside `await dispatch_semaphore.acquire()`, which only returns
    # once a slot frees up -- cooperatively waiting for that here
    # would make shutdown's timing depend on how long the *next*
    # in-flight handler happens to take, defeating the whole point of
    # a bounded WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS below. Cancelling
    # interrupts the acquire() immediately; the one slot it might
    # have just been about to use was never actually claimed, so
    # nothing here leaks a permit. The DLQ consumer still needs its
    # own explicit cancel since it's a genuine AMQP consumer tag, not
    # part of this loop.
    dispatch_loop_task.cancel()
    try:
        await dispatch_loop_task
    except asyncio.CancelledError:
        pass
    await dlq_queue.cancel(dlq_consumer_tag)

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
