"""
Outbox relay process entrypoint (Phase 17).

A fourth standalone, independently deployable process, alongside the
API, worker, and scheduler -- `docker compose up --scale
outbox_relay=N` is meant to be safe for the same reason scaling the
scheduler is: every replica runs the identical poll loop below, and
OutboxRepository.claim_unpublished() (FOR UPDATE SKIP LOCKED, the
same Postgres pattern persistence/repositories/task_repository.py
uses for the scheduler) is what makes concurrent replicas relay
disjoint sets of outbox rows without coordinating with each other.

This is the only process in the entire system that still constructs
a TaskPublisher and connects to RabbitMQ to publish a message. Every
other write path (API task creation, manual DLQ retry, worker-side
automatic retry, scheduler dispatch) writes an OutboxMessage row
instead and stops there -- see docs/outbox.md for why centralizing
the actual publish here, behind the outbox table, is what closes the
dual-write gap that existed at every one of those call sites through
Phase 16.
"""

import asyncio
import signal

from config import get_settings
from messaging.connection import RabbitMQConnection
from messaging.publisher import TaskPublisher
from services.outbox_relay.relay import relay_once


async def main() -> None:
    settings = get_settings()

    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    publisher = TaskPublisher(rabbitmq.task_exchange)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    print(
        f"[outbox-relay] polling every {settings.outbox_relay_poll_interval_seconds}s, "
        f"batch size {settings.outbox_relay_batch_size}",
        flush=True,
    )

    while not stop_event.is_set():
        relayed = await relay_once(publisher, limit=settings.outbox_relay_batch_size)
        if relayed:
            print(f"[outbox-relay] relayed {relayed} message(s)", flush=True)

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=settings.outbox_relay_poll_interval_seconds
            )
        except asyncio.TimeoutError:
            pass

    print("[outbox-relay] shutdown signal received", flush=True)
    await rabbitmq.close()
    print("[outbox-relay] stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
