"""
Scheduler process entrypoint (Phase 16; Phase 17 drops its RabbitMQ
dependency entirely).

A third standalone, independently deployable process alongside the
API and worker (see services/worker/main.py's module docstring for
why a separate process rather than a thread/background task bolted
onto one of the others) -- `docker compose up --scale scheduler=N`
is meant to be safe for the same reason scaling workers is: every
replica runs the identical poll loop below, and
persistence/repositories/task_repository.py's
claim_due_scheduled_tasks() (FOR UPDATE SKIP LOCKED) is what makes
concurrent replicas claim disjoint sets of due tasks without needing
to coordinate with each other at all -- no leader election, no
distributed lock, no registry of "which scheduler owns which task."
See docs/scheduling.md.

Deliberately NOT named to collide with scheduling/priority_scheduler.py
(Phase 15's WeightedQueueSelector, used inside the worker's own
message-dispatch loop to keep high priority from starving low) --
that's a same-process, in-memory fairness policy over *already-queued*
RabbitMQ messages. This module is a separate process whose job is
entirely about Postgres: noticing when a *not-yet-queued* task's
scheduled_at has arrived and queuing it for the first time. Two
different problems that happen to both have "schedul-" in the name;
see docs/scheduling.md for the explicit disambiguation.

This process has no per-message work to drain on shutdown the way a
worker does (services/worker/main.py's wait_for_drain) -- a poll
cycle either hasn't started (nothing to wait for) or has already
committed as a complete unit by the time anything could be
interrupted (see dispatcher.py). SIGTERM here just stops the loop
from starting another cycle.

Phase 17 removes this process's RabbitMQConnection/TaskPublisher
entirely: claiming a due task now ends with writing an outbox row
(in the same Postgres transaction as the QUEUED status change) rather
than publishing directly -- see dispatcher.py and docs/outbox.md.
This process is Postgres-only now; services/outbox_relay/ is the only
thing left that connects to RabbitMQ to actually publish.
"""

import asyncio
import signal

from config import get_settings
from services.scheduler.dispatcher import claim_and_dispatch_due_tasks


async def main() -> None:
    settings = get_settings()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    print(
        f"[scheduler] polling every {settings.scheduler_poll_interval_seconds}s, "
        f"batch size {settings.scheduler_batch_size}",
        flush=True,
    )

    while not stop_event.is_set():
        dispatched = await claim_and_dispatch_due_tasks(limit=settings.scheduler_batch_size)
        if dispatched:
            print(f"[scheduler] dispatched {dispatched} due task(s)", flush=True)

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=settings.scheduler_poll_interval_seconds
            )
        except asyncio.TimeoutError:
            pass

    print("[scheduler] shutdown signal received, stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
