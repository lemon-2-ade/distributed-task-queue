"""
Background heartbeat loop, run as its own asyncio task alongside a
worker's message consumers (see services/worker/main.py).

Why a separate loop instead of piggybacking the heartbeat on message
processing: a worker with no messages in flight -- an idle worker
waiting on empty queues -- would never touch Redis at all if
heartbeating only happened as a side effect of handling a message,
and would look dead (TTL-expired) despite being perfectly healthy
and ready to pick up the next task. Liveness has to be reported on
its own clock, independent of whether there's work to do.
"""

import asyncio

from coordination.worker_registry import WorkerRegistry


async def run_heartbeat_loop(
    registry: WorkerRegistry, worker_id: str, interval_seconds: int, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        try:
            await registry.heartbeat(worker_id)
        except Exception:
            # A single missed heartbeat (a transient Redis blip) is
            # not worth crashing the whole worker process over -- the
            # TTL margin over the interval (see worker_registry.py)
            # exists precisely to absorb this. It just tries again
            # next tick; if Redis stays down past the TTL window,
            # the registry entry expires and the worker correctly
            # stops being counted as active until it recovers.
            pass

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
