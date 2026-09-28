"""Demonstration handler: sleeps for `payload["seconds"]` (default 1).

Exists to make queue-wait-time vs execution-time and worker
concurrency (Phase 6) visible/demonstrable -- a handler that returns
instantly can't show a worker being "busy."
"""

import asyncio

from task_handlers.registry import register


@register("sleep")
async def sleep_task(payload: dict) -> dict:
    seconds = float(payload.get("seconds", 1))
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}
