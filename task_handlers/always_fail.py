"""Demonstration handler: always raises. Exists purely so retries and
the dead-letter queue can be exercised live end-to-end -- not
something a real system would ship, but the project needs *some* way
to watch FAILED -> RETRYING -> ... -> DEAD_LETTERED happen for real
rather than only in tests."""

from task_handlers.registry import register


@register("always_fail")
async def always_fail_task(payload: dict) -> dict:
    raise RuntimeError(payload.get("message", "this task always fails (demo handler)"))
