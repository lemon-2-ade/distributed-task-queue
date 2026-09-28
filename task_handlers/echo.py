"""Demonstration handler: returns its payload unchanged. Useful for
verifying the pipeline end-to-end without any real side effects."""

from task_handlers.registry import register


@register("echo")
async def echo_task(payload: dict) -> dict:
    return {"echo": payload}
