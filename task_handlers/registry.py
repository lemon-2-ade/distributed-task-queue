"""
The task handler registry -- the single, closed set of task types
this system will ever execute.

A message on the queue carries a `task_type` string, not code. The
worker looks that string up here and runs whatever function is
registered under it. This is the mechanism that makes "no arbitrary
code execution" an actual guarantee rather than a policy: if
`task_type` isn't a key in TASK_HANDLERS, nothing runs, full stop --
there is no path from "attacker-controlled string in a message
payload" to "attacker-chosen code executes."

`@register("name")` is how a handler joins the registry. Importing
task_handlers (see __init__.py) is what causes every decorated
function to actually run its registration -- a handler module that's
never imported is a handler that will never be found, which is a
deliberate opt-in rather than the registry scanning a directory for
anything that looks like a handler.
"""

from collections.abc import Awaitable, Callable

TaskHandler = Callable[[dict], Awaitable[dict | None]]

TASK_HANDLERS: dict[str, TaskHandler] = {}


def register(task_type: str) -> Callable[[TaskHandler], TaskHandler]:
    def decorator(fn: TaskHandler) -> TaskHandler:
        if task_type in TASK_HANDLERS:
            raise ValueError(f"task_type {task_type!r} is already registered")
        TASK_HANDLERS[task_type] = fn
        return fn

    return decorator
