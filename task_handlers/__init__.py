"""
Importing this package registers every built-in task handler (the
submodule imports below run each module's @register(...) decorator
as a side effect). Anything that needs to look up a handler by
task_type imports TASK_HANDLERS from here, not from the individual
handler modules, so the registry is always fully populated.
"""

from task_handlers import echo, sleep  # noqa: F401
from task_handlers.registry import TASK_HANDLERS

__all__ = ["TASK_HANDLERS"]
