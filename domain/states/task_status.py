"""
The task state machine.

PENDING -> QUEUED -> RUNNING -> SUCCESS
                              -> FAILED -> RETRYING -> QUEUED (loop)
                                        -> DEAD_LETTERED (retries exhausted)
QUEUED  -> CANCELLED   (cancelled before a worker picked it up)
RUNNING -> CANCELLED   (cooperative cancellation while executing)
RUNNING -> TIMEOUT     (exceeded its timeout; may itself retry or DLQ)

This enum is intentionally framework-free (no SQLAlchemy/Pydantic
import here) -- it's a domain concept that the persistence layer,
the API schemas, and the worker all need to agree on, and none of
them should be the "owner" of what a valid state is.
"""

from enum import StrEnum


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    DEAD_LETTERED = "DEAD_LETTERED"


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCESS,
        TaskStatus.CANCELLED,
        TaskStatus.DEAD_LETTERED,
    }
)
