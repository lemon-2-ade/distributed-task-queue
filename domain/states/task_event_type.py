"""Event types recorded to task_events -- the append-only audit log
of everything that happened to a task, independent of (and more
granular than) its current `status` column. See docs/task-lifecycle.md."""

from enum import StrEnum


class TaskEventType(StrEnum):
    TASK_CREATED = "TASK_CREATED"
    TASK_QUEUED = "TASK_QUEUED"
    TASK_STARTED = "TASK_STARTED"
    TASK_RETRYING = "TASK_RETRYING"
    TASK_FAILED = "TASK_FAILED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_CANCELLED = "TASK_CANCELLED"
    TASK_TIMEOUT = "TASK_TIMEOUT"
    TASK_DEAD_LETTERED = "TASK_DEAD_LETTERED"
    # Phase 13: recorded (not a status change) when a redelivered
    # message arrives for a task that's already RUNNING -- see
    # persistence/state_manager.py's record_duplicate_delivery() and
    # docs/idempotency.md.
    DUPLICATE_DELIVERY_DETECTED = "DUPLICATE_DELIVERY_DETECTED"
