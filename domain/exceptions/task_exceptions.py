"""Domain-level exceptions -- framework-free, so persistence and API
code can both raise/catch them without either depending on the other."""

import uuid

from domain.states.task_status import TaskStatus


class InvalidStateTransitionError(Exception):
    def __init__(self, task_id: uuid.UUID, from_status: TaskStatus, to_status: TaskStatus) -> None:
        self.task_id = task_id
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"task {task_id}: cannot transition {from_status.value} -> {to_status.value}"
        )


class PermanentTaskError(Exception):
    """
    A task handler raises this instead of a plain Exception to say
    "this failure will never succeed no matter how many times you
    retry it" (e.g. the payload fails validation, a business rule
    rejects it outright). The worker treats it as non-retryable
    regardless of remaining retry budget -- see
    services/worker/consumer.py. Any other exception is assumed
    transient (a network blip, a timeout, a dependency that might
    recover) and is retried up to the task's max_retries.
    """
