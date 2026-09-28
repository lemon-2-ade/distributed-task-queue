"""
The state machine's edges -- the only place that decides whether
moving a task from one status to another is legal.

    PENDING -> QUEUED -> RUNNING -> SUCCESS
                                  -> FAILED -> RETRYING -> QUEUED (loop)
                                            -> DEAD_LETTERED
                        -> TIMEOUT -> RETRYING -> QUEUED (loop)
                                   -> DEAD_LETTERED
    PENDING -> CANCELLED
    QUEUED  -> CANCELLED
    RUNNING -> CANCELLED

Everything that changes Task.status in this codebase should go
through persistence.state_manager.TaskStateManager, which consults
this table, rather than a repository or route setting .status
directly -- otherwise "is PENDING -> SUCCESS legal" has to be
re-derived (and could be gotten wrong differently) at every call
site instead of being answerable in exactly one place.
"""

from domain.states.task_status import TaskStatus

VALID_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELLED}),
    TaskStatus.QUEUED: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED}
    ),
    TaskStatus.FAILED: frozenset({TaskStatus.RETRYING, TaskStatus.DEAD_LETTERED}),
    TaskStatus.TIMEOUT: frozenset({TaskStatus.RETRYING, TaskStatus.DEAD_LETTERED}),
    TaskStatus.RETRYING: frozenset({TaskStatus.QUEUED}),
    # Terminal for the automatic system -- SUCCESS and CANCELLED have
    # no outgoing edges at all. DEAD_LETTERED has exactly one: a
    # human administrator can explicitly requeue a dead-lettered task
    # (POST /tasks/{id}/retry, Phase 9). That's a deliberate,
    # single-purpose exception to "terminal means terminal" -- every
    # other transition in this table is something the system decides
    # on its own; this one only happens when a person asks for it.
    TaskStatus.SUCCESS: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.DEAD_LETTERED: frozenset({TaskStatus.QUEUED}),
}


def is_valid_transition(from_status: TaskStatus, to_status: TaskStatus) -> bool:
    return to_status in VALID_TRANSITIONS.get(from_status, frozenset())
