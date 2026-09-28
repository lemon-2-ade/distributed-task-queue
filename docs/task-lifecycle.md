# Task lifecycle

## The state machine

```
PENDING
   |
   v
QUEUED
   |
   v
RUNNING
  /   \
 /     \
SUCCESS FAILED
          |
          v
       RETRYING
          |
          v
       QUEUED  (loop back into RUNNING on next pickup)

After retry exhaustion (Phase 8/9):
FAILED / TIMEOUT
  |
  v
DEAD_LETTERED

Cancellable from PENDING, QUEUED, or RUNNING -> CANCELLED
RUNNING can also end in TIMEOUT -> RETRYING or DEAD_LETTERED
```

The full transition table lives in code, not just in this doc:
`domain/states/transitions.py`'s `VALID_TRANSITIONS`. That's the
single source of truth `persistence.state_manager.TaskStateManager`
consults on every status change -- this diagram is a picture of that
table, not an independent description that could drift from it.

## Who's allowed to change a task's status

Nothing except `TaskStateManager.transition()` (and
`.record_creation()` for the very first, pre-PENDING-history event).
Before Phase 7, the worker and the API each called
`TaskRepository.update_status()` directly, which writes the column
but does nothing else -- no validation, no audit trail. That's still
available (see its docstring) for call sites that genuinely don't
need lifecycle tracking, but the worker and `TaskService` no longer
use it for that reason: every real status change now goes through
one place that enforces the state machine and keeps
`task_events`/`task_attempts` in sync automatically.

## task_attempts vs task_events

Two different questions, two different tables:

- **task_attempts**: "how many times has this run, and what
  happened each time?" One row per RUNNING episode. Opened when a
  task enters RUNNING (`attempt_number = retry_count + 1`), closed
  (`completed_at`, final `status`, `error`) when it leaves RUNNING
  for any terminal-for-that-attempt state (SUCCESS, FAILED, TIMEOUT,
  CANCELLED).
- **task_events**: "show me everything that happened to this task,
  in order." One append-only row per state-machine transition (plus
  the initial TASK_CREATED). This is what `GET /tasks/{id}/events`
  reads. It's strictly finer-grained than `task_attempts` -- e.g.
  TASK_QUEUED has no corresponding attempt row, since nothing ran
  yet.

Both are written inside the same transaction as the `Task.status`
update itself, via one `session.flush()` inside `transition()` --
not as separate, later writes -- so a crash can't leave the task's
current status out of sync with its own history.

## Why validate transitions at all

Without `is_valid_transition()`, a bug anywhere that calls
`transition(task_id, some_status)` with the wrong status (a typo, a
stale reference to an old workflow, a race between two code paths)
would silently corrupt a task's history -- e.g. a completed task
"restarting" into RUNNING, or a cancelled task somehow reaching
SUCCESS. `InvalidStateTransitionError` turns that class of bug into
an exception raised at the moment it would happen, with the illegal
`from -> to` pair in the message, instead of a row in the database
that quietly stops making sense.
