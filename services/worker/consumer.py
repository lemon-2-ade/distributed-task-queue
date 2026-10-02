"""
Turns a raw RabbitMQ message into: look up the handler, run it,
persist the result through the task state machine, then ack --
never the other order. See docs/rabbitmq.md ("why ACK timing
matters"): acking before the terminal state is durably written would
let a worker crash between those two steps silently lose the task,
because RabbitMQ would already consider it delivered-and-done.

Every status change goes through persistence.state_manager.
TaskStateManager rather than writing Task.status directly, so every
transition is validated against the state machine and recorded to
task_events/task_attempts automatically.

Retries (Phase 8): a handler failure is one of two things --
1. A `PermanentTaskError`: the handler is telling us this will never
   succeed (bad payload, a rejected business rule). No retry,
   regardless of remaining budget.
2. Any other exception: assumed transient. If `retry_count <
   max_retries`, the task goes FAILED -> RETRYING (recording the
   attempt and incrementing retry_count), this worker sleeps for a
   jittered exponential backoff (domain/retry_policy.py), then
   RETRYING -> QUEUED and a *new* message is published for the next
   attempt -- only then is the *original* message acked. If retries
   are exhausted, the task stays FAILED and the original message is
   nacked with requeue=False, which (via the dead-letter-exchange
   argument on every priority queue) sends it to the DLQ. Nothing
   marks it DEAD_LETTERED yet -- Phase 9 adds a consumer that
   watches the DLQ and does that; this phase only gets the message
   there.

Load tracking (Phase 11): every in-flight message bumps this
worker's active_task_count in the registry (coordination/
worker_registry.py) for the duration it holds a semaphore slot,
decremented in a `finally` so it's accurate even on every exit path
above (parse success but task-not-found, no handler, success,
failure-into-retry, failure-into-DLQ). This is what makes
LeastLoadedStrategy's notion of "load" (coordination/load_balancer.py)
correspond to real, current work rather than something stale or
inferred.

Deliberate tradeoff, not an oversight: sleeping for the backoff delay
happens *inside* this handler, while holding both the semaphore slot
(Phase 6) and the original message unacked. That means a retrying
task occupies one of this worker's WORKER_CONCURRENCY slots for the
whole backoff window instead of freeing it up immediately -- the
alternative (a broker-side delay queue using per-message TTL + a
dead-letter-exchange trick, since core RabbitMQ has no native
"deliver this message in N seconds") adds real complexity and its
own gotcha (RabbitMQ only inspects a queue's *head* for TTL expiry,
so variable per-message TTLs in one queue don't expire strictly in
TTL order). This project takes the simpler, fully-in-application-code
option and documents the cost rather than building a delay-queue
mechanism whose edge cases would need their own explanation. See
docs/retries.md.

Concurrency (Phase 6): aio-pika already invokes this callback as its
own asyncio task per delivered message, so multiple messages are
"in flight" simultaneously whenever prefetch_count > 1 -- see
services/worker/main.py. The semaphore below is a second, explicit,
application-level bound on how many handler bodies may actually be
executing at once.

Timeouts and cancellation (Phase 12): the actual `await handler(...)`
call is wrapped in its own `asyncio.Task`, tracked in
`MessageHandler.running_tasks` keyed by task_id. That one extra layer
of indirection is what makes both features possible:
- **Timeout**: if `Task.timeout` is set, the handler task is awaited
  through `asyncio.wait_for(..., timeout=...)`. If it fires,
  `asyncio.wait_for` cancels the handler task itself and raises
  `TimeoutError` here -- caught distinctly from a normal exception
  and routed through the same retry/DLQ machinery as a FAILED task,
  just tagged TIMEOUT instead (see `_handle_terminal_failure`).
- **Cancellation**: `services/worker/main.py` runs a background
  listener on `coordination/cancellation.py`'s Pub/Sub channel; when
  a cancellation request for a task_id this worker currently owns
  arrives, it calls `MessageHandler.cancel_task()`, which cancels
  that specific handler task. The `await` on it here then raises
  `asyncio.CancelledError`, caught distinctly from both of the above
  and transitioned straight to CANCELLED -- no retry, since this
  wasn't a failure, it was a deliberate stop.
See docs/timeouts-and-cancellation.md for the full design and the
races this has to account for.

Every `handle()` invocation also registers itself (via
`asyncio.current_task()`) in `MessageHandler.in_flight`, which is
what `services/worker/main.py`'s graceful shutdown drains on SIGTERM
before closing any connections -- see
docs/graceful-shutdown.md.
"""

import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from config import get_settings
from coordination.worker_registry import WorkerRegistry
from domain.exceptions import InvalidStateTransitionError, PermanentTaskError
from domain.retry_policy import compute_backoff_seconds
from domain.states import TaskPriority, TaskStatus
from messaging.publisher import TaskPublisher
from persistence.database import AsyncSessionLocal
from persistence.models import Task
from persistence.state_manager import TaskStateManager
from task_handlers import TASK_HANDLERS


class MessageHandler:
    """
    Bundles the per-worker state that Phase 12 needs beyond what a
    bare closure could hold cleanly: a live map of task_id -> the
    asyncio.Task actually running that task's handler right now
    (`running_tasks`, used by cancel_task()), and the set of every
    handle() invocation currently in progress at all, including ones
    still waiting on the semaphore (`in_flight`, used by
    wait_for_drain() during graceful shutdown).
    """

    def __init__(
        self, worker_id: str, concurrency: int, publisher: TaskPublisher, registry: WorkerRegistry
    ) -> None:
        self.worker_id = worker_id
        self._publisher = publisher
        self._registry = registry
        self._semaphore = asyncio.Semaphore(concurrency)
        self._settings = get_settings()
        self.running_tasks: dict[uuid.UUID, asyncio.Task] = {}
        self.in_flight: set[asyncio.Task] = set()

    def cancel_task(self, task_id: uuid.UUID) -> bool:
        """Called from the cancellation listener (main.py) when a
        request names a task_id this worker is currently running.
        Returns whether there was anything to cancel -- False just
        means the task already finished or was never here, which is
        a normal, harmless race (see docs/timeouts-and-cancellation.md)."""
        handler_task = self.running_tasks.get(task_id)
        if handler_task is not None and not handler_task.done():
            handler_task.cancel()
            return True
        return False

    async def wait_for_drain(self, timeout: float) -> set[asyncio.Task]:
        """Waits up to `timeout` seconds for every currently in-flight
        handle() call to finish on its own. Returns whatever is still
        running when the timeout elapses, so the caller can decide
        what to do about it (see services/worker/main.py)."""
        tasks = [t for t in self.in_flight if not t.done()]
        if not tasks:
            return set()
        _done, pending = await asyncio.wait(tasks, timeout=timeout)
        return pending

    async def handle(self, message: AbstractIncomingMessage) -> None:
        current = asyncio.current_task()
        assert current is not None
        self.in_flight.add(current)
        try:
            await self._handle(message)
        finally:
            self.in_flight.discard(current)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        async with self._semaphore:
            try:
                body = json.loads(message.body)
                task_id = uuid.UUID(body["task_id"])
                task_type = body["task_type"]
                payload = body["payload"]
                priority = TaskPriority(body["priority"])
            except Exception:
                # Can't even parse this message -- definitely not
                # something a retry would fix, and not a real task
                # this worker is "working on," so it never counts
                # toward active_task_count below.
                await message.nack(requeue=False)
                return

            # Bracket everything from here on with the load counter:
            # this message now occupies one of this worker's
            # WORKER_CONCURRENCY slots for real, for as long as it
            # takes to reach an ack/nack (including any retry-backoff
            # sleep in _handle_terminal_failure) -- see
            # coordination/worker_registry.py's increment_load()/
            # decrement_load() docstrings for why this is a Redis
            # HINCRBY rather than a plain counter, and why the
            # decrement side guards against going negative.
            await self._registry.increment_load(self.worker_id)
            try:
                try:
                    task = await _transition(task_id, TaskStatus.RUNNING, worker_id=self.worker_id)
                except ValueError:
                    # No matching task row: a stray or duplicate-delivered
                    # message referencing a task we don't know about.
                    await message.nack(requeue=False)
                    return
                except InvalidStateTransitionError:
                    # The task exists but isn't QUEUED/PENDING any
                    # more by the time this delivery arrived -- the
                    # one real case this covers today is an admin
                    # cancelling a PENDING/QUEUED task
                    # (services/api/services/task_service.py): the
                    # task is already CANCELLED, but RabbitMQ has no
                    # way to recall the message that was already
                    # published before that happened. Nothing to run;
                    # just ack and move on. See
                    # docs/timeouts-and-cancellation.md.
                    await message.ack()
                    return

                handler = TASK_HANDLERS.get(task_type)
                if handler is None:
                    await _transition(
                        task_id,
                        TaskStatus.FAILED,
                        error=f"no handler registered for task_type={task_type!r}",
                    )
                    await message.nack(requeue=False)
                    return

                handler_task = asyncio.ensure_future(handler(payload))
                self.running_tasks[task_id] = handler_task
                try:
                    if task.timeout:
                        result = await asyncio.wait_for(handler_task, timeout=task.timeout)
                    else:
                        result = await handler_task
                except asyncio.TimeoutError:
                    # wait_for already cancelled handler_task for us.
                    await _handle_terminal_failure(
                        message,
                        task_id=task_id,
                        task_type=task_type,
                        payload=payload,
                        priority=priority,
                        error=TimeoutError(f"task exceeded its {task.timeout}s timeout"),
                        failure_status=TaskStatus.TIMEOUT,
                        publisher=self._publisher,
                        settings=self._settings,
                    )
                    return
                except asyncio.CancelledError:
                    # Only reached when *this* handler_task was the
                    # thing cancelled (cancel_task(), driven by the
                    # Pub/Sub listener in main.py) -- not when the
                    # outer handle() task itself is cancelled by
                    # worker shutdown, which doesn't touch
                    # running_tasks at all. No retry: this is a
                    # deliberate stop, not a failure.
                    await _transition(
                        task_id,
                        TaskStatus.CANCELLED,
                        error="cancelled by administrator",
                    )
                    await message.ack()
                    return
                except Exception as exc:
                    await _handle_terminal_failure(
                        message,
                        task_id=task_id,
                        task_type=task_type,
                        payload=payload,
                        priority=priority,
                        error=exc,
                        failure_status=TaskStatus.FAILED,
                        publisher=self._publisher,
                        settings=self._settings,
                    )
                    return
                finally:
                    self.running_tasks.pop(task_id, None)

                await _transition(task_id, TaskStatus.SUCCESS, result=result)
                await message.ack()
            finally:
                await self._registry.decrement_load(self.worker_id)


def make_message_handler(
    worker_id: str, concurrency: int, publisher: TaskPublisher, registry: WorkerRegistry
) -> MessageHandler:
    """
    Returns a MessageHandler bound to this worker's id (recorded on
    every attempt), a concurrency limit of at most `concurrency`
    handlers running at once, a `publisher` used to republish a task
    for its next attempt after a transient failure, and a `registry`
    used to report this worker's current load (Phase 11). Pass
    `.handle` as the aio-pika consumer callback; keep the returned
    object itself around too, for `.cancel_task()` (Phase 12
    cancellation) and `.wait_for_drain()` (Phase 12 graceful
    shutdown) -- see services/worker/main.py.
    """
    return MessageHandler(worker_id, concurrency, publisher, registry)


async def _handle_terminal_failure(
    message: AbstractIncomingMessage,
    *,
    task_id: uuid.UUID,
    task_type: str,
    payload: dict,
    priority: TaskPriority,
    error: Exception,
    failure_status: TaskStatus,
    publisher: TaskPublisher,
    settings,
) -> None:
    """
    Shared by two distinct causes that nonetheless follow the exact
    same retry-or-dead-letter logic: a handler raising an exception
    (`failure_status=FAILED`, Phase 8) and a handler exceeding its
    `Task.timeout` (`failure_status=TIMEOUT`, Phase 12). Both FAILED
    and TIMEOUT have identical outgoing edges in the state machine
    (-> RETRYING or -> DEAD_LETTERED, domain/states/transitions.py),
    which is exactly what makes sharing this function correct rather
    than coincidental: from the retry policy's point of view, "it
    raised" and "it ran too long" are the same kind of transient
    problem, worth the same backoff-and-retry treatment, up to the
    same retry budget.
    """
    task = await _transition(task_id, failure_status, error=str(error))

    is_permanent = isinstance(error, PermanentTaskError)
    retries_remaining = task.retry_count < task.max_retries

    if is_permanent or not retries_remaining:
        # Exhausted or unretryable: leave it FAILED/TIMEOUT, send the
        # message to the DLQ. See this module's docstring for why
        # nothing marks it DEAD_LETTERED yet.
        await message.nack(requeue=False)
        return

    next_attempt = task.retry_count + 1
    delay_seconds = compute_backoff_seconds(
        next_attempt,
        base_delay=settings.retry_base_delay_seconds,
        max_delay=settings.retry_max_delay_seconds,
        jitter_fraction=settings.retry_jitter_fraction,
    )

    await _transition(task_id, TaskStatus.RETRYING, retry_count=next_attempt)

    await asyncio.sleep(delay_seconds)

    await _transition(task_id, TaskStatus.QUEUED)
    await publisher.publish_task(
        task_id=task_id, task_type=task_type, payload=payload, priority=priority
    )
    # Only now: the original message's work (recording the failure
    # and publishing its replacement) is durably done, so it's safe
    # to ack it. Acking earlier and then crashing mid-backoff-sleep
    # would silently lose the retry -- the same "why ACK timing
    # matters" reasoning as the success path.
    await message.ack()


async def _transition(task_id: uuid.UUID, to_status: TaskStatus, **kwargs: object) -> Task:
    async with AsyncSessionLocal() as session:
        state_manager = TaskStateManager(session)
        task = await state_manager.transition(task_id, to_status, **kwargs)
        await session.commit()
        return task


def make_dlq_handler():
    """
    Consumes dead_letter.queue (Phase 4's topology) and is the only
    thing that actually marks a task DEAD_LETTERED. Everything before
    this (Phase 8's retry logic) can only get a message *into* the
    DLQ by nacking with requeue=False -- nothing upstream of here
    updates Task.status when that happens, so until this consumer
    processes the message, a permanently-failed task sits at FAILED,
    not DEAD_LETTERED. That's a real, brief window of eventual
    consistency between "RabbitMQ has routed this to the DLQ" and
    "Postgres reflects that" -- see docs/dead-letter-queue.md.

    Why a separate consumer instead of marking DEAD_LETTERED directly
    at the moment of nack (in _handle_terminal_failure): the DLQ's
    dead-letter-exchange routing is the actual mechanism RabbitMQ
    uses to decide a message belongs in the DLQ. Marking the task
    dead-lettered based on "we nacked it" would be assuming that
    routing succeeds without checking -- consuming the real
    dead_letter.queue instead means the task's status reflects where
    the message actually ended up, not just what this worker intended.
    """

    async def handle_dlq_message(message: AbstractIncomingMessage) -> None:
        try:
            body = json.loads(message.body)
            task_id = uuid.UUID(body["task_id"])
        except Exception:
            # Can't identify which task this was -- nothing to mark,
            # nothing gained by leaving it in the DLQ forever either.
            await message.ack()
            return

        try:
            await _transition(
                task_id,
                TaskStatus.DEAD_LETTERED,
                event_metadata={"reason": "retries_exhausted_or_permanent_error"},
            )
        except (ValueError, InvalidStateTransitionError):
            # ValueError: no such task (shouldn't happen, but not a
            # reason to get stuck). InvalidStateTransitionError: the
            # task has already moved on from FAILED/TIMEOUT by the
            # time this DLQ message was processed -- e.g. an
            # administrator manually retried it (POST
            # /tasks/{id}/retry) and it's now QUEUED, RUNNING, or
            # even SUCCESS again. That's a genuine race between this
            # consumer and an admin action, not a bug: the DLQ
            # message is now stale and safe to discard, since the
            # task's real current state is correct without it.
            pass

        await message.ack()

    return handle_dlq_message
