"""
Redis Pub/Sub channel used to ask a *specific* worker to cancel a
*specific* RUNNING task it currently owns (Phase 12).

Why this needs its own mechanism, distinct from everything else in
this codebase: every other piece of coordination so far (the task
queues, the worker registry) is about getting work *to* a worker or
reporting state *from* one. Cancellation is different -- it's a
targeted interrupt sent *into* a worker process that's already
mid-execution, running a task handler this worker chose on its own
(via RabbitMQ's dispatch) and that nothing else in the system knows
which specific worker owns, except the `worker_id` recorded on the
Task row when it entered RUNNING.

A RabbitMQ message can't do this job: there's no way to "recall" a
message already delivered and being processed, and publishing a new
message doesn't reach a process that isn't consuming from a queue for
that purpose. Redis Pub/Sub fits instead -- every worker subscribes
to one shared channel, so publishing a cancellation request reaches
every worker process immediately, and each one locally checks "is
this task_id one *I* am running right now" (services/worker/
consumer.py's `running_tasks`) before doing anything. Workers that
don't own the task just see the message and ignore it -- harmless,
cheap, and simpler than maintaining a separate channel per worker.

This is fire-and-forget by design, not request/response: publishing
a cancellation request doesn't wait for, or even know whether, any
worker actually received and acted on it. If the owning worker isn't
currently running (already finished, or isn't even up anymore), the
request is simply never seen by anyone and nothing happens -- see
services/api/services/task_service.py's cancel_task() and
docs/timeouts-and-cancellation.md for how that's handled (the caller
already knows the task's last-known status from Postgres; this
channel is strictly best-effort on top of that, never the source of
truth for whether a task is cancelled).
"""

import json
import uuid

import redis.asyncio as redis

from config import get_settings

CANCELLATION_CHANNEL = "task_cancellations"


class CancellationBroadcaster:
    def __init__(self, redis_client: "redis.Redis | None" = None) -> None:
        settings = get_settings()
        self._redis = redis_client or redis.from_url(settings.redis_url, decode_responses=True)

    async def request_cancel(self, task_id: uuid.UUID, worker_id: str) -> None:
        await self._redis.publish(
            CANCELLATION_CHANNEL, json.dumps({"task_id": str(task_id), "worker_id": worker_id})
        )

    async def listen(self):
        """
        Async generator yielding (task_id, worker_id) for every
        cancellation request published on the channel, forever, until
        the underlying pubsub connection is closed. The caller (each
        worker's main loop) is responsible for filtering by
        worker_id -- this layer doesn't know which worker it's
        running inside.
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(CANCELLATION_CHANNEL)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    # pubsub.listen() also yields the subscribe
                    # confirmation itself as a "subscribe"-type
                    # message -- not a real cancellation request.
                    continue
                try:
                    data = json.loads(message["data"])
                    yield uuid.UUID(data["task_id"]), data["worker_id"]
                except Exception:
                    # A malformed payload on this channel is not
                    # something any worker can act on -- skip it
                    # rather than crashing the listener loop over it.
                    continue
        finally:
            await pubsub.unsubscribe(CANCELLATION_CHANNEL)
            await pubsub.aclose()

    async def close(self) -> None:
        await self._redis.aclose()
