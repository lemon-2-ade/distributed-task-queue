"""
Queue-depth-aware admission control for POST /tasks (Phase 14).

docs/rabbitmq.md's Prefetch section already describes this project's
*first* backpressure mechanism: `prefetch_count` stops RabbitMQ from
dumping its entire backlog onto one worker, which protects workers
from being overwhelmed on the consuming side. It says nothing about
the *producing* side -- nothing stops the API from accepting
submissions faster than every worker combined can ever drain them,
which just means the queues grow without bound while producers stay
perfectly happy, oblivious that they're burying the system. That's
the gap this module closes: a second backpressure mechanism, at
admission time, on top of (not instead of) prefetch.

## Why reject at admission instead of letting the queue grow

An unbounded backlog isn't free even before anything times out or
crashes: RabbitMQ persists every durable, persistent message to disk
(Phase 4's topology makes every task message both), so a queue depth
has a real resource cost, and a backlog large enough can turn "a
burst of traffic" into "the broker runs out of disk" -- a failure
mode with no graceful degradation path, only an outage. Rejecting new
work at the door, loudly (`503` with a clear reason and a
`Retry-After` hint), gives the producer the chance to actually react
-- slow down, queue client-side, alert someone -- while the system is
still healthy enough to say so. Silently accepting everything and
letting the queue balloon gives the producer no such signal until
something has already broken.

## Why this measures total queue depth, not per-queue

The three priority queues (`messaging/queues.py`) are drained by the
same shared pool of workers -- there's no dedicated worker capacity
reserved per priority (see docs/architecture.md's "RabbitMQ dispatch
vs. application-level load balancing" section: RabbitMQ decides which
*consumer* gets a message, not which *queue* gets drained first).
Backpressure here is about overall system capacity, not any one
queue's capacity specifically, so the three depths are summed into
one number and compared against one threshold
(`BACKPRESSURE_MAX_QUEUE_DEPTH`) rather than tracked separately.
"""

from aio_pika.abc import AbstractChannel

from messaging.queues import QUEUE_BY_PRIORITY


async def get_total_queue_depth(channel: AbstractChannel) -> int:
    """
    Sums ready-message counts across the three priority queues via a
    *passive* declare -- `passive=True` asks RabbitMQ "tell me about
    this queue" without creating or modifying it (it already exists;
    Phase 4's topology declared it for real). The response includes
    `message_count`, the number of messages currently sitting ready
    in that queue (not counting ones already delivered-but-unacked
    to a consumer) -- exactly "how much backlog is actually waiting"
    rather than "how much work exists in the system" more broadly.
    """
    total = 0
    for queue_name in QUEUE_BY_PRIORITY.values():
        queue = await channel.declare_queue(queue_name, passive=True)
        total += queue.declaration_result.message_count
    return total
