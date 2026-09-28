"""
Queue names and the priority -> queue / routing-key mapping.

Why three separate queues instead of one queue with an in-message
priority field: RabbitMQ has no notion of per-message priority
ordering by default (priority queues exist as an opt-in feature but
come with their own throughput tradeoffs and don't solve starvation
on their own). Separate queues let workers choose to *drain
high_priority.queue before ever touching low_priority.queue* --
which is a starvation risk we accept deliberately and mitigate in
the worker's consumption strategy (Phase 6+), not something we paper
over with a "priority" attribute nobody enforces.

Every queue here also declares a dead-letter-exchange, so a message
that is rejected (nacked without requeue) or whose consumer never
acks it before a TTL doesn't just vanish or loop forever -- it's
redirected to the DLQ automatically by RabbitMQ, not by ad hoc
application code. See docs/rabbitmq.md.
"""

from domain.states import TaskPriority

HIGH_PRIORITY_QUEUE = "high_priority.queue"
NORMAL_PRIORITY_QUEUE = "normal_priority.queue"
LOW_PRIORITY_QUEUE = "low_priority.queue"
DEAD_LETTER_QUEUE = "dead_letter.queue"

DEAD_LETTER_ROUTING_KEY = "dead_letter"

# Routing key == priority queue name's role, kept short and stable
# since it's wire protocol, not a display string.
ROUTING_KEY_BY_PRIORITY = {
    TaskPriority.HIGH: "high",
    TaskPriority.NORMAL: "normal",
    TaskPriority.LOW: "low",
}

QUEUE_BY_PRIORITY = {
    TaskPriority.HIGH: HIGH_PRIORITY_QUEUE,
    TaskPriority.NORMAL: NORMAL_PRIORITY_QUEUE,
    TaskPriority.LOW: LOW_PRIORITY_QUEUE,
}
