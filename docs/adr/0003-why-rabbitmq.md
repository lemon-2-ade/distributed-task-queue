# ADR-003: RabbitMQ as the message broker

## Context
The system needs a broker that can hold work durably between
"the API accepted a task" and "a worker executes it," redeliver work
a crashed consumer didn't finish, and support the DLQ/priority-queue
patterns this project is built to demonstrate.

## Decision
Use RabbitMQ, accessed directly via `aio-pika` (see ADR-002 for why
not through a framework like Celery).

## Alternatives
- **Kafka**: built for high-throughput log/event streaming with
  consumer-group offset tracking, not per-message ack/nack or
  built-in dead-lettering. Modeling "redeliver this one message if
  its consumer crashes" is far more natural in RabbitMQ than in
  Kafka's offset model.
- **AWS SQS**: would offload a lot of this project's actual learning
  goal (queue topology, ack timing, DLQ wiring) to a managed service
  that hides the mechanics; also not runnable in the local
  Docker Compose stack this project targets.
- **Redis Streams / lists as a queue**: possible, but would blur the
  line between Redis's role here (coordination/ephemeral state, see
  ADR-004 once written) and a durable broker's role, and lacks
  RabbitMQ's native exchange/routing/DLX primitives.

## Tradeoffs
RabbitMQ requires running and understanding a broker with real
operational surface area (exchanges, bindings, prefetch tuning) --
more upfront complexity than a hosted queue, but that complexity is
exactly what Phase 4 through Phase 9 exist to teach.

## Consequences
The project's queue topology (direct exchange, priority-routed
queues, a dedicated dead-letter exchange) is expressed directly in
AMQP concepts rather than behind an abstraction, which is what makes
docs/rabbitmq.md possible to write honestly.
