# ADR-002: Direct RabbitMQ/aio-pika instead of Celery, RQ, or Dramatiq

## Context
Celery (and similar frameworks) already solve everything this project
sets out to build: task routing, retries, result backends, worker pools.
Using one would produce a working system quickly, but would hide the
mechanics this project exists to learn — acknowledgement timing, prefetch,
redelivery, DLQ wiring, idempotency, and the dual-write problem between a
database and a broker.

## Decision
Talk to RabbitMQ directly via `aio-pika`. Build the task registry, retry
logic, DLQ handling, and worker consumption loop by hand.

## Alternatives
- **Celery**: production-proven, but its broker interaction, retry policy,
  and result backend are all internal implementation details — using it
  would mean configuring a black box rather than building the box.
- **RQ**: simpler, Redis-based, but the same objection applies, and this
  project specifically wants RabbitMQ's exchange/routing/DLQ model.
- **Dramatiq**: similar tradeoff to Celery.

## Tradeoffs
Building this by hand means more code, more edge cases to get right (e.g.
correct ack/nack timing, race conditions between scheduler replicas), and
no ready-made ecosystem (Flower-style monitoring, battle-tested retry
edge cases). In exchange, every distributed-systems mechanism in the
system is visible and explainable rather than delegated.

## Consequences
Higher implementation cost, but that cost *is* the point: each phase of
this project (retries, DLQ, idempotency, outbox, heartbeats) corresponds
to a piece of functionality Celery would otherwise have provided for
free and invisibly.
