# Architecture

## Why this system looks the way it does

A task queue has exactly one job: accept work from a producer, and
guarantee that work eventually runs somewhere, even in the presence of
crashes, restarts, and network failures. Everything in this architecture
follows from that one sentence:

- Work has to be **durable** once accepted → PostgreSQL.
- Work has to be **distributed to workers** without the API knowing which
  worker will run it → RabbitMQ.
- Workers have to **coordinate** (who's alive, who's overloaded, who's
  allowed to submit right now) without a shared process → Redis.
- All of it has to be **observable**, because distributed failures are
  invisible by default → Prometheus, Grafana, OpenTelemetry.

## Components

| Component | Responsibility |
|---|---|
| **FastAPI API service** | HTTP boundary. Validates requests, writes task rows, enqueues onto RabbitMQ (via the Outbox in later phases), exposes read endpoints. Holds no business logic itself — delegates to the service layer. |
| **Task Manager** (service layer) | The use-case layer between HTTP routes and persistence/messaging. Creation, cancellation, retry, state queries. |
| **RabbitMQ messaging layer** | The exchange/queue/routing-key topology, publisher, and consumer wrappers. Owns delivery guarantees (ack/nack, redelivery, DLQ). |
| **Worker service** | An independently deployable process that consumes messages, executes task handlers, reports state back to PostgreSQL, and reports liveness to Redis. |
| **Worker Registry** | Redis-backed record of which workers exist, their capacity/load, and whether their heartbeat is current. |
| **Scheduler** | Polls PostgreSQL for tasks whose `scheduled_at` has arrived, atomically claims them, and enqueues them to the Outbox — safe to run as multiple replicas. |
| **Outbox Relay** | The only process that publishes to RabbitMQ. Polls PostgreSQL's `outbox_messages` table for unpublished rows (written atomically alongside every status change that needs a message sent) and relays them — safe to run as multiple replicas. See `docs/outbox.md`. |
| **PostgreSQL persistence layer** | System of record: tasks, attempts, events, idempotency keys, outbox. |
| **Redis coordination layer** | Ephemeral, fast-changing state: heartbeats, rate-limit counters, short-lived locks/leases. Never the system of record. |
| **Load Balancing / Scheduling Strategy** | Application-level worker selection (round-robin, least-loaded) — distinct from RabbitMQ's own consumer dispatch, see below. |
| **Retry Manager** | Computes backoff/jitter and decides retry vs. dead-letter. |
| **Task State Manager** | Enforces the task state machine (see `docs/task-lifecycle.md`, added when the state machine is implemented). |
| **Observability layer** | Metrics, structured logs, and traces, shared by every service. |
| **CLI / admin dashboard** | Optional, later-phase conveniences layered on top of the API. |

## RabbitMQ dispatch vs. application-level load balancing

RabbitMQ's default consumer dispatch (round-robin over connected
consumers on a queue, modulated by `prefetch`) decides **which TCP
connection** receives the next message. It has no concept of "worker
capacity" or "current load" — it will happily hand a busy worker another
message if that worker's prefetch slot is free. The Load Balancing
Strategy component in this project is a separate, application-level
concern: it's for cases like the scheduler choosing where to route work,
or an admin view of which worker *should* pick up the next high-priority
item. Conflating the two is a common misunderstanding this project
deliberately keeps distinct.

## Status

This document will grow section by section as each phase is implemented.
Phase 1 only establishes the repository shape and toolchain described
here — no messaging, persistence, or worker code exists yet.
