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
  invisible by default → Prometheus, Grafana, OpenTelemetry/Jaeger.
- Failures in one task_type have to be **contained** so they don't
  burn capacity meant for everyone else → a per-task_type circuit
  breaker in the worker.

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
| **Circuit Breaker** | Per-task_type, per-worker-process failure tracking (Phase 20): opens after consecutive handler failures and rejects further calls of that task_type immediately (no handler execution) until a cooldown elapses, then allows a single trial call before fully closing -- see `docs/circuit-breaker.md`. Deliberately local to each worker process, not shared via Redis. |
| **Task State Manager** | Enforces the task state machine (see `docs/task-lifecycle.md`, added when the state machine is implemented). |
| **Observability layer** | Metrics (Phase 18: each service exposes a Prometheus `/metrics` endpoint, scraped by the `prometheus` service and visualized in `grafana` -- see docs/metrics.md) and distributed tracing (Phase 19: OpenTelemetry spans exported via OTLP/gRPC to the `jaeger` service, propagated across RabbitMQ and the transactional outbox -- see docs/tracing.md). Structured logging is plain per-event log lines, already present since early phases. |
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

## Resilience verification

Every component above is designed to survive specific failures (a
worker dying mid-task, the broker or either datastore going
unreachable) -- but a design doc claiming that and a real system
proving it are different things. `scripts/chaos_test.py` (Phase 21)
runs real failure-injection scenarios against a live
`docker compose up` stack (kills a worker mid-execution, stops and
restarts RabbitMQ/Postgres/Redis) and checks the system actually
recovers the way the relevant doc above says it should. See
`docs/chaos-testing.md`, including the one real gap this phase found
and fixed.

Resilience under *failure* and resilience under *load* are different
questions, so Phase 22 adds a second, separate verification tool
rather than folding load generation into the chaos script:
`scripts/load_test.py` drives real concurrent HTTP traffic against a
live stack and reports actual latency percentiles, status-code
distribution, and end-to-end completion time, and specifically
validates that the rate limiter and backpressure admission control
(`docs/rate-limiting-and-backpressure.md`) behave as designed under
real concurrency, not just in a unit test. See `docs/load-testing.md`.

## Security hardening

Phase 23 closes three concrete gaps rather than adding a new
component: `/tasks` and `/workers` now require an `X-API-Key` header
(`services/api/auth.py` -- a setting that existed since Phase 2 but
was never enforced), `POST /tasks` rejects an oversized request body
before it's read into memory (`services/api/middleware.py`), and
every service container now runs as a dedicated non-root user
instead of root. See `docs/security.md`, including what's
deliberately still out of scope (TLS termination, per-client rate
limiting, CORS, dependency scanning) and why.

## Status

All 24 phases are complete. This document grew section by section as
each phase landed -- the dated structure above (Resilience
verification, Security hardening, ...) reflects the order things were
actually built in, not a reorganized final outline, so it doubles as
a rough timeline of the project alongside being a reference. See the
top-level [README.md](../README.md) for a summary of what's here and
how to run it, and every other file in this `docs/` directory for one
phase's full design rationale each.
