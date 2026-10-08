# Metrics (Phase 18)

## What this phase adds, and what it deliberately doesn't

This phase adds Prometheus metrics and Grafana dashboards -- the
*first half* of this project's observability story (see
docs/architecture.md's Observability layer row). It does **not** add
structured logging or distributed tracing; those are explicitly
later phases (OpenTelemetry tracing is already a stated dependency
in pyproject.toml, unused until then). Metrics answer "is the system
healthy, in aggregate, right now and over time" -- logs and traces
answer "what exactly happened to *this one* task" -- and this
project builds them as genuinely separate concerns rather than
pretending metrics alone cover observability.

## Why Prometheus + Grafana, and why pull

This is the de facto standard pairing for exactly this project's
shape: several independent long-lived processes (API, worker,
scheduler, outbox relay -- see docs/architecture.md), none of them
short-lived batch jobs, which is the one case Prometheus itself
recommends push (its optional Pushgateway) over for. Pull also turns
"a service is down" into something Prometheus itself observes
directly (the scrape fails, the target goes `up == 0`) instead of
something that has to be inferred from the *absence* of pushed data,
which is a much weaker signal.

## Per-service metrics modules, not one shared file

Each service owns its own `metrics.py`
(`services/api/metrics.py`, `services/worker/metrics.py`,
`services/scheduler/metrics.py`, `services/outbox_relay/metrics.py`),
not one `observability/metrics.py` with every metric the whole
system has. `prometheus_client` registers a metric into one global
default registry the instant it's instantiated -- if every metric
lived in one shared module, importing it anywhere would make every
process's `/metrics` output list every other process's metrics too,
permanently stuck at zero wherever that process never actually
records them. `observability/metrics.py` holds only what's genuinely
shared: histogram bucket boundaries (`TASK_DURATION_BUCKETS`) and
`start_metrics_server()`, the one piece of wiring three of the four
processes need (see below).

## Why the API exposes /metrics differently from the other three

The API already is an HTTP server (FastAPI/Starlette), so
`services/api/main.py` just mounts `prometheus_client`'s
`make_asgi_app()` at `/metrics` on its existing app and port -- no
new port, no new wiring. The worker, scheduler, and outbox relay are
pure `asyncio` loops with no HTTP server of their own at all, so each
calls `observability.metrics.start_metrics_server(port)` at startup,
opening a small, separate `prometheus_client`-managed HTTP listener
purely to serve `/metrics` (`WORKER_METRICS_PORT` /
`SCHEDULER_METRICS_PORT` / `OUTBOX_RELAY_METRICS_PORT`, default
9101/9102/9103). That listener never touches RabbitMQ, Postgres, or
Redis -- its only job is existing for Prometheus to scrape.

## Gauges for state outside the API process

`rabbitmq_queue_depth`, `outbox_unpublished_rows`, and
`dead_lettered_tasks_current` (all defined in
`services/api/metrics.py`) describe state that lives in RabbitMQ and
Postgres, not state that happens *at* an API request -- nothing
about "how many messages are sitting in a queue" occurs at the
moment someone calls `GET /metrics`. A `prometheus_client` custom
`Collector` could compute these at scrape time instead, but
`collect()` must be synchronous, and every one of these numbers
needs an async RabbitMQ or Postgres round trip. Instead,
`poll_external_gauges()` runs as a background task (started from the
API's lifespan, same pattern as `coordination/heartbeat.py`'s
`run_heartbeat_loop`) that refreshes these gauges every
`METRICS_POLL_INTERVAL_SECONDS` (default 5s) regardless of whether
anyone is scraping. The accepted tradeoff: a reader of `/metrics`
sees a value up to one poll interval stale, not a live read --
acceptable because these are dashboard/alerting numbers (trending
over minutes), not anything this codebase makes a decision based on.
`count_by_status()` (`TaskRepository`) and `count_unpublished()`
(`OutboxRepository`) back these gauges with a plain indexed
`COUNT(*)` each -- cheap even as the tables grow, not
`len(await repo.list(...))`, which would materialize rows nothing
needs.

## `tasks_processed_total`'s `outcome` label vs. `TaskStatus`

Deliberately not the same vocabulary. A worker's single handling of
one message ends in one of a fixed set of *outcomes* --
`success` | `retried` | `dead_lettered` | `cancelled` |
`duplicate_discarded` | `no_handler` -- which only partially overlaps
`domain/states.py`'s `TaskStatus`. One `retried` outcome, for
example, spans the FAILED -> RETRYING -> QUEUED sequence
`services/worker/consumer.py`'s `_handle_terminal_failure` walks
through as a single worker-side event. Reusing `TaskStatus` values
directly as the label would conflate "what status the row is in"
(the system of record, `persistence/models.py`) with "what this
worker did with this message" (an operational signal only) -- two
related but distinct things this project keeps visibly separate.

## `task_duration_seconds` measures the handler, not the request

Timed from immediately before `handler(payload)` starts to whichever
exit is taken (success, timeout, cancellation, or exception) --
deliberately excluding time spent only queued before a worker picked
the message up, and excluding a retry's backoff sleep
(`domain/retry_policy.py`). Mixing those in would make the histogram
answer "how long did this task take end-to-end" (a different,
also-useful question this project doesn't currently answer) instead
of "how long did the handler itself actually run," which is what
`TASK_DURATION_BUCKETS` (`observability/metrics.py`) is tuned for.

## HTTP metrics use the raw request path -- a known cardinality gap

`services/api/main.py`'s metrics middleware labels
`http_requests_total`/`http_request_duration_seconds` with
`request.url.path` as-is (e.g. `/tasks/3fae2b91-...`), not the
matched route template (`/tasks/{task_id}`). A real production
deployment would want the latter -- every distinct task_id currently
creates a new label value, which is exactly the unbounded-cardinality
problem Prometheus's documentation warns against. Fixing it means
resolving the matched route *before* this plain ASGI middleware form
runs, which needs Starlette internals this implementation doesn't
reach for. Documented as a known gap, not silently left unaddressed
-- acceptable for this project's traffic volume, not something to
copy into a system handling real request volume with real path
cardinality.

## A known gap: scraping scaled replicas in plain Docker Compose

`docker-compose.yml`'s worker/scheduler/outbox_relay services are
designed to run as N replicas (`docker compose up --scale worker=5`
-- see `services/worker/main.py`'s module docstring, and
docs/scheduling.md / docs/outbox.md for why that's safe at the
data layer). Prometheus's `static_configs` in `prometheus/
prometheus.yml` name one DNS target per service (`worker:9101`,
etc.); plain Docker Compose resolves that name to a different
replica's IP on each DNS lookup (round-robin), but gives Prometheus
no way to enumerate "all current replicas" as distinct scrape
targets the way real service discovery (Docker Swarm's DNS-SRV
records, or Prometheus's `docker_sd_configs` against the Docker
API) would. This project doesn't set either up. The practical
effect: metrics are correct and complete for the default, unscaled
(replicas=1) configuration this project ships; scaling past one
replica of any of these three services means Prometheus keeps
scraping only whichever single replica its one static target happens
to resolve to at request time, silently under-counting the others.
A real, acknowledged limitation -- not a solved problem, and not
hidden by this writeup.

## Reference: metric names

| Metric | Type | Labels | Service |
|---|---|---|---|
| `http_requests_total` | Counter | `method`, `path`, `status` | api |
| `http_request_duration_seconds` | Histogram | `method`, `path` | api |
| `tasks_created_total` | Counter | `task_type`, `priority` | api |
| `tasks_rejected_total` | Counter | `reason` | api |
| `rabbitmq_queue_depth` | Gauge | `priority` | api |
| `outbox_unpublished_rows` | Gauge | -- | api |
| `dead_lettered_tasks_current` | Gauge | -- | api |
| `tasks_processed_total` | Counter | `task_type`, `outcome` | worker |
| `task_duration_seconds` | Histogram | `task_type` | worker |
| `worker_active_tasks` | Gauge | -- | worker |
| `scheduler_dispatched_total` | Counter | -- | scheduler |
| `scheduler_poll_duration_seconds` | Histogram | -- | scheduler |
| `outbox_relayed_total` | Counter | -- | outbox_relay |
| `outbox_relay_poll_duration_seconds` | Histogram | -- | outbox_relay |

`grafana/dashboards/task-queue-overview.json` (provisioned
automatically, see `grafana/provisioning/`) charts all of the above.
