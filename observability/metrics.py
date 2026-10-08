"""
Shared metrics infrastructure (Phase 18).

Why per-service metric modules instead of one shared file of every
metric the system has: this project runs as four separate OS
processes (API, worker, scheduler, outbox relay -- see each
service's main.py), and each process's own /metrics output should
describe *that* process. prometheus_client registers every metric
object into one global default registry the moment it's
instantiated, so if every metric for every service lived in one
shared module, importing it anywhere (even just by accident, e.g.
through a shared import chain) would make an unrelated process's
/metrics output list task-execution histograms it can never actually
record, permanently stuck at zero -- confusing for anyone reading
that endpoint, and nothing stops it from silently growing worse as
more metrics get added. Instead: each service owns a small
`metrics.py` inside its own `services/<name>/` package (see
services/api/metrics.py, services/worker/metrics.py,
services/scheduler/metrics.py, services/outbox_relay/metrics.py) and
this module holds only what's genuinely common across all of them --
shared histogram bucket boundaries, and the one bit of actual
wiring (start_metrics_server) that three of the four processes need
because, unlike the API, they have no HTTP server of their own
already.

## Why Prometheus specifically, and why pull rather than push

Prometheus's model -- each process exposes current metric state on
an HTTP endpoint, and Prometheus itself decides when to scrape it --
is the de facto standard this pairs with Grafana for, and it fits
this system's shape well: every service here is already a long-lived
process (not a short-lived job that would've vanished before a
puller got to it), so there's no need for the push-gateway pattern
Prometheus itself recommends only for batch jobs. Pull also means a
scrape failing (a service briefly down) is itself an observable
signal in Prometheus (the target goes "down"), which a push model
would just lose.

## Why the API's /metrics differs structurally from the other three

services/api/main.py mounts prometheus_client's ASGI app at /metrics
on the API's own existing HTTP server and port -- no extra wiring
needed, since FastAPI/Starlette already is one. The worker,
scheduler, and outbox relay have no HTTP server at all (they're pure
asyncio loops -- see each one's main.py), so each calls
start_metrics_server() below to open a small, separate
prometheus_client HTTP listener, on its own port
(WORKER_METRICS_PORT / SCHEDULER_METRICS_PORT /
OUTBOX_RELAY_METRICS_PORT), purely to serve /metrics. That listener
has nothing to do with this system's actual work (it never touches
RabbitMQ, Postgres, or Redis) -- it exists solely so Prometheus has
something to scrape.

## A known gap: scraping scaled replicas

docker-compose.yml's worker/scheduler/outbox_relay services are all
designed to run as N replicas (`docker compose up --scale worker=5`
-- see services/worker/main.py's module docstring). Plain Docker
Compose gives replicas of one service the same DNS name, round-robin
resolved to a different container IP on each lookup, but no way for
Prometheus's static_configs to enumerate "all current replicas of
worker" as distinct scrape targets -- that needs real service
discovery (Docker Swarm's DNS-SRV records, or Prometheus's
docker_sd_configs against the Docker API), neither of which this
project sets up. prometheus/prometheus.yml's static targets
(worker:9101, etc.) work correctly for the default, unscaled
(replicas=1) compose configuration this project ships and documents,
but scaling a service past 1 replica means Prometheus silently only
ever scrapes whichever single replica its one static target happens
to resolve to at request time -- a real, acknowledged limitation,
not a solved problem. See docs/metrics.md.
"""

from prometheus_client import start_http_server

# Shared bucket boundaries for every task-duration-shaped histogram
# in this system (currently: task execution time in the worker).
# Spans from sub-5ms (much faster than any handler this project
# ships) out to 2 minutes, biased toward finer resolution in the
# sub-second range where most of this project's demo handlers
# (echo, sleep) actually land -- a bucket scheme this coarse would
# make p50/p95 queries in Grafana meaningless for fast tasks, and
# one this fine out at the tail would just be wasted cardinality.
TASK_DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
)


def start_metrics_server(port: int) -> None:
    """
    Opens a background HTTP listener (prometheus_client manages its
    own thread for this) serving /metrics on `port`, for a process
    that has no other HTTP server to mount onto. Safe to call once,
    early in a process's startup, before the main event loop's work
    begins -- it does not block.
    """
    start_http_server(port)
