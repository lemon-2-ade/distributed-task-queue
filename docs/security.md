# Security hardening

Phase 23. Three concrete gaps closed, plus an explicit account of
what's deliberately still out of scope -- the same "name the
tradeoff rather than silently leave it" stance every other phase in
this project takes (see e.g. `docs/rate-limiting-and-backpressure.md`'s
"why system-wide rather than per-client" section, which this phase's
own scoping decision directly follows).

## API key authentication

`config.py`'s `api_key` setting has existed since Phase 2, unused --
every route was reachable by anyone who could open a TCP connection
to the API. `services/api/auth.py`'s `require_api_key` dependency is
now attached, router-wide, to `/tasks/*` and `/workers/*`
(`services/api/main.py`'s `include_router(..., dependencies=[...])`
calls): every request to either router needs a valid `X-API-Key`
header, checked with `secrets.compare_digest` (constant-time, so a
response's timing can't leak how many leading characters of the key
were correct).

`/health`, `/ready`, and `/metrics` are deliberately left open. See
`services/api/auth.py`'s module docstring for the full reasoning;
short version: an orchestrator's liveness probe and Prometheus's
scraper have no credential to send, and gating either behind a key
that might be rotated or misconfigured would turn "the key changed"
into "the process gets killed" or "monitoring goes dark" -- a worse
failure mode than leaving two status endpoints and a metrics feed
unauthenticated.

This is one shared secret, not per-caller credentials, for the same
reason `coordination/rate_limiter.py` is a system-wide limiter rather
than a per-client one: this project has no notion of distinct callers
yet, so there's nothing for a "per-client" key to actually
distinguish. A real multi-tenant credential scheme (issued-per-caller
keys, scoped permissions) is a natural follow-on once something gives
each caller its own identity -- not something this phase pretends to
offer in the meantime.

A process-startup warning (`services/api/main.py`'s `create_app()`)
logs loudly, but does not refuse to start, if `API_KEY` is still the
`change-me` placeholder from `.env.example` in a non-`development`
environment. It's a warning rather than a hard failure specifically
because this is a portfolio/learning project meant to run locally
with one `docker compose up` -- failing to start over a config
default would get in the way of exactly the kind of casual
local run this project is built for. A real deployment would
want this to be a hard failure (or, better, no default at all);
this project names that gap instead of hiding it.

## Request body size limit

`services/api/middleware.py`'s `MaxBodySizeMiddleware` rejects a
request body over `MAX_REQUEST_BODY_BYTES` (default 1 MiB) with
`413 Payload Too Large`, checked via `Content-Length` when present
and by counting streamed bytes as they arrive otherwise -- see the
module docstring for why this has to be raw ASGI middleware rather
than Starlette's `BaseHTTPMiddleware` (which buffers the whole body
into memory first, defeating the point of a size guard before the
oversized body has already been fully read).

This is the same admission-control instinct
`docs/rate-limiting-and-backpressure.md` already documents for
*request volume* and *queue depth* -- reject loudly and early, before
paying the cost of what's about to be rejected anyway -- applied to a
third resource an unauthenticated-by-volume attacker could otherwise
exhaust: memory, via a single arbitrarily large request body. It's
registered as the outermost middleware in `services/api/main.py`
(after the metrics middleware, since Starlette wraps in
reverse-of-registration order -- see the comment at its registration
site), so an oversized body is rejected before it reaches API-key
auth, rate limiting, or anything else: there's no reason to spend a
Redis round trip or a key comparison on a request this guard is going
to reject regardless.

## Containers run as a non-root user

All four service Dockerfiles (`docker/api.Dockerfile`,
`docker/worker.Dockerfile`, `docker/scheduler.Dockerfile`,
`docker/outbox-relay.Dockerfile`) previously ran their process as
root -- the default for a plain `FROM python:3.12-slim` image with no
`USER` directive. Each now creates a dedicated system user (`dtq`,
no home directory, no login shell -- it only ever needs to own and
run `/app`) and switches to it with `USER dtq` before the final
`CMD`. This limits what a container escape or a code-execution bug in
a dependency could do on the host: a process running as `dtq` can't,
for instance, write to paths outside what it was `chown`'d, the way a
root process inside the container could if it ever broke out of (or
exploited a misconfiguration in) the container boundary.

## Secrets handling

Already in place since early phases, confirmed here rather than
changed: `.env` (the file that would hold real secrets) is in
`.gitignore` and in `.dockerignore`, so it's never committed and never
baked into an image layer. `.env.example` -- the file that *is*
committed -- uses the same `change-me` placeholder for every secret
(`POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`, `API_KEY`,
`GRAFANA_ADMIN_PASSWORD`) specifically so a real deployment has no
real secret to accidentally commit in the first place, and so a
grep across the repo's history for one of these names never turns up
anything live.

## What's explicitly out of scope, and why

- **TLS termination.** This project's services speak plain HTTP to
  each other and to callers. A real deployment puts a reverse proxy
  or ingress (nginx, a cloud load balancer, a service mesh sidecar)
  in front that terminates TLS -- that's infrastructure this
  project's `docker-compose.yml` doesn't model, and bolting a
  self-signed cert into the API process itself would just be a worse
  version of a solved problem, not a teaching opportunity specific to
  a task queue.
- **Per-client rate limiting and CORS.** Both need caller identity
  (a key per caller, an allowed-origins list specific to real client
  applications) this project doesn't have yet -- see the API key
  section above and `docs/rate-limiting-and-backpressure.md`'s
  existing "why system-wide" section, which makes the identical
  argument for the rate limiter. No `CORSMiddleware` is registered at
  all right now, which is the conservative default (browsers can't
  make cross-origin calls succeed without it) rather than a gap.
- **Dependency vulnerability scanning.** Catching a known-CVE
  dependency is a CI/supply-chain concern (`pip-audit`, `safety`,
  Dependabot, orthogonal to anything this project's own code does),
  not something `docker compose up` or a FastAPI middleware can
  address -- out of scope for the same reason this project doesn't
  build its own CI pipeline.
