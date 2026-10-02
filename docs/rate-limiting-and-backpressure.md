# Rate limiting and backpressure

Two different guards on `POST /tasks`, checked in order, protecting
against two different kinds of overload.

## Rate limiting

### What

`coordination/rate_limiter.py`'s `RateLimiter`: a Redis fixed-window
counter. At most `RATE_LIMIT_REQUESTS_PER_WINDOW` calls to
`POST /tasks` per `RATE_LIMIT_WINDOW_SECONDS`, system-wide. Over the
limit gets `429 Too Many Requests` with a `Retry-After` header.

### Why system-wide rather than per-client

A rate limiter that matters needs to know *who* it's limiting, so
one caller's burst doesn't starve everyone else. This project has no
per-caller identity -- `API_KEY` is one shared secret, not a
credential that distinguishes callers from each other. A "per-client"
limiter built on that would be fake precision: every caller looks
identical to the system, so "per-client" and "system-wide" collapse
into the same thing. This limiter is honestly scoped to protecting
the service as a whole, which is the real thing it can do today. A
genuine per-client limiter is a natural extension once an auth phase
gives each caller its own identity to key the Redis counter on.

### Why a fixed window, not a token bucket

See `coordination/rate_limiter.py`'s module docstring for the full
tradeoff: a fixed window is one `INCR` + one conditional `EXPIRE` per
check, trivial to reason about, and good enough to stop sustained
overload -- which is the actual goal. Its known weakness is a
window-boundary burst (up to ~2x the nominal rate right at a window
edge); a token bucket or sliding-window log avoids that at the cost
of more Redis state and (for a token bucket) a Lua script for atomic
refill-and-spend. This project takes the simpler, well-understood
option and names the tradeoff rather than silently picking the more
sophisticated algorithm by default.

## Backpressure

### What

`messaging/backpressure.py`'s `get_total_queue_depth()`: sums the
`message_count` across the three priority queues via a passive
RabbitMQ declare. `POST /tasks` rejects new submissions with
`503 Service Unavailable` (and a `Retry-After` hint) once that total
reaches `BACKPRESSURE_MAX_QUEUE_DEPTH`.

### Why this is a second mechanism, not a duplicate of prefetch

`docs/rabbitmq.md`'s Prefetch section already covers this project's
*first* backpressure mechanism: `prefetch_count` stops RabbitMQ from
dumping an entire backlog onto one worker, which protects the
*consuming* side. It does nothing about the *producing* side --
nothing stops the API from accepting work faster than every worker
combined can ever drain, which just means the queues grow without
bound while every producer call keeps succeeding, oblivious that it's
burying the system. Queue-depth admission control is the
complementary half: a backstop on the producing side, independent of
(and layered on top of) whatever's happening with prefetch on the
consuming side.

### Why reject outright instead of letting the backlog grow

Every task message is both durable and persistent (Phase 4), so
RabbitMQ writes each one to disk -- a large backlog isn't just "work
waiting its turn," it's real, growing disk usage with no natural
ceiling. A queue deep enough can turn "the system is busy" into "the
broker runs out of disk," which has no graceful degradation path, only
an outage. Rejecting new submissions loudly once a threshold is
crossed gives the producer an actual signal to react to (slow down,
buffer client-side, page someone) while the system is still healthy
enough to say so -- the alternative (silently accepting everything)
gives no such warning until something has already broken.

### Why total depth across all three queues, not per-queue

The three priority queues are drained by the same shared pool of
workers -- see `docs/architecture.md`'s "RabbitMQ dispatch vs.
application-level load balancing" section: there's no capacity
reserved per priority, RabbitMQ just hands messages to whichever
consumer has a free prefetch slot. Backpressure here is about overall
system capacity, which is a property of all three queues combined,
not any one of them individually.

## Why these checks run before the rate limiter was even questioned, not after task creation

Both checks happen at the very top of `create_task()`
(`services/api/routers/tasks.py`), before `TaskService.create_task()`
does anything -- including the idempotency-key lookup (Phase 13). A
request that's going to be rejected shouldn't pay for a database
round trip first; rejecting as early as possible is both cheaper and
more honest about why the request failed (overload, not anything
about the request body itself).

## Failure modes

- **Redis is unreachable when `RateLimiter.check()` runs**: not
  caught specially here -- the exception propagates and the request
  fails with a `500`, rather than silently allowing every request
  through (which would mean "the rate limiter is down" quietly
  becomes "there is no rate limiter"). Whether failing open or
  failing closed is correct here is a real design question with
  reasonable arguments either way; this project fails closed (safer
  default, matches the general pattern in `/ready` of surfacing a
  broken dependency rather than hiding it) and makes the choice
  visible rather than silent.
- **The passive queue declare itself fails** (channel/connection
  issue): same reasoning -- propagates as a `500` rather than
  silently skipping the backpressure check.
- **Threshold tuned too low for real traffic**: every burst gets
  rejected, which looks like an outage to callers even though the
  system is actually healthy -- `BACKPRESSURE_MAX_QUEUE_DEPTH`'s
  default (10,000) is a guess, not a measured value, the same
  caveat as `WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS`
  (`docs/graceful-shutdown.md`).
