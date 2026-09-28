# Worker registry and heartbeats

## What

A Redis-backed record of which worker processes currently exist and
are alive: `coordination/worker_registry.py`'s `WorkerRegistry`.
Every worker registers itself on startup, refreshes ("heartbeats")
that registration on a fixed interval for as long as it runs, and
the API exposes the current set via `GET /workers`.

## Why

The system needs to answer "which workers are alive right now" for
things later phases build on: load-aware scheduling, detecting a
worker that died mid-task (Phase 12+), capacity-aware autoscaling
decisions, an operator dashboard. None of that is possible without
first having a live, self-maintaining answer to that question. This
phase only builds the registry itself -- nothing yet *makes decisions*
based on who's alive; see `services/worker/main.py`'s module
docstring for why that split is deliberate.

## Why Redis, and why TTL-based liveness instead of explicit deregistration

A worker can stop existing in ways that never run its own shutdown
code: `kill -9`, an OOM-kill, a crashed container, a severed network
link. Any registry design that relies on the worker *telling* someone
it's gone will simply never hear about these -- there is no message
to receive. The only way to detect this class of failure is for
something else to notice the *absence* of activity.

Redis's key expiry (`EXPIRE`) gives this almost for free: each
worker's entry (`worker:{worker_id}`, a hash) carries a TTL that gets
pushed forward on every heartbeat. Stop heartbeating -- for any
reason, including a hard crash -- and the entry simply ceases to
exist once the TTL elapses, with no separate reaper process required.
Contrast with Postgres, which has no built-in expiring-row primitive;
the same behavior there would need a periodic job scanning for stale
`last_heartbeat_at` timestamps and deleting rows -- a whole extra
component, doing something Redis already does as a primitive
operation. That's the general shape of what Redis is used for in
this system: fast-changing, ephemeral coordination state, as opposed
to Postgres's role as the durable source of truth for task state.

## The interval/TTL gap

`WORKER_HEARTBEAT_INTERVAL_SECONDS=5`, `WORKER_HEARTBEAT_TTL_SECONDS=15`
(`.env.example`). The TTL is deliberately three times the interval,
not equal to it. If TTL equaled the interval exactly, a single
delayed heartbeat -- a GC pause, a brief Redis network blip, the
event loop being busy finishing a slow task -- would let the entry
expire even though the worker is perfectly healthy. The gap is
slack: a worker has to miss *several* consecutive heartbeats before
it's considered gone, which is a much stronger (and much less noisy)
signal of an actual failure than "the last one was a bit late."

## Why a separate heartbeat loop instead of heartbeating on message activity

`coordination/heartbeat.py` runs as its own `asyncio.Task`, on its
own timer, independent of the consumer callbacks in
`services/worker/consumer.py`. If heartbeating only happened as a
side effect of handling a message, an idle worker -- caught up, with
empty queues, doing nothing wrong at all -- would go quiet and
eventually expire out of the registry despite being healthy and
ready for the next task. Liveness has to be reported on its own
clock, decoupled from whether there happens to be work available.

## Why SCAN, not KEYS, for listing

`WorkerRegistry.list_active_workers()` (used by `GET /workers`) walks
the keyspace with `scan_iter`, which is cursor-based and yields
control back between batches, rather than `KEYS worker:*`, which
blocks Redis's single-threaded event loop until it has walked the
*entire* keyspace in one shot. At this project's scale (a handful of
worker keys) `KEYS` would never actually cause a problem -- the point
of using `SCAN` here is to build the pattern that stays correct as
the keyspace grows, not to solve a problem this project will ever
actually hit.

## Failure modes

- **Worker crashes hard**: entry silently expires after up to
  `WORKER_HEARTBEAT_TTL_SECONDS`; `GET /workers` stops listing it on
  its own, no cleanup code needed.
- **Redis itself is briefly unreachable**: `heartbeat()`'s caller
  (`coordination/heartbeat.py`) swallows the exception and retries
  on the next tick rather than crashing the worker process -- a
  worker's ability to process RabbitMQ messages doesn't depend on
  Redis being up, only its *visibility* in the registry does. If
  Redis stays down past the TTL window, the worker's entry expires
  even though the worker itself is still fine and still processing
  tasks; this is a known, accepted gap (the registry is a liveness
  hint, not the thing gating whether a worker actually does work).
- **Worker shuts down cleanly (SIGTERM)**: explicitly calls
  `deregister()` before exiting, so it disappears from `GET /workers`
  immediately rather than lingering (looking alive) for up to the
  full TTL window after it's already gone.
