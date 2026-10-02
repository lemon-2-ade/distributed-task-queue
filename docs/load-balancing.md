# Application-level load balancing

## What

Two worker-selection strategies, `coordination/load_balancer.py`:

- `RoundRobinStrategy` -- cycles through currently-alive workers in a
  fixed, deterministic order.
- `LeastLoadedStrategy` -- picks whichever alive worker currently has
  the fewest in-flight tasks.

Exposed read-only via `GET /workers/select?strategy=least_loaded` (or
`round_robin`), which reports which worker that strategy would pick
*right now*, given the current registry snapshot.

## Why this is separate from RabbitMQ's own dispatch

RabbitMQ already decides which connected consumer receives the next
message off a queue -- round-robin across consumers, gated by each
consumer's available `prefetch` slots. It has no notion of "worker
capacity" at all: a worker with a free prefetch slot gets the next
message even if it's already struggling through several slow tasks
in its other slots. That's a deliberate simplicity in RabbitMQ's
design, not a bug -- but it means "which worker *should* handle the
next thing" is a question RabbitMQ's dispatch literally cannot
answer, because it isn't tracking the information (current load)
that answer would require.

This project keeps that distinction explicit (see
`docs/architecture.md`'s "RabbitMQ dispatch vs. application-level
load balancing" section, written back in Phase 1 before either side
existed) rather than quietly conflating "which consumer gets this
AMQP delivery" with "which worker is the best choice for this work" --
they're different questions, answered by different components, and
only one of them (RabbitMQ's) is on the critical path for every
message today.

## Why this phase doesn't wire a strategy into real dispatch

There currently isn't a decision point in this system that needs to
*pick* a specific worker. The priority queues are consumed by every
worker uniformly; RabbitMQ decides delivery. A strategy becomes
load-bearing once something needs to route to one worker in
particular -- the clearest future case is the scheduler (Phase 16),
which could use `LeastLoadedStrategy` to decide *when* to release a
batch of scheduled tasks rather than firehosing them all into the
queues at once. Building the strategies now, backed by real registry
data (Phase 10's `active_task_count`, kept current by
`services/worker/consumer.py`), and exposing them as an inspectable,
independently testable admin endpoint, is the deliberate way to get
there without inventing fake routing logic today just to have
something to route.

## Why active_task_count lives in the registry, not computed on demand

`LeastLoadedStrategy` needs "how many tasks is this worker handling
right now" for every alive worker, cheaply, on every call. Computing
that by querying Postgres for `COUNT(*) WHERE status='RUNNING' AND
worker_id=...` per worker on every `GET /workers/select` call would
work, but adds a database round trip (or N of them) to a read that's
meant to be a fast, frequent, low-stakes lookup -- and worker liveness
data already lives in Redis for exactly this kind of
reporting-shaped, ephemeral, frequently-read state (see
`docs/worker-registry.md`). Each worker already reports its own load
as a side effect of handling messages (`increment_load`/
`decrement_load` around every handler invocation,
`services/worker/consumer.py`), so reading it back is a single Redis
hash read per worker, already-fetched as part of
`list_active_workers()`.

## Why round-robin needs a sorted worker list

Redis's `SCAN` (which `list_active_workers()` uses) makes no
ordering guarantee across calls -- the Nth key returned this call
might not be the Nth key returned next call. Without sorting by
`worker_id` first, an internal "pick index N mod len(workers)"
counter wouldn't actually rotate through the full set in a
predictable way; it would just be a counter bumping against an
effectively-random ordering. Sorting first makes the rotation
genuinely even and the output reproducible for a given registry
state and counter value, which also makes the strategy unit-testable
without a live Redis.

## Failure modes

- **No workers alive**: both strategies return `None`;
  `GET /workers/select` reports `503`, not an empty/null `200` --
  "nobody to pick" is a real failure state for a caller that needed
  an answer, not a normal empty-list result.
- **`active_task_count` momentarily stale or wrong**: bounded by the
  same staleness window as the rest of the registry (up to
  `WORKER_HEARTBEAT_TTL_SECONDS` if a worker died mid-task without
  decrementing) -- see `docs/worker-registry.md`'s failure-modes
  section. `decrement_load` additionally clamps at zero rather than
  going negative (see its docstring in
  `coordination/worker_registry.py`), so a lost decrement can only
  ever make a worker look *more* loaded than it really is, never
  less -- the safer direction for a strategy whose whole job is to
  avoid overloading an already-busy worker.
- **`RoundRobinStrategy`'s counter reset**: every API process restart
  resets the counter to 0, and multiple API replicas (not yet part
  of this project's deployment, but a reasonable future state) would
  each keep their own independent counter. That means "round-robin"
  is only a true rotation per-process, not globally across every
  caller of `GET /workers/select` -- acceptable for an admin/
  diagnostic endpoint, and worth revisiting if a strategy instance
  ever needs to coordinate across multiple API processes (at which
  point the counter itself would need to live in Redis, the same
  state-sharing tool already used everywhere else in this layer).
