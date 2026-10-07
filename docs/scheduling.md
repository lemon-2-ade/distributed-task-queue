# Scheduler (deferred task execution)

## What

A third standalone process, `services/scheduler/main.py`, that polls
PostgreSQL on a fixed interval for `PENDING` tasks whose
`scheduled_at` has arrived, atomically claims a batch of them, marks
them `QUEUED`, and publishes each one to RabbitMQ -- exactly the
publish step `POST /tasks` itself would have done immediately, just
deferred until the requested time.

`POST /tasks` accepts an optional `scheduled_at`. When it's absent or
already in the past, nothing changes from every phase before this
one: the task is published immediately. When it's in the future, the
task is created and left `PENDING` -- not published, not even marked
`QUEUED` -- until the scheduler notices.

## Why this needs its own process, not a background task in the API

A deferred task isn't *triggered* by anything an API request does --
nobody calls an endpoint at the moment `scheduled_at` arrives. Some
component has to be the one actively noticing that time has passed,
independent of any request. Bolting that onto the API process (an
`asyncio` background task started in its lifespan) would tie "is
deferred scheduling currently working" to "is the API process up,"
and would make horizontally scaling the API (running N replicas
behind a load balancer) accidentally also scale however many polling
loops are racing each other for the same rows -- survivable, given
the locking scheme below, but conflating two independently-scaled
concerns (HTTP capacity vs. scheduling throughput) for no reason. A
separate process scales independently and fails independently: the
API can be fully down while already-scheduled tasks still fire on
time, and vice versa.

## Why this is a disambiguation worth stating explicitly:scheduling/ vs services/scheduler/

Two different things in this codebase have "schedul-" in the name,
and they solve unrelated problems:

- `scheduling/priority_scheduler.py` (Phase 15) -- `WeightedQueueSelector`,
  used *inside* the worker process's own message-dispatch loop to
  decide which of the three already-populated priority queues to
  pull from next, so high priority doesn't starve normal/low. Pure
  in-memory logic, no Postgres, no process of its own.
- `services/scheduler/` (this phase) -- a standalone process whose
  entire job is noticing when a *not-yet-queued* task's time has
  come and queuing it for the first time. Pure Postgres-polling
  logic, no RabbitMQ consumption at all (it's only ever a publisher).

Neither reuses the other's code or depends on it. The naming
collision is coincidental (both are, in the broad English sense,
"schedulers"), not a hint that one builds on the other.

## Why polling instead of something event-driven

There's no mechanism in this stack that pushes a notification at the
exact instant a timestamp is reached -- Postgres has no "wake me up
when `NOW() >= some_column`" primitive, and nothing is *writing* to
the tasks table at that moment to trigger a `LISTEN`/`NOTIFY`
either. Polling on a short, fixed interval
(`SCHEDULER_POLL_INTERVAL_SECONDS`, default 1s) is the simplest
correct option: it bounds how late a task can be ("up to one poll
interval late," not unbounded), costs one cheap, indexed query per
interval when there's nothing due, and needs no extra infrastructure
beyond what this project already has. A more elaborate approach
(e.g. a min-heap of pending fire times with a precisely-timed sleep
until the next one) would reduce that worst-case lateness but adds
real complexity for a guarantee this project doesn't actually need --
"fires within about a second of its scheduled time" is a reasonable
bar for a task queue, not a real-time system.

## Why FOR UPDATE SKIP LOCKED makes multiple scheduler replicas safe

`TaskRepository.claim_due_scheduled_tasks()` selects due, `PENDING`
rows with `FOR UPDATE SKIP LOCKED`. Two scheduler replicas running
this query at the same instant against an overlapping set of due
tasks are *guaranteed* to walk away with disjoint result sets:
`FOR UPDATE` locks whatever the first query's transaction selects,
and `SKIP LOCKED` tells the second query to silently exclude any row
it can't lock rather than block waiting for it (and definitely
rather than claim it anyway). Each replica only sees what nobody
else already has. There's no leader election, no distributed lock
service, no coordination between replicas required -- the database's
own row-level locking *is* the coordination mechanism, same as it
would be for any other "many workers competing to claim rows from
one table" problem. See
`persistence/repositories/task_repository.py`'s docstring for the
mechanics.

This is the same *shape* of problem RabbitMQ's own consumer dispatch
already solves for worker replicas (many consumers, one queue,
nobody double-delivers) -- just solved at the Postgres row level
instead of the message-broker level, because the thing being
competed for here is rows in a table, not messages on a queue.

## Why claiming writes an outbox row instead of publishing directly

`services/scheduler/dispatcher.py`'s `claim_and_dispatch_due_tasks`
claims, transitions to `QUEUED`, and -- as of Phase 17 -- writes an
`outbox_messages` row, all in the one transaction that commit
releases the `FOR UPDATE SKIP LOCKED` row locks for. It does **not**
publish to RabbitMQ itself; `services/outbox_relay/` does that
separately, on its own schedule. Earlier phases (through Phase 16)
had this function publish directly after committing -- a second,
separate operation vulnerable to the same dual-write problem
documented throughout this project (`docs/architecture.md`): a crash
between the commit and the publish would leave a task `QUEUED` in
Postgres that was never actually enqueued in RabbitMQ, with nothing
left to pick it up. Deferring the actual publish to the outbox is
what closes that gap -- see `docs/outbox.md` for the full design. It
also means this function never has to choose between holding its
Postgres transaction open across a RabbitMQ network call (blocking
every other scheduler replica on these same row locks for as long as
that call takes) and accepting the dual-write risk -- it does
neither, because publishing isn't this function's problem anymore.

## Failure modes

- **Scheduler process down**: scheduled tasks simply accumulate as
  `PENDING` with a past `scheduled_at` until a scheduler instance
  comes back and polls them -- nothing is lost, just delayed. This
  is the same durability guarantee every other `PENDING` task in
  this system already has (Postgres is the source of truth).
- **Crash after this function's commit, before the outbox relay
  publishes**: not a problem -- the outbox row is already durably
  committed at that point, and `services/outbox_relay/` will find and
  relay it on its own schedule, independent of whether the scheduler
  process that wrote it is still running. See `docs/outbox.md` for
  the (different, and accepted) failure mode this introduces instead.
- **Clock skew between the API host and the database**: `scheduled_at`
  comparisons use Postgres's own clock (the query's `<= now` bound is
  computed in the scheduler process and sent as a parameter -- see
  `claim_due_scheduled_tasks`), so skew between the API process (which
  only ever writes `scheduled_at`, never compares it) and the
  scheduler process (which does the comparing) could make a task fire
  slightly earlier or later than a wall-clock-perfect read of
  `scheduled_at` would suggest. In practice, container clocks on the
  same Docker Compose host stay tightly synced, so this is a
  theoretical caveat worth naming rather than a practical problem at
  this project's scale.
