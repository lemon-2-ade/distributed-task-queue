
# Chaos / failure-injection testing (Phase 21)

## Why this phase is different from every phase before it

Every phase up to this one was verified the same way: write a
`verify_phaseNN.py` script, run it against an embedded Postgres
(`pgserver`) and, where needed, `fakeredis`, with fake AMQP messages
standing in for RabbitMQ -- fast, no Docker required, good enough to
prove the *code* does what it's supposed to. None of that can answer
this phase's actual question: **does the running system survive a
real dependency actually disappearing out from under it?** Mocking a
Redis call raising an exception proves the code handles that
exception; it says nothing about whether the real
`docker compose stop redis` -> reconnect -> recover sequence behaves
the way the design docs claim it does. So this phase's verification
is a different kind of artifact entirely: `scripts/chaos_test.py`, a
small Typer CLI meant to be run against a real, live
`docker compose up` stack, not this project's usual sandboxed
development loop -- and, unlike every `verify_phaseNN.py` before it,
it's committed to the repo rather than thrown away, specifically so
it can be rerun by anyone, any time, against their own stack.

## The four scenarios

```
python scripts/chaos_test.py worker-crash
python scripts/chaos_test.py rabbitmq-outage
python scripts/chaos_test.py postgres-outage
python scripts/chaos_test.py redis-outage
python scripts/chaos_test.py run-all
```

Phase 23 added API key authentication to `/tasks` (see
`docs/security.md`); this script reads `API_KEY` from the environment
(the same variable the stack's own `.env` sets) and sends it as
`X-API-Key` on every request, so it keeps working unmodified against
a stack with auth enabled -- just run it with that variable in its
environment (or leave it unset to use `.env.example`'s own
"change-me" placeholder against a stack still using the default).

**`worker-crash`** -- submits a `sleep` task, waits for it to reach
`RUNNING`, then `SIGKILL`s the worker process mid-execution and
confirms the task still eventually completes. This is the most
direct possible exercise of docs/rabbitmq.md's "why ACK timing
matters": a worker that dies between receiving a message and acking
it leaves that message unacked, and RabbitMQ's at-least-once
delivery means another consumer (the same worker, restarted by
Docker Compose's `restart: unless-stopped`, or another replica)
eventually gets it and runs it from scratch. It's also a live,
visible argument for docs/idempotency.md's insistence that handlers
tolerate re-execution: the demo `sleep` handler is harmless to rerun,
but a handler with real side effects (charging a card, sending an
email) would rely on exactly the idempotency-key machinery this
project built in Phase 13 to avoid doing that twice.

**`rabbitmq-outage`** -- stops RabbitMQ, confirms `/ready` correctly
flags it, submits several tasks *while it's down* (these succeed --
task creation only ever touches Postgres's outbox table, never the
broker directly, since Phase 17), confirms the outbox's unpublished-
row backlog actually grows while the broker is unreachable, restarts
RabbitMQ, and confirms the backlog drains back to zero with every
task that was "created" during the outage eventually completing.
This is the Transactional Outbox pattern's entire reason for
existing (docs/outbox.md): a direct publish during this outage would
have failed outright (or worse, succeeded in Postgres and silently
never reached RabbitMQ -- the dual-write problem docs/outbox.md
documents at length) instead of durably recording the intent and
catching up once the broker is reachable again.

**`postgres-outage`** -- stops Postgres, confirms `/ready` reports
`"database unavailable"` *and nothing else* (proving
`services/api/routers/health.py`'s three dependency checks are
genuinely independent, not one check that fails closed on anything),
restarts Postgres, and confirms the system resumes serving traffic
and processing a fresh task with no manual intervention --
SQLAlchemy's connection pool reconnects on its own once the database
is reachable again.

**`redis-outage`** -- stops Redis, confirms `/ready` flags it in
isolation (same independence check), and then submits a task *while
Redis is still down*. This is the scenario that actually found a real
gap -- see below.

## The gap this phase found and fixed

Before this phase, `coordination/worker_registry.py`'s
`increment_load()` and `decrement_load()` had no failure handling at
all, unlike every other Redis-touching call in this file --
`heartbeat()` already explicitly swallows exceptions, with a
docstring explaining that a transient Redis blip shouldn't be allowed
to crash a worker over something as recoverable as a missed
heartbeat. `increment_load()` is called in
`services/worker/consumer.py`'s `_handle()` *before* the main
try/finally block even starts, and `decrement_load()` runs inside
that block's `finally`. An unguarded exception from either one, with
Redis down, meant: every single message this worker received failed
immediately, before the handler ever ran, for as long as Redis
stayed down -- not a graceful degradation of load-balancing accuracy
(which is all this bookkeeping is actually for), but a full outage of
task processing on a dependency that, by this project's own design
docs, was never supposed to be in the critical path for *running* a
task at all (see docs/worker-registry.md -- load tracking exists to
feed `LeastLoadedStrategy`, not to gate execution).

Running the `redis-outage` scenario against the stack surfaced this
immediately: the task submitted while Redis was down never left
`QUEUED`. The fix (now in `coordination/worker_registry.py`) wraps
both calls in the same best-effort `try/except Exception: pass`
pattern `heartbeat()` already used, with a comment explaining the
reasoning and pointing back to this chaos scenario. After the fix,
the same scenario passes: a Redis outage degrades load-balancing
accuracy (every worker's reported load goes stale/inaccurate until
Redis is back) without taking a single task down with it. This is
exactly chaos engineering's actual point -- not just confirming the
system survives failures it was already designed to survive, but
finding the ones it silently wasn't, while there's still only one of
them to fix.

## What this phase deliberately doesn't cover

No network-partition simulation (asymmetric packet loss, latency
injection via `tc`/`toxiproxy`) -- the four scenarios above are all
"a dependency disappears entirely and comes back," which Docker
Compose's own `stop`/`start`/`kill` already model faithfully and
without adding a new dependency just for this phase. A real
production chaos-engineering practice (Chaos Monkey-style random,
continuous injection; gradual degradation rather than hard outages)
is a much larger undertaking than one phase of a from-scratch
learning project, and this project documents that scope boundary
rather than pretending four scripted scenarios are a complete chaos
testing program.

No automated CI integration: these scenarios genuinely stop and
restart the actual `rabbitmq`/`postgres`/`redis`/`worker` containers
in whatever Compose stack is running when invoked, which is exactly
the kind of destructive, stateful operation that doesn't belong in
an automated pipeline without a dedicated, disposable environment --
out of scope for what this phase set out to build.
