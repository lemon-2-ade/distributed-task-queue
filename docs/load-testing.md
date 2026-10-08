# Load testing

Phase 22. `scripts/load_test.py` generates real concurrent HTTP
traffic against a live `docker compose up` stack and reports what
actually happened -- latency percentiles, status-code distribution,
end-to-end completion time -- rather than reasoning about what the
system *should* do under load.

## Why this is a hand-rolled script, not Locust/k6

Same reasoning as `scripts/chaos_test.py` (Phase 21) for building
rather than importing: the mechanics of a load generator -- pace
calls to a target rate, bound how many are in flight at once, collect
latency samples, compute percentiles -- are simple enough to write
directly with `asyncio` and `httpx` (already a dev dependency), and
doing so keeps what's being measured and how in plain sight in this
project's own code instead of behind a framework's configuration DSL.
It's the same stance this project takes on not reaching for
Celery/RQ for the queue itself: understanding the tool by building it
is the point of the exercise.

## Why this needs a live stack, not pgserver/fakeredis

Every other phase's `verify_phaseNN.py` script could substitute an
embedded Postgres and `fakeredis` for the real thing, because it was
checking that *code* behaved correctly. This phase is checking that
the *running system* -- real HTTP over a real network stack, a real
RabbitMQ with real consumers competing for prefetch slots, a real
Postgres under real concurrent connection load -- holds up under
concurrency. There's no meaningful way to fake that; the numbers this
script reports are only honest when gathered against the real thing.

## Commands

### `run` -- sustained-rate load

```
python scripts/load_test.py run --rate 20 --duration 30 --concurrency 50
```

Paces `POST /tasks` calls at (approximately) `--rate` per second for
`--duration` seconds, capped at `--concurrency` requests in flight at
once -- the same bounded-concurrency pattern
`services/worker/consumer.py` uses for handler concurrency. A target
rate the server can't actually sustain shows up honestly as growing
latency, 429s, or 503s, rather than being silently absorbed by an
unbounded client-side queue.

It reports, in order:

1. **Achieved rate vs. requested rate** -- how close the generator
   actually got to the target, and the status-code breakdown
   (`201` created, `429` rate limited, `503` backpressure, `0`
   connection error -- the script's own sentinel for "no response at
   all," not a real HTTP status).
2. **POST /tasks request latency**, for accepted requests only -- how
   long the API itself took to accept and durably enqueue the task
   (the outbox write, Phase 6), not how long the task took to run.
3. **End-to-end completion latency**, for a sample of
   `--completion-sample-size` of the accepted tasks, polled until
   each reaches a terminal status. This is computed from the
   server's own `completed_at - created_at` timestamps, not
   client-observed wall time, so it's queue-wait-plus-execution time
   as the system itself recorded it -- uncontaminated by this
   script's own polling interval.

A full backlog run isn't tracked to completion (that would just make
this script a second load generator); a bounded sample is.

### `rate-limit-probe` -- validates Phase 14's rate limiter

```
python scripts/load_test.py rate-limit-probe --burst 200
```

The opposite of `run`'s steady-rate shape: fires `--burst` requests
with no pacing at all, specifically to exceed
`RATE_LIMIT_REQUESTS_PER_WINDOW` (see
`docs/rate-limiting-and-backpressure.md`) within its window, and
confirms the limiter actually rejects the excess with `429` and a
`Retry-After` header -- not just that the code reviews as if it
would. It then waits past the window and confirms a follow-up request
succeeds again, i.e. the limiter actually recovers rather than
latching open.

## What this phase doesn't cover

Backpressure (the `503` + `Retry-After` path in
`docs/rate-limiting-and-backpressure.md`) triggers on total queue
depth across all three priority queues, which only shows up under
sustained load that outpaces every worker combined -- a realistic
trigger depends on how many workers are running and
`BACKPRESSURE_MAX_QUEUE_DEPTH`'s configured value, not a fixed
request count like the rate limiter. `run` with a high `--rate` and
`--duration`, against a stack with workers scaled down, is the way to
exercise it; this script doesn't automate that scenario the way
`rate-limit-probe` automates the rate-limit one, since there's no
single burst size that reliably triggers it independent of the
stack's own configuration.
