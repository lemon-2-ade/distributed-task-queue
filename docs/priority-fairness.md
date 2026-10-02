# Priority and fairness

## The gap this phase closes

Three separate priority queues (`messaging/queues.py`) have existed
since Phase 4, each with its own routing key. Through Phase 14,
though, nothing about *how a worker consumed them* actually
enforced any ordering between priorities: each queue had its own
automatic, broker-pushed AMQP consumer, and RabbitMQ delivered from
whichever queue had a ready message and a free prefetch slot on this
worker -- no concept of "prefer high over normal" at all. Both
`services/worker/main.py`'s and `messaging/queues.py`'s docstrings
flagged this honestly from the moment the three queues were created:
having separate queues is necessary for priority to be possible, but
not sufficient for it to actually happen.

## Strict priority is the wrong fix

The obvious-sounding fix -- always drain `high_priority.queue`
completely before touching `normal_priority.queue` at all -- is
strict priority scheduling, and it has a real failure mode: under
sustained high-priority load, normal- and low-priority tasks never
get a turn, no matter how long they've been waiting. That's not
"prioritized," that's "starved." This project's actual requirement
is tasks that matter more get served *sooner*, not tasks that matter
less never get served while anything else exists -- different
guarantees, and only the first one was ever the goal.

## Smooth Weighted Round-Robin (SWRR)

`scheduling/priority_scheduler.py`'s `WeightedQueueSelector`
implements the same algorithm nginx uses for weighted upstream
selection: each priority has a weight
(`PRIORITY_WEIGHT_HIGH=4`, `_NORMAL=2`, `_LOW=1` by default), and
every pick adds each priority's weight to a running score, then takes
whichever has the highest score and subtracts the total weight from
it. Over any `sum(weights)`-length window this produces exactly
`weight` picks per priority, interleaved smoothly (`H, N, H, L, H, N,
H` for weights 4/2/1) rather than in weight-sized blocks (`H, H, H,
H, N, N, L`). The interleaved version matters for *latency*: a
normal-priority task waiting behind a block of 4 high-priority picks
in a row waits far longer on average than one waiting behind a
spread-out rotation, even though both produce the same long-run
4:2:1 throughput split.

Every priority is picked on a bounded, predictable schedule
regardless of how much high-priority traffic exists -- the property
strict priority scheduling cannot offer.

## Why this needed a pull model, not just a smarter push subscription

AMQP's push-based `queue.consume()` doesn't give an application any
say over *which* of several active consumers (across different
queues, in this case) receives the next delivery -- that choice
belongs entirely to the broker. There was no way to bolt SWRR onto
three independent automatic subscriptions and have it mean anything.
`services/worker/main.py`'s `_run_priority_dispatch_loop` replaces
that with an explicit pull loop: on every free concurrency slot, ask
the selector which queue to check, `queue.get(fail=False)` it
(returns `None` instead of raising if empty), and only dispatch a
handler once a message is actually in hand. This is the only way to
make the scheduling decision at the point this project actually
controls it.

### Falling through when the preferred queue is empty

If SWRR's pick for this turn is empty (e.g. it chose `high` but no
high-priority work exists right now), the loop doesn't wait -- it
falls through to the other queues, in weight order
(`WeightedQueueSelector.fallback_order`), so an empty high-priority
queue never stalls throughput on normal/low when there's real work
waiting there. Falling through by weight (not insertion order) keeps
the same "more important first" intent on the fallback path, not
just the common path.

### Replacing prefetch's backpressure role

The old push-based consumers got their concurrency ceiling from AMQP
`prefetch_count` -- RabbitMQ itself wouldn't deliver more than that
many unacked messages to this worker at once. A pull loop has no
such built-in limit: `queue.get()` would happily fetch as fast as
called, with nothing stopping it from pulling far more messages than
the worker can actually process concurrently and holding them
unacked in memory. `dispatch_semaphore` (acquired *before* every
pull, released once that message's `handle()` call finishes)
reproduces the same ceiling explicitly, at the application level
instead of the broker level. `MessageHandler` still has its own
internal semaphore too (`services/worker/consumer.py`, unchanged
since Phase 6) -- the two are deliberately redundant at the same
concurrency number rather than merged: one bounds *pull rate*, the
other bounds *execution rate*, and keeping them as two separate,
independently-correct mechanisms means neither has to know the other
exists.

### Idle backoff

When every queue comes up empty on the same turn, the loop backs off
for `PRIORITY_POLL_IDLE_SLEEP_SECONDS` before trying again, rather
than looping back into another all-empty round immediately. Without
this, an idle worker would busy-poll three empty queues as fast as
the event loop allows, burning CPU and spamming RabbitMQ for no
benefit -- there's nothing to gain by checking again a microsecond
later when nothing has changed.

## Why the DLQ consumer is untouched

`dead_letter.queue` keeps its original push-based
`queue.consume()` subscription from Phase 9. It isn't part of the
three-way priority split this phase is about -- DLQ traffic is
exceptional, not a fourth priority tier competing for the same
weighted schedule -- so giving it its own simple, independent
consumer keeps it unaffected by any of the above.

## Failure modes

- **Weights tuned so one priority effectively starves another in
  practice**: SWRR guarantees a *proportional*, not *absolute*,
  share -- setting `PRIORITY_WEIGHT_LOW=1` against overwhelming,
  sustained `PRIORITY_WEIGHT_HIGH` traffic still means low-priority
  tasks wait a long time between turns, even though they're never
  mathematically starved outright. Fairness here is about the
  scheduling algorithm's guarantee, not a promise about real-world
  wait times under any possible load pattern -- that still depends
  on tuning the weights (and ultimately, worker capacity) to the
  actual traffic mix.
- **Shutdown while every concurrency slot is busy**: the dispatch
  loop is sitting inside `dispatch_semaphore.acquire()`, waiting for
  a slot. `main.py` cancels the loop task outright on shutdown
  rather than awaiting it cooperatively, specifically so this case
  doesn't make shutdown's timing depend on how long whatever's
  currently running happens to take -- see the inline comment at the
  cancellation site for the reasoning in full.
