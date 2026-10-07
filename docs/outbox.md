# The transactional outbox

## What

A new table, `outbox_messages`, and a fourth standalone process,
`services/outbox_relay/`, that together close the "dual-write
problem" this project has documented and deliberately left open
since Phase 5: every place that used to write a Task status change to
Postgres and *then*, as a second, separate operation, publish a
message to RabbitMQ now writes the status change *and* an
`outbox_messages` row in the exact same Postgres transaction. The
relay process is the only thing left that drains that table into
RabbitMQ.

## The problem this replaces

Through Phase 16, four call sites did the same risky two-step thing:

1. `services/api/services/task_service.py`'s `create_task()` --
   publish a brand-new task.
2. `services/api/services/task_service.py`'s
   `retry_dead_lettered_task()` -- publish a manually-retried task.
3. `services/worker/consumer.py`'s `_handle_terminal_failure()` --
   republish a task for its next automatic retry attempt.
4. `services/scheduler/dispatcher.py`'s `claim_and_dispatch_due_tasks()`
   (Phase 16) -- publish a deferred task once it comes due.

Each one committed a Postgres transaction (recording QUEUED), then
called `TaskPublisher.publish_task()` as a separate step. A crash,
a connection drop, or a process kill landing in the gap between those
two operations left a task permanently `QUEUED` in Postgres with no
message ever having reached RabbitMQ -- durable-looking, but silently
dead. Nothing would ever pick it up. This isn't a hypothetical:
"commit to database, then call a second external system" is one of
the most common sources of silent data loss in distributed systems,
specifically because each half looks completely fine in isolation.

## The fix: make the two things one atomic thing

Postgres can only make one kind of operation truly atomic: a
transaction against itself. It has no way to also make a RabbitMQ
publish part of that same atomic unit directly. The outbox pattern's
insight is to stop trying to make *two systems* agree atomically, and
instead convert the problem into *one system, twice*: write the
status change, and write a durable record of "this needs to be
published" (`OutboxMessage`), together, in one Postgres transaction.
That part is now genuinely atomic -- it's a single system. The actual
cross-system step (talking to RabbitMQ) is moved entirely out of the
transaction and out of the original call site, into a separate
process whose only job is noticing unpublished outbox rows and
relaying them. If that relay process crashes before finishing, the
unpublished row is still sitting right there in Postgres, unaffected,
ready to be found again -- nothing was ever at risk of being silently
forgotten, because the fact "this needs publishing" was committed
durably before anything attempted a risky, external operation at all.

## Why a separate process rather than relaying inline

The same reasoning as `docs/scheduling.md`'s "why this needs its own
process": nothing actively triggers "the outbox has something to
relay" the way an HTTP request triggers API work. Something has to
actively poll for unpublished rows, independent of whatever process
happened to write them -- and running that as its own process, like
the scheduler, means it scales and fails independently of the
API/worker/scheduler, and a RabbitMQ outage doesn't degrade anything
about accepting new work into Postgres (tasks just keep queuing up in
the outbox, waiting for the relay to catch up once RabbitMQ is back).

## Why `claim_unpublished` uses the same FOR UPDATE SKIP LOCKED pattern as the scheduler

Multiple `outbox_relay` replicas need to be able to run concurrently
without double-relaying the same row, for exactly the reason multiple
scheduler replicas need to avoid double-claiming the same due task
(`docs/scheduling.md`). `OutboxRepository.claim_unpublished()` is the
identical pattern applied to a different table: lock whatever a
replica selects, skip whatever's already locked by another replica's
concurrent query. No coordination service required.

## Why the relay holds its transaction open across the RabbitMQ publish (unlike the scheduler)

`docs/scheduling.md` is explicit that the scheduler's dispatcher
commits its Postgres work *before* anything that reaches outside
Postgres -- it can do that because publishing is a separate step it
gets to defer to the outbox. The relay doesn't have that luxury: its
entire reason to exist *is* the step of reaching outside Postgres.
There's nowhere further to defer the RabbitMQ call to. So
`relay_once()` keeps its claimed rows' locks held for the duration of
the batch's publish calls, and marks the whole batch published in one
commit at the end. See `services/outbox_relay/relay.py`'s docstring
for the accepted cost of that choice.

## The new failure mode this trades for the old one

A crash in `relay_once()` partway through a batch -- say, after
message 3 of 5 has genuinely reached RabbitMQ, but before the
transaction commits -- rolls back the *entire* batch, including
message 3's `published_at` update. The next poll cycle finds all five
rows unpublished again and relays them again, so message 3 gets
published to RabbitMQ a second time.

This is a real, deliberate tradeoff, not an oversight: it converts
"a task can be silently lost forever" into "a task can occasionally
be published twice." The second failure mode is categorically less
bad, and this system was already built to handle it: RabbitMQ's own
at-least-once delivery semantics (`docs/rabbitmq.md`) mean a
duplicate delivery was already a normal, expected event before this
phase existed, and Phase 13's idempotency handling
(`docs/idempotency.md`, and `services/worker/consumer.py`'s
RUNNING-redelivery detection) already exists specifically to make
duplicates safe. The outbox doesn't introduce a new class of problem
this system has no answer for -- it just adds one more legitimate
source of the one kind of problem (duplicates) this system already
knows how to live with, in exchange for eliminating the kind of
problem (silent loss) it had no answer for at all.

## Failure modes

- **Relay process down**: outbox rows simply accumulate as
  unpublished until a relay instance comes back and polls them --
  nothing is lost, just delayed, the same durability guarantee every
  other part of this system gets from Postgres being the source of
  truth.
- **Crash mid-batch in the relay**: see above -- possible duplicate
  publish of whatever was mid-batch, never a lost one.
- **A writer crashes between its own commit (status change + outbox
  row) and returning to its caller**: not a problem at all -- the
  commit already happened. The outbox row is durable; the relay picks
  it up on its own schedule, independent of whether the writer's
  process is even still running.
- **`outbox_messages` grows without bound**: this table is append-only
  by design (`persistence/models.py`'s `OutboxMessage` docstring) --
  published rows are kept as an audit trail, not deleted by
  application code. A real deployment would need retention/archival
  tooling for this table (and for `task_events`, which has the same
  property) -- out of scope for this project, called out explicitly
  rather than left as a silent gap.
