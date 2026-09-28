# RabbitMQ

## Core vocabulary

- **Exchange**: where a publisher sends a message. An exchange
  doesn't store anything -- it routes. This project uses `direct`
  exchanges, which route a message to every queue bound with a
  routing key that exactly matches the message's routing key.
- **Queue**: where messages actually sit until a consumer takes
  them. A queue is bound to one or more exchanges via a **binding**
  (exchange + routing key -> queue).
- **Routing key**: a string attached to a published message.
  `task.exchange` uses the task's priority ("high"/"normal"/"low")
  as the routing key, so a direct exchange is sufficient -- there's
  no need for the pattern-matching a `topic` exchange offers, since
  every message maps to exactly one queue.
- **Durable** (exchange/queue): the *definition* survives a broker
  restart. A durable queue that RabbitMQ forgot about after a crash
  would mean every consumer's binding silently vanishes.
- **Persistent** (message `delivery_mode`): the *message itself* is
  written to disk, not just kept in memory. Durability and
  persistence are two different guarantees that both have to be true
  for "this message survives a broker restart" to hold -- a durable
  queue holding a non-persistent message still loses it on restart.

## Acknowledgements, redelivery, and why ACK timing matters

A consumer doesn't just "receive" a message -- it receives it, does
work, and then explicitly tells RabbitMQ `ack` (done, remove it) or
`nack`/`reject` (something went wrong). Until the broker gets an ack,
the message is considered **unacknowledged**, still technically
"delivered" but not yet consumed.

If a consumer disconnects (crashes, loses network, is killed) before
acking a message it was holding, RabbitMQ **redelivers** that message
to another consumer. This is the mechanical basis of at-least-once
delivery in this system: a worker can crash mid-execution, and the
task isn't lost -- it comes back. The cost is that the task may now
run twice, which is exactly why idempotency (Phase 13) exists as a
separate, necessary concern rather than a nice-to-have.

This is also why **ACK timing matters**, and why this project's
workers (Phase 5) only ack *after* the task's terminal state has been
durably written to PostgreSQL -- acking earlier (e.g. immediately on
receipt) would mean a worker crash between "ack sent" and "state
persisted" loses the task silently: RabbitMQ thinks it's done, but
nothing ever ran it to completion or recorded that.

## Prefetch

`channel.set_qos(prefetch_count=N)` (used by consumers, Phase 5)
limits how many unacknowledged messages RabbitMQ will hand a single
consumer at once. Without a prefetch limit, RabbitMQ will push every
ready message to the first connected consumer as fast as the network
allows, even if that consumer has no spare capacity to work on them
-- one overloaded worker ends up hoarding a queue's entire backlog
while idle workers on other connections starve. Prefetch is this
project's first and simplest backpressure mechanism (see
docs/architecture.md and the dedicated backpressure phase).

## Dead-letter exchange and redelivery loops

Every priority queue in this project is declared with
`x-dead-letter-exchange` pointing at `dead_letter.exchange` (see
`messaging/topology.py`). When a consumer nacks/rejects a message
with `requeue=False`, RabbitMQ doesn't discard it or hand it back to
the same queue -- it republishes it to the configured dead-letter
exchange instead. Without this, a message a worker can never
successfully process (a permanently broken payload, a bug in a task
handler) would either be lost the moment someone rejects it, or --
worse -- loop forever between "redelivered" and "rejected" if nacked
with `requeue=True`, burning CPU and cluttering logs without ever
resolving.

## Publisher confirms

This project's channel is opened with `publisher_confirms=True`
(`messaging/connection.py`), aio-pika's default. Without confirms,
`await exchange.publish(...)` returning successfully only means "the
bytes were written to the TCP socket" -- it says nothing about
whether RabbitMQ actually received and stored the message. With
confirms, the `await` doesn't complete until the broker has
acknowledged the message, so a caller that gets past `publish()`
without an exception has a real guarantee, not just an optimistic
one.

## Queue topology in this project

```
task.exchange (direct)
    |
    +--routing key "high"---> high_priority.queue
    |
    +--routing key "normal"-> normal_priority.queue
    |
    +--routing key "low"----> low_priority.queue

dead_letter.exchange (direct)
    |
    +--routing key "dead_letter"--> dead_letter.queue
```

## Priority, fairness, and starvation

Separate per-priority queues (rather than RabbitMQ's built-in
priority-queue feature) give this project full control over how
workers drain them -- but that control is also a responsibility: a
worker that always checks `high_priority.queue` first and only ever
looks at `normal_priority.queue`/`low_priority.queue` when
`high_priority.queue` is empty will **starve** low-priority tasks
indefinitely under sustained high-priority load. This project's
worker consumption strategy (Phase 6 onward) has to make a deliberate
choice about this tradeoff -- documented there once implemented --
rather than pretending "process high priority first" is a free
policy with no downside.

## What Phase 4 does *not* yet do

This phase only builds the exchange/queue/binding topology and the
publisher side. It does not yet include: a consumer (Phase 5), retry
logic (Phase 8), or dead-letter *policy* -- i.e. deciding when a
message should actually be rejected into the DLQ vs. retried (Phase
9). The DLX wiring exists now because it's part of the topology, but
nothing rejects a message into it yet.
