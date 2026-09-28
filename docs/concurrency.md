# Concurrency, parallelism, and horizontal scaling

## Concurrency is not parallelism

**Concurrency** is structuring a program so multiple tasks can be
*in progress* at once, by interleaving them -- while one task is
waiting (on network I/O, disk, a timer), another gets to run.
**Parallelism** is multiple tasks actually *executing at the same
instant*, on different CPU cores.

`asyncio`, which this project's worker uses, gives concurrency, not
parallelism. A single Python process has one Global Interpreter Lock
(GIL): at any given nanosecond, only one thread in that process is
executing Python bytecode. `asyncio` doesn't get around the GIL --
what it does is let a task voluntarily yield control (at every
`await`) while it's waiting on I/O, so the event loop can run a
different task during that wait. This project's task handlers
(`echo`, `sleep`) are I/O-bound in the sense that matters here:
`asyncio.sleep()` yields control instead of blocking, so N sleeping
tasks cost roughly the same wall-clock time as 1, not N times as
much -- but that's concurrency (better use of idle wait time), not
parallel execution.

**This project does not claim asyncio creates CPU parallelism.** A
CPU-bound task handler (heavy computation, no `await` points) would
block the entire event loop for its duration, stalling every other
concurrently "running" task in that same worker process -- asyncio
concurrency only helps when tasks actually spend time waiting, not
computing.

## Where this project gets real parallelism

Not from asyncio within one process. From **multiple worker
processes**, each with its own Python interpreter and its own GIL:

```
docker compose up --scale worker=5
```

This starts 5 independent `worker` containers, each running its own
`services/worker/main.py`, each with its own RabbitMQ connection and
consumer tags on the same three queues. RabbitMQ round-robins
deliveries across whichever consumers currently have prefetch
capacity (see docs/rabbitmq.md) -- it has no idea these 5 consumers
happen to be identical Docker containers; from its side they're just
5 more AMQP consumers on the queue. Five OS processes really can run
Python bytecode at the same instant on a multi-core machine, which
one process with `asyncio.gather()` cannot.

## WORKER_CONCURRENCY: what it actually bounds

`WORKER_CONCURRENCY` (env var, default 10) controls two related
settings inside a *single* worker process:

1. `channel.set_qos(prefetch_count=WORKER_CONCURRENCY)` -- how many
   unacknowledged messages RabbitMQ will hand this one process at a
   time (an AMQP-level setting; see docs/rabbitmq.md).
2. An `asyncio.Semaphore(WORKER_CONCURRENCY)` inside the message
   handler -- an application-level cap on how many task handlers may
   actually be executing inside this process at once (see
   `services/worker/consumer.py`'s docstring for why both exist).

So within one process, `WORKER_CONCURRENCY` I/O-bound tasks can be
"in flight" concurrently, interleaved on that process's single event
loop. Across N worker replicas, total system throughput scales with
both `WORKER_CONCURRENCY` *and* `N` -- but only the `N` axis buys
actual CPU parallelism.

## CPU-bound work: a known limitation, not solved yet

Nothing in this project currently protects the event loop from a
CPU-bound task handler blocking it. A future task handler that does
real computation (as opposed to I/O-bound handlers like `sleep`)
would need to run in a separate process (e.g. via
`loop.run_in_executor` with a `ProcessPoolExecutor`) to avoid
stalling every other task the worker is concurrently holding. This
project's demonstration handlers are deliberately I/O-bound so this
gap doesn't distort the concurrency story; it's called out here
rather than glossed over.

## Threads

Not used in this project's worker. Python threads would sidestep the
"blocks the event loop" problem for I/O-bound work, but still can't
give CPU parallelism for pure-Python code under the GIL (only
C-extension code that releases the GIL, like some NumPy operations,
benefits). Given the worker is already `asyncio`-based and I/O-bound
handlers are the norm, threads would add complexity without solving
a problem this project actually has.
