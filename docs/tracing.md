
# Distributed tracing (Phase 19)

## What this phase adds, and how it completes the observability story

Phase 18 (docs/metrics.md) answered "is the system healthy, in
aggregate, right now." This phase answers the other question metrics
structurally can't: "what exactly happened to *this one task*, across
every process it touched, in what order, and how long did each step
take." A Prometheus histogram can tell you p99 task duration crept up
this hour; it cannot tell you that task `a1b2c3...` spent 4 of its
4.2 seconds sitting unpublished in the outbox table because the relay
was briefly behind. A trace can, because a trace is the *one task's*
story, not an aggregate over all of them.

With this phase, the three pillars of observability this project set
out to build (docs/architecture.md) are all in place: **metrics**
(Phase 18, Prometheus/Grafana) for aggregate health, **tracing**
(this phase, OpenTelemetry/Jaeger) for individual task journeys, and
plain structured logs (already present since early phases, one line
per significant event) for everything in between. None of the three
subsumes the others -- that's deliberate, not a gap.

## The actual problem: trace context doesn't cross a Postgres row

OpenTelemetry's default propagation mechanism is Python's
`contextvars`: a span started inside one async call stack is
automatically the parent of any span started further down that same
live stack, with zero code needed to wire it up. `FastAPIInstrumentor`
leans entirely on this -- every span created while handling one HTTP
request nests correctly under that request's span, automatically.

That mechanism requires the parent and child span to exist on the
same call stack, in the same process, overlapping in time. Nothing in
this system's design guarantees that across its real boundaries:

- The API (or scheduler) that decides a task needs to run finishes its
  HTTP request (or poll cycle) long before a worker ever picks the
  task up. There's no live call stack connecting them.
- Worse: the thing standing between them isn't even a message queue
  with its own context-propagation story -- it's a row in Postgres's
  `outbox_messages` table (persistence/models.py's `OutboxMessage`,
  docs/outbox.md). That row can sit there, completely inert, for an
  arbitrary stretch of time with no Python process attached to it at
  all. `contextvars` has nothing to attach to.

So trace context has to be carried as **data**, explicitly, at every
boundary where it would otherwise be lost:

```
[API / scheduler]  --inject-->  OutboxMessage.trace_context (JSONB)
        |                                |
        | (an arbitrary delay --         | outbox relay reads it back
        |  no live process)              v
        |                        [outbox relay] --inject--> AMQP message headers
        |                                                         |
        |                                                         | (RabbitMQ)
        v                                                         v
  (original HTTP request/poll span, now long finished)      [worker] --extract-- continues the trace
```

`observability/tracing.py`'s `inject_trace_context()` /
`extract_trace_context()` are the only two functions that do this
serialization, both thin wrappers over OpenTelemetry's own
W3C-standard `traceparent`/`tracestate` propagator, using a plain
`dict[str, str]` as the carrier -- JSON-serializable, so it fits
directly into a JSONB column or an AMQP message's headers table with
no extra encoding step of this project's own invention.

## Where trace context actually gets carried, boundary by boundary

1. **API request -> outbox row.** `services/api/main.py` instruments
   the whole FastAPI app with `FastAPIInstrumentor`, so every request
   already has an active span by the time
   `services/api/services/task_service.py`'s `create_task()` or
   `retry_dead_lettered_task()` runs. Neither needs to open a span of
   its own -- they just call `inject_trace_context()` and store the
   result on the `OutboxMessage` they write in the same transaction as
   the `QUEUED` status change (see docs/outbox.md for why that's one
   transaction in the first place).

2. **Scheduler dispatch -> outbox row.** A scheduled task has no
   inbound HTTP request to inherit a span from -- `services/scheduler/
   dispatcher.py`'s poll loop is its own process, woken by a timer, not
   a request. So this is this system's *other* trace root: each
   claimed due task gets a fresh span
   (`scheduler.dispatch_task.<task_type>`), and that span's context is
   what gets captured onto its outbox row.

3. **Outbox row -> AMQP message headers.** `services/outbox_relay/
   relay.py` is the one place that bridges stored context back to a
   live one. For each unpublished row, it extracts the stored
   `trace_context` and starts a *child* span of it
   (`outbox_relay.publish.<task_type>`, via `observability.tracing
   .start_span_from_carrier`) -- not just a bare pass-through -- so the
   relay step itself shows up as a real hop in the trace, with its own
   duration, rather than silently vanishing. That child span's own
   context (not the original row's) is what gets injected into the
   outgoing AMQP message's headers via `TaskPublisher.publish_task(...,
   headers=...)`.

4. **AMQP message headers -> worker execution.** `services/worker/
   consumer.py`'s `MessageHandler._handle()` extracts `message.headers`
   into a span (`worker.execute_task.<task_type>`) that wraps the
   entire attempt -- the `RUNNING` transition, the handler call, and
   (on failure) the retry-or-dead-letter decision.

5. **A retry's outbox row continues the *same* trace.** This is the
   one deliberate departure from "boundary in, boundary out, repeat."
   When an attempt fails transiently, `_handle_terminal_failure` calls
   `_transition_to_queued_and_enqueue_outbox` to write the retry's
   outbox row -- and that call happens *while the current attempt's
   span from step 4 is still active* on the call stack (nothing about
   the retry path leaves that `with` block before the enqueue
   happens). So `inject_trace_context()` there captures a *child* of
   the attempt that just failed, not a fresh trace. The result: a task
   retried three times before succeeding shows up in Jaeger as **one**
   trace, with three nested `worker.execute_task` spans telling that
   whole story end to end, instead of three unrelated traces with no
   link between them. See "Why retries don't start a new trace" below
   for the reasoning.

6. **Dead-lettering continues the same trace too.** RabbitMQ's
   dead-letter-exchange routing only *adds* an `x-death` header array
   when it redirects a nacked message to the DLQ -- it doesn't strip
   whatever headers were already there. So `make_dlq_handler()`'s
   consumer extracts the same trace context the last real attempt saw,
   and "this task was dead-lettered" lands as the final child span on
   that one trace, not an orphaned event with no story behind it.

A row or message with no stored/attached trace context (anything
written before this phase shipped, or written while
`OTEL_TRACES_ENABLED=false`) is handled the same way at every step:
`extract_trace_context(None)` returns a context with no parent, so
the next span just becomes its own trace root instead of raising.
Nothing in this system ever *requires* an incoming trace context to
function.

## Why retries continue the same trace instead of starting a new one

A task that fails twice and succeeds on the third attempt is, to
whoever's debugging it, one story: "this task took three tries." A
design where each attempt got its own fresh `trace_id` would scatter
that story across three disconnected Jaeger traces with nothing
linking them except a shared `task_id` buried in span attributes --
recoverable with enough digging, but not what a trace is supposed to
make effortless. Nesting every attempt as a child span under one
root trace means opening *one* trace in Jaeger shows the whole
lifecycle: attempt 1 (failed, 200ms), backoff sleep, attempt 2
(failed, 180ms), backoff sleep, attempt 3 (succeeded, 220ms) -- in
order, with real durations, no cross-referencing required.

## Why Jaeger's built-in OTLP receiver, and no separate OpenTelemetry Collector

Jaeger has accepted OTLP natively (gRPC and HTTP) since v1.35, so
pointing `OTEL_EXPORTER_OTLP_ENDPOINT` straight at Jaeger's collector
port (`4317`, gRPC) needs nothing else running. A standalone
OpenTelemetry Collector is a real, common piece of this stack in
production -- it earns its place when you need to fan traces out to
more than one backend, batch/sample centrally across many short-lived
processes, or scrub sensitive attributes before they leave the
cluster -- but none of that applies to one small demo system with one
trace backend. Adding a Collector here would be one more moving part
to explain with no corresponding lesson to teach. This project
documents the simplification instead of hiding it.

`jaegertracing/all-in-one` is used for the same reason: it's Jaeger's
collector, query service, and UI in a single container backed by
in-memory storage. A real deployment would back it with
Elasticsearch, Cassandra, or similar for durability across restarts;
this project's traces only need to survive one `docker compose`
session.

## Why asyncpg and aio-pika aren't auto-instrumented

`opentelemetry-instrumentation-asyncpg` and
`opentelemetry-instrumentation-aio-pika` both exist and would save
writing the spans in `services/outbox_relay/relay.py` and
`services/worker/consumer.py` by hand. This project doesn't use them,
for the same reason it builds the task queue itself from scratch
instead of reaching for Celery: the point is to show *how* the
pieces work, and a span per SQL query or per AMQP channel operation
is noise at the granularity anyone would actually read in Jaeger --
it would bury the handful of spans that matter (one per task attempt,
one per outbox relay publish) under hundreds of auto-generated ones
with no teaching value. Every span in this system is placed by hand,
only at a boundary that corresponds to a meaningful step in a task's
life. `FastAPIInstrumentor` is the one auto-instrumentation this
project does use, and only because an HTTP request *is* exactly the
meaningful boundary a hand-written span there would have measured
anyway.

## Known limitation: the API's per-route cardinality gap also affects traces

docs/metrics.md already documents that `services/api/main.py`'s
metrics middleware labels HTTP metrics with the raw request path
(`request.url.path`), not the matched route template, as an accepted
cardinality tradeoff. `FastAPIInstrumentor`'s auto-generated spans
have the same characteristic -- a span for `GET /tasks/<uuid>` is
named with that UUID in it rather than the `/tasks/{task_id}`
template. Span names aren't aggregated into a time series the way a
Prometheus label is, so this doesn't cause the same cardinality
*cost* here, but it does mean Jaeger's "search by operation name"
view won't group all `GET /tasks/{id}` requests together the way a
route-template-aware instrumentation would. Documented as a known gap
rather than solved, consistent with how docs/metrics.md treats the
same underlying limitation.

## Verifying this phase

Bring the stack up (`docker compose up --build`) and open the Jaeger
UI at `http://localhost:16686`. Create a task with `POST /tasks`,
select the `api` service in the Jaeger UI, and find its trace -- it
should show the HTTP request span as the root, with the outbox
relay's publish span and the worker's execution span nested
underneath, in order. To see a multi-attempt trace, create a task
whose handler is set up to fail a couple of times before succeeding
(or exceeds its `Task.timeout`) and confirm every attempt's
`worker.execute_task` span appears nested under the *same* trace
rather than as separate traces.
