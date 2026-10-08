"""
Shared distributed tracing infrastructure (Phase 19).

## Why OpenTelemetry, and why Jaeger with no separate Collector

OpenTelemetry is the vendor-neutral standard for traces: a `TracerProvider`
creates `Span`s, a `BatchSpanProcessor` buffers them, and an exporter ships
finished batches off-process. This project exports via OTLP/gRPC straight
into Jaeger's own built-in OTLP receiver -- Jaeger has accepted OTLP
natively since v1.35, so a standalone OpenTelemetry Collector would be
a second extra process sitting between every service and Jaeger for no
benefit this project needs (the Collector earns its keep when you want to
fan traces out to multiple backends, batch across many short-lived
processes, or scrub/sample centrally). That's a deliberate simplification,
documented here and in docs/tracing.md, not an oversight.

## The one problem this module exists to solve

OpenTelemetry's default context propagation is contextvars-based: a span
started inside an async call stack is automatically the parent of whatever
spans start further down that *same live stack*. That works perfectly
within one process -- e.g. FastAPIInstrumentor's auto-instrumentation
means every span created while handling one HTTP request is already
correctly nested. It breaks completely at this system's real
process boundaries, because the producer and consumer of a trace are
never on the same call stack at the same time:

  - the API (or scheduler) finishes its request long before a worker
    picks the task up off RabbitMQ;
  - a task's journey to the worker passes through a Postgres row (the
    transactional outbox -- see persistence/models.py's OutboxMessage)
    that sits inert for an arbitrary stretch of time with no Python
    process attached to it at all.

So trace context has to be carried as *data*, by hand, at each boundary:
serialized into the outbox row (new trace_context JSONB column), read
back out and re-injected into the AMQP message's headers by the outbox
relay right before it publishes, and extracted from those headers by the
worker when it receives the message. inject_trace_context() /
extract_trace_context() below are that serialization: thin wrappers over
OpenTelemetry's own W3C-standard `traceparent` propagator, using a plain
dict as the carrier (not Python's contextvars, which only exist within
one live process).

## Why retries continue the same trace instead of starting a new one

A task that fails and retries is, from an operator's point of view, one
story: "this task was attempted three times before it succeeded." Giving
each attempt a fresh trace_id would scatter that story across three
unrelated Jaeger traces with no link between them. Instead, this project
carries the *original* trace context forward into each retry's outbox
row (see services/worker/consumer.py), so every attempt becomes a child
span nested under the same root trace -- one Jaeger trace, however many
attempts it took.

## Why asyncpg and aio-pika aren't auto-instrumented

opentelemetry-instrumentation-asyncpg and -aio-pika exist and would save
some hand-written spans. This project doesn't use them, for the same
reason it doesn't use Celery/RQ: the point of this project is to show
*how* the pieces work, and a span per SQL query or per AMQP channel
operation is also just noise at the granularity anyone would actually
read in Jaeger -- it would bury the handful of spans that matter (one
per task attempt, one per outbox relay publish) under hundreds of
auto-generated ones. Spans here are placed by hand, only at the
boundaries that correspond to a meaningful step in a task's life.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Tracer

# Guards against calling setup_tracing() more than once per process
# (e.g. a test importing a service's main module twice) -- doing so
# would otherwise attach a second BatchSpanProcessor/exporter pair to
# the global TracerProvider and double-export every span.
_configured = False


def setup_tracing(
    service_name: str,
    *,
    otel_exporter_otlp_endpoint: str,
    otel_traces_enabled: bool,
) -> None:
    """
    Configures this process's global TracerProvider.

    When `otel_traces_enabled` is False, this deliberately does
    nothing and leaves the OpenTelemetry API's built-in no-op
    TracerProvider in place: get_tracer()/start_as_current_span() all
    keep working (so instrumented code never needs an `if tracing
    enabled` branch of its own), they just produce spans that are
    created and immediately discarded. That's the supported way to run
    this project locally/in tests with no Jaeger container up, rather
    than something this module has to special-case everywhere else.
    """
    global _configured
    if not otel_traces_enabled or _configured:
        return
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otel_exporter_otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _configured = True


def get_tracer(instrumenting_module_name: str) -> Tracer:
    """
    Thin wrapper over trace.get_tracer(), named to match the other
    helpers here. `instrumenting_module_name` is conventionally
    __name__ of the calling module -- it shows up as the span's
    instrumentation scope in Jaeger, which is useful for telling apart
    "a span named 'execute_task'" raised by the worker's consumer vs.
    (hypothetically) anywhere else.
    """
    return trace.get_tracer(instrumenting_module_name)


def inject_trace_context() -> dict[str, str]:
    """
    Captures the *currently active* span's context (i.e. whatever span
    is open on this call stack right now) as a plain dict of W3C
    `traceparent`/`tracestate` headers, suitable for storing as JSON
    (OutboxMessage.trace_context) or passing as AMQP message headers.

    Returns {} if no span is active (e.g. tracing disabled, or called
    outside any span) -- extract_trace_context() below treats an empty
    dict as "no parent," which is the correct, safe fallback: the next
    span just becomes its own trace root instead of erroring out.
    """
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier


def extract_trace_context(carrier: dict[str, str] | None) -> Context:
    """
    The inverse of inject_trace_context(): rebuilds an OpenTelemetry
    Context from a previously-captured dict, so that a span started
    with `context=extract_trace_context(...)` becomes a *child* of
    the span that called inject_trace_context(), even though the two
    calls happened in different processes with nothing but this dict
    passed between them (via a Postgres row, AMQP headers, or both in
    sequence).

    `carrier` is None-safe (a row/message with no stored trace context
    -- e.g. anything created before Phase 19 shipped) and extract()
    with an empty/missing carrier correctly returns a context with no
    parent, same as inject_trace_context()'s own empty-dict fallback.
    """
    return extract(carrier or {})


@contextmanager
def start_span_from_carrier(
    tracer: Tracer,
    span_name: str,
    carrier: dict[str, str] | None,
    **kwargs,
) -> Iterator[Span]:
    """
    Convenience combining extract_trace_context() with
    tracer.start_as_current_span(): starts `span_name` as a child of
    whatever trace context `carrier` holds (or as a fresh trace root,
    if `carrier` is empty/None), and makes it the active span for the
    duration of the `with` block. This is the shape every cross-process
    boundary in this system uses (worker consuming a task message,
    outbox relay re-publishing a row) -- extracting and starting are
    always done together, so keeping them as two separate calls at
    every call site would just be repeated boilerplate.
    """
    ctx = extract_trace_context(carrier)
    with tracer.start_as_current_span(span_name, context=ctx, **kwargs) as span:
        yield span
