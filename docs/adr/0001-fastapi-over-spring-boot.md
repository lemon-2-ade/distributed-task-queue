# ADR-001: FastAPI instead of Java/Spring Boot

## Context
This project's explicit goal is to learn distributed-systems internals
(queues, coordination, retries, consistency) rather than to learn a new
web framework or language. The implementer already needs to reason deeply
about asyncio-based concurrency for the worker fleet, since that's where
most of the interesting scheduling/concurrency logic lives.

## Decision
Use Python 3.12+ with FastAPI for the API layer, and asyncio throughout
the worker/scheduler services. Java and Spring Boot are explicitly
excluded.

## Alternatives
- **Spring Boot**: mature, widely used in production task-queue-adjacent
  systems, but introduces a second language/runtime and its own
  concurrency model (threads + reactor types), which would split focus
  away from the distributed-systems learning goal.
- **Flask / Django**: viable, but lack first-class native async support
  and typed request/response validation without extra libraries.
- **Go**: excellent for this kind of system, but the project's stated
  requirement is a Python backend.

## Tradeoffs
FastAPI + asyncio gives native async I/O, Pydantic validation, and
automatic OpenAPI docs, at the cost of Python's GIL limiting true CPU
parallelism within a single process — which is exactly why the worker
fleet scales via multiple processes/containers rather than in-process
threads (see `docs/architecture.md` and the concurrency discussion added
in the worker-service phase).

## Consequences
One language end-to-end (API, workers, scheduler, tests, tooling), which
keeps the project's actual subject — distributed coordination — the
focus instead of cross-language plumbing.
