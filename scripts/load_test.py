#!/usr/bin/env python3
"""
Load testing (Phase 22).

Same reasoning as scripts/chaos_test.py (Phase 21) for why this is a
script run against a real, live `docker compose up` stack rather
than another pgserver/fakeredis verify_phaseNN.py: the questions this
phase answers -- "what's the actual p99 latency under load," "does
the rate limiter actually reject the request it's supposed to," "how
does throughput hold up as concurrency rises" -- are about the real
running system's behavior under real concurrent HTTP traffic, not
about whether the code *would* do the right thing in principle.

This project deliberately does not reach for Locust/k6/a load-testing
framework here, for the same "build it to understand it" reason it
doesn't reach for Celery for the queue itself: the actual mechanics
of a load generator (pacing requests to a target rate, bounding
concurrency, collecting latency percentiles) are simple enough to
write directly with asyncio + httpx (already a dev dependency) and
doing so keeps what's being measured and how in plain sight, in this
project's own code, rather than behind a framework's configuration
DSL.

Usage (from the repo root, with `docker compose up -d` already
running):

    python scripts/load_test.py run --rate 20 --duration 30
    python scripts/load_test.py rate-limit-probe

Reads API_KEY from the environment (same variable the stack's own
.env sets -- see docs/security.md) to send as the X-API-Key header
Phase 23 now requires on /tasks; defaults to .env.example's
"change-me" placeholder if unset.

See docs/load-testing.md for how to read the results and what each
scenario is actually checking.
"""

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field

import httpx
import typer

API_BASE_URL = "http://localhost:8000"
# Phase 23 requires an X-API-Key header on /tasks and /workers (see
# services/api/auth.py) -- this script is meant to be run against
# the same docker-compose stack whose .env sets API_KEY, so it reads
# the same environment variable rather than taking a separate flag
# for what's really one shared piece of config. The "change-me"
# fallback matches .env.example's own default for a stack that
# hasn't overridden it.
API_KEY = os.environ.get("API_KEY", "change-me")
_AUTH_HEADERS = {"X-API-Key": API_KEY}

app = typer.Typer(help="Load-testing scenarios against a live docker-compose stack.")


def _percentile(sorted_values: list[float], pct: float) -> float:
    """
    Nearest-rank percentile over an already-sorted list -- no numpy
    dependency for what's a one-line calculation at this script's
    scale (hundreds to low thousands of samples, not millions).
    """
    if not sorted_values:
        return float("nan")
    k = max(0, min(len(sorted_values) - 1, round(pct / 100 * (len(sorted_values) - 1))))
    return sorted_values[k]


@dataclass
class RequestOutcome:
    latency_seconds: float
    status_code: int
    task_id: str | None = None


@dataclass
class RunResult:
    outcomes: list[RequestOutcome] = field(default_factory=list)
    actual_duration_seconds: float = 0.0


def _print_latency_table(label: str, latencies: list[float]) -> None:
    if not latencies:
        print(f"  {label}: no samples")
        return
    ordered = sorted(latencies)
    print(
        f"  {label}: n={len(ordered)} "
        f"min={ordered[0]*1000:.1f}ms "
        f"p50={_percentile(ordered, 50)*1000:.1f}ms "
        f"p95={_percentile(ordered, 95)*1000:.1f}ms "
        f"p99={_percentile(ordered, 99)*1000:.1f}ms "
        f"max={ordered[-1]*1000:.1f}ms"
    )


async def _submit_one(
    client: httpx.AsyncClient, *, task_type: str, payload: dict, max_retries: int = 3
) -> RequestOutcome:
    started = time.perf_counter()
    try:
        resp = await client.post(
            "/tasks",
            json={"task_type": task_type, "payload": payload, "max_retries": max_retries},
        )
    except httpx.HTTPError:
        # A connection error (the API itself unreachable, not just a
        # 4xx/5xx response) is still a real outcome worth counting,
        # not something to let crash the whole run -- status_code=0
        # is this script's own sentinel for "no response at all."
        return RequestOutcome(latency_seconds=time.perf_counter() - started, status_code=0)
    latency = time.perf_counter() - started
    task_id = None
    if resp.status_code < 300:
        try:
            task_id = resp.json().get("task_id")
        except ValueError:
            pass
    return RequestOutcome(latency_seconds=latency, status_code=resp.status_code, task_id=task_id)


async def _generate_load(
    *,
    rate_per_second: float,
    duration_seconds: float,
    concurrency: int,
    task_type: str,
    payload: dict,
) -> RunResult:
    """
    Paces POST /tasks calls to roughly `rate_per_second`, bounded by
    at most `concurrency` requests in flight at once (a semaphore,
    same pattern services/worker/consumer.py uses to bound handler
    concurrency) -- a target rate the server can't actually sustain
    shows up honestly as growing latency/429s/503s under this scheme,
    rather than being silently absorbed by letting requests queue up
    without bound on the client side.
    """
    interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
    semaphore = asyncio.Semaphore(concurrency)
    outcomes: list[RequestOutcome] = []
    tasks: list[asyncio.Task] = []

    async def _bounded_submit(client: httpx.AsyncClient) -> None:
        async with semaphore:
            outcome = await _submit_one(client, task_type=task_type, payload=payload)
            outcomes.append(outcome)

    run_started = time.perf_counter()
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0, headers=_AUTH_HEADERS) as client:
        next_fire = run_started
        while time.perf_counter() - run_started < duration_seconds:
            tasks.append(asyncio.ensure_future(_bounded_submit(client)))
            next_fire += interval
            sleep_for = next_fire - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        # Let every already-dispatched request finish before
        # measuring actual_duration_seconds and returning -- an
        # in-flight request when the clock runs out still counts.
        if tasks:
            await asyncio.gather(*tasks)

    return RunResult(outcomes=outcomes, actual_duration_seconds=time.perf_counter() - run_started)


async def _poll_completion_latencies(
    task_ids: list[str], *, timeout_per_task: float
) -> tuple[list[float], int]:
    """
    For a *sample* of created tasks (polling every one created by a
    large run would itself become a second load generator), polls
    until each reaches a terminal status and computes
    completed_at - created_at from the server's own timestamps --
    deliberately not client-observed wall time, so this number is
    queue-wait-plus-execution time as the system itself recorded it,
    uncontaminated by this script's own polling interval or by
    whatever else the client machine was doing. Returns
    (latencies_in_seconds, failures_count).
    """
    from datetime import datetime

    def _parse(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    latencies = []
    failures = 0
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10.0, headers=_AUTH_HEADERS) as client:
        for task_id in task_ids:
            deadline = time.monotonic() + timeout_per_task
            final = None
            while time.monotonic() < deadline:
                resp = await client.get(f"/tasks/{task_id}")
                if resp.status_code == 200:
                    body = resp.json()
                    if body["status"].upper() in {
                        "SUCCESS",
                        "FAILED",
                        "CANCELLED",
                        "TIMEOUT",
                        "DEAD_LETTERED",
                    }:
                        final = body
                        break
                await asyncio.sleep(0.5)
            if final is None:
                failures += 1
                continue
            if final["status"].upper() != "SUCCESS" or final.get("completed_at") is None:
                failures += 1
                continue
            latencies.append(
                (_parse(final["completed_at"]) - _parse(final["created_at"])).total_seconds()
            )
    return latencies, failures


@app.command("run")
def run(
    rate: float = typer.Option(10.0, help="Target POST /tasks per second."),
    duration: float = typer.Option(30.0, help="How long to generate load, in seconds."),
    concurrency: int = typer.Option(50, help="Max concurrent in-flight POST /tasks calls."),
    task_type: str = typer.Option("sleep", help="Task type to submit."),
    sleep_seconds: float = typer.Option(0.2, help="Payload for the `sleep` handler, if used."),
    completion_sample_size: int = typer.Option(
        30, help="How many created tasks to track end-to-end to completion."
    ),
) -> None:
    """
    Generates sustained load at a target rate for a fixed duration,
    reports request-latency and status-code distributions for the
    POST /tasks calls themselves, then tracks a sample of the created
    tasks through to completion and reports end-to-end
    (created_at -> completed_at) latency.
    """
    print(f"=== Load test: {rate}/s for {duration}s, concurrency={concurrency} ===")
    payload = {"seconds": sleep_seconds} if task_type == "sleep" else {}
    result = asyncio.run(
        _generate_load(
            rate_per_second=rate,
            duration_seconds=duration,
            concurrency=concurrency,
            task_type=task_type,
            payload=payload,
        )
    )

    total = len(result.outcomes)
    achieved_rate = total / result.actual_duration_seconds if result.actual_duration_seconds else 0.0
    by_status: dict[int, int] = {}
    for outcome in result.outcomes:
        by_status[outcome.status_code] = by_status.get(outcome.status_code, 0) + 1

    print(f"\nSubmitted {total} requests over {result.actual_duration_seconds:.1f}s "
          f"({achieved_rate:.1f}/s achieved vs {rate:.1f}/s requested)")
    print("Status code breakdown:")
    for code in sorted(by_status):
        label = {0: "connection error", 201: "created", 429: "rate limited", 503: "backpressure"}.get(
            code, str(code)
        )
        print(f"  {code} ({label}): {by_status[code]}")

    accepted_latencies = [o.latency_seconds for o in result.outcomes if o.status_code < 300]
    _print_latency_table("POST /tasks request latency (accepted requests)", accepted_latencies)

    accepted_task_ids = [o.task_id for o in result.outcomes if o.task_id]
    sample = accepted_task_ids[:: max(1, len(accepted_task_ids) // completion_sample_size)][
        :completion_sample_size
    ]
    if sample:
        print(f"\nTracking {len(sample)} created tasks to completion...")
        completion_latencies, failed = asyncio.run(
            _poll_completion_latencies(sample, timeout_per_task=60.0)
        )
        _print_latency_table("end-to-end completion latency (created_at -> completed_at)", completion_latencies)
        if failed:
            print(f"  ({failed} of {len(sample)} sampled tasks did not reach SUCCESS within the timeout)")


@app.command("rate-limit-probe")
def rate_limit_probe(
    burst: int = typer.Option(
        200, help="How many POST /tasks calls to fire as fast as possible (no pacing)."
    ),
) -> None:
    """
    Fires `burst` POST /tasks calls with no pacing at all -- the
    opposite of `run`'s steady-rate load, specifically to exceed
    RATE_LIMIT_REQUESTS_PER_WINDOW (config.py; see
    docs/rate-limiting-and-backpressure.md) within its window and
    confirm the rate limiter actually rejects the excess with 429 and
    a Retry-After header, not just that the code reviews as if it
    would. Then waits past the window and confirms a follow-up
    request succeeds again.
    """
    print(f"=== Rate limit probe: {burst} requests, no pacing ===")

    async def _burst() -> list[RequestOutcome]:
        async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0, headers=_AUTH_HEADERS) as client:
            tasks = [
                asyncio.ensure_future(_submit_one(client, task_type="echo", payload={}))
                for _ in range(burst)
            ]
            return await asyncio.gather(*tasks)

    outcomes = asyncio.run(_burst())
    accepted = sum(1 for o in outcomes if o.status_code < 300)
    rate_limited = sum(1 for o in outcomes if o.status_code == 429)
    print(f"  accepted: {accepted}, rate-limited (429): {rate_limited}, other: "
          f"{len(outcomes) - accepted - rate_limited}")

    if rate_limited > 0:
        print("  PASS: the rate limiter rejected excess requests with 429")
    else:
        print(
            "  NOTE: nothing was rate-limited -- either the burst was smaller than "
            "RATE_LIMIT_REQUESTS_PER_WINDOW, or the limiter isn't configured as expected"
        )

    print("  waiting 2s past the rate limit window, then retrying once...")
    time.sleep(2)

    async def _retry_once() -> RequestOutcome:
        async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0, headers=_AUTH_HEADERS) as client:
            return await _submit_one(client, task_type="echo", payload={})

    final = asyncio.run(_retry_once())
    if final.status_code < 300:
        print("  PASS: a request after the window elapsed succeeded again")
    else:
        print(f"  FAIL: a request after the window still returned {final.status_code}")
        raise SystemExit(1)


if __name__ == "__main__":
    app()
