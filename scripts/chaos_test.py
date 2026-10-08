#!/usr/bin/env python3
"""
Chaos / failure-injection scenarios (Phase 21).

Unlike every other verification script this project has used
(verify_phaseNN.py, run against an embedded pgserver/fakeredis stack
with no Docker involved -- see docs/ for how each earlier phase was
checked locally), these scenarios need the *real* thing: real
RabbitMQ, real Postgres, real Redis, actually killed and restarted
underneath a running system, to answer questions no amount of
mocking can answer -- "does this system actually survive the
dependency it depends on disappearing," not "does the code I wrote
to survive it get called when I simulate the failure myself." So
this script is meant to be run against a live `docker compose up`
stack on a real machine, not in this project's sandboxed local dev
loop, and it is committed to the repo (unlike the throwaway
verify_phaseNN.py scripts) specifically so it can be rerun by anyone
against their own stack, any time, not just once during development.

Usage (from the repo root, with `docker compose up -d` already
running):

    python scripts/chaos_test.py worker-crash
    python scripts/chaos_test.py rabbitmq-outage
    python scripts/chaos_test.py postgres-outage
    python scripts/chaos_test.py redis-outage
    python scripts/chaos_test.py run-all

See docs/chaos-testing.md for what each scenario actually proves,
and for the one real gap this phase found and fixed
(coordination/worker_registry.py's increment_load()/decrement_load()
used to crash task handling outright on a Redis outage -- see that
file's Phase 21 comments).
"""

import re
import subprocess
import sys
import time
import uuid

import httpx
import typer

API_BASE_URL = "http://localhost:8000"
COMPOSE_PROJECT_DIR = "."

app = typer.Typer(help="Chaos/failure-injection scenarios against a live docker-compose stack.")


def _compose(*args: str) -> None:
    cmd = ["docker", "compose", *args]
    print(f"    $ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=COMPOSE_PROJECT_DIR, check=True)


def _create_task(client: httpx.Client, *, task_type: str, payload: dict, max_retries: int = 3) -> str:
    resp = client.post(
        "/tasks",
        json={"task_type": task_type, "payload": payload, "max_retries": max_retries},
    )
    resp.raise_for_status()
    return resp.json()["task_id"]


def _get_task(client: httpx.Client, task_id: str) -> dict:
    resp = client.get(f"/tasks/{task_id}")
    resp.raise_for_status()
    return resp.json()


def _poll_until(client: httpx.Client, task_id: str, *, statuses: set[str], timeout: float) -> dict:
    """Polls GET /tasks/{id} until its status (matched
    case-insensitively -- domain/states/task_status.py's TaskStatus
    values are uppercase, but this script's callers read more
    naturally written lowercase) is one of `statuses`, or raises
    TimeoutError. The simplest possible wait primitive -- good enough
    for a chaos script run by a human watching it, deliberately not
    trying to be a generic test-polling library."""
    wanted = {s.upper() for s in statuses}
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = _get_task(client, task_id)
        if last["status"].upper() in wanted:
            return last
        time.sleep(1)
    raise TimeoutError(
        f"task {task_id} did not reach {statuses} within {timeout}s "
        f"(last seen status: {last['status'] if last else 'unknown'})"
    )


def _scrape_metric(client: httpx.Client, metric_name: str) -> float | None:
    """
    Parses one gauge's current value out of the API's /metrics
    (Prometheus text-exposition format) -- a tiny hand-rolled parser
    rather than a dependency, since all this script needs is "find
    the line that starts with this exact metric name and no label
    suffix, read the trailing number."
    """
    resp = client.get("/metrics")
    resp.raise_for_status()
    pattern = re.compile(rf"^{re.escape(metric_name)}(\{{[^}}]*\}})?\s+([0-9.eE+-]+)$")
    for line in resp.text.splitlines():
        match = pattern.match(line)
        if match:
            return float(match.group(2))
    return None


def _ready_detail(client: httpx.Client) -> tuple[int, str]:
    resp = client.get("/ready")
    detail = resp.json().get("detail", "")
    return resp.status_code, detail


def _ok(message: str) -> None:
    print(f"  PASS: {message}")


def _fail(message: str) -> None:
    print(f"  FAIL: {message}")
    raise SystemExit(1)


@app.command("worker-crash")
def worker_crash() -> None:
    """
    Kills the worker process (SIGKILL) while it's mid-execution of a
    task, and confirms the task is redelivered and still completes.

    This is the most direct possible exercise of the "why ACK timing
    matters" design in docs/rabbitmq.md: a worker that dies between
    receiving a message and acking it leaves that message unacked,
    and RabbitMQ's at-least-once delivery guarantee means another
    consumer eventually gets it. It is also a live demonstration of
    why docs/idempotency.md insists handlers be written to tolerate
    re-execution -- the `sleep` handler used here is harmless to
    re-run, but a real handler with side effects (charging a card,
    sending an email) would need the idempotency-key machinery this
    project already built (Phase 13) to avoid doing that twice.
    """
    print("=== Scenario: worker crash mid-task ===")
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        task_id = _create_task(client, task_type="sleep", payload={"seconds": 8})
        print(f"  created task {task_id} (sleep 8s)")

        _poll_until(client, task_id, statuses={"running"}, timeout=15.0)
        _ok("task reached RUNNING")

        # Give it a couple seconds of real execution time before
        # pulling the rug out, so this is a genuine mid-flight kill,
        # not a race against the RUNNING transition itself.
        time.sleep(2)
        print("  killing the worker process (SIGKILL)...")
        _compose("kill", "-s", "SIGKILL", "worker")

        # restart: unless-stopped (docker-compose.yml) brings a fresh
        # worker process up on its own -- no action needed here, just
        # patience. Total budget: time for Docker to notice and
        # restart the container, plus the full 8s re-execution of the
        # handler on whichever worker (the same one, restarted, or
        # another replica) picks the redelivered message up.
        final = _poll_until(client, task_id, statuses={"success", "dead_lettered"}, timeout=60.0)

    if final["status"].upper() == "SUCCESS":
        _ok(f"task {task_id} completed SUCCESS after the worker was killed and restarted")
    else:
        _fail(f"task {task_id} ended as {final['status']!r}, not success")


@app.command("rabbitmq-outage")
def rabbitmq_outage() -> None:
    """
    Stops RabbitMQ, confirms new tasks still get created (the outbox
    absorbs them without ever touching the broker -- see
    docs/outbox.md), confirms the unpublished-row backlog actually
    grows while the broker is down, then restarts RabbitMQ and
    confirms the backlog drains back to zero and every task that was
    "created" during the outage still, eventually, actually runs.
    This is the entire reason the Transactional Outbox pattern
    (Phase 17) exists: a would-be direct publish failing outright
    during the outage is exactly the dual-write problem Phase 17
    replaced with "durably record the intent, publish it whenever
    the broker is next reachable."
    """
    print("=== Scenario: RabbitMQ outage ===")
    task_count = 5
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        print("  stopping rabbitmq...")
        _compose("stop", "rabbitmq")

        status_code, detail = _ready_detail(client)
        if status_code == 503 and "rabbitmq unavailable" in detail:
            _ok("/ready correctly reports rabbitmq unavailable")
        else:
            _fail(f"/ready did not flag the outage (status={status_code}, detail={detail!r})")

        task_ids = [
            _create_task(client, task_type="echo", payload={"n": i}) for i in range(task_count)
        ]
        _ok(f"created {task_count} tasks while RabbitMQ was down (outbox absorbed them)")

        backlog = _scrape_metric(client, "outbox_unpublished_rows")
        if backlog is not None and backlog >= task_count:
            _ok(f"outbox_unpublished_rows backlog grew to {backlog} (>= {task_count})")
        else:
            _fail(f"expected outbox_unpublished_rows >= {task_count}, saw {backlog}")

        print("  restarting rabbitmq...")
        _compose("start", "rabbitmq")

        deadline = time.monotonic() + 60.0
        drained = False
        while time.monotonic() < deadline:
            backlog = _scrape_metric(client, "outbox_unpublished_rows")
            if backlog == 0:
                drained = True
                break
            time.sleep(2)
        if drained:
            _ok("outbox backlog drained back to 0 after RabbitMQ recovered")
        else:
            _fail(f"outbox backlog did not drain within 60s (last seen: {backlog})")

        for task_id in task_ids:
            final = _poll_until(client, task_id, statuses={"success", "dead_lettered"}, timeout=30.0)
            if final["status"].upper() != "SUCCESS":
                _fail(f"task {task_id} ended as {final['status']!r}, not success")
    _ok(f"all {task_count} tasks created during the outage completed SUCCESS")


@app.command("postgres-outage")
def postgres_outage() -> None:
    """
    Stops Postgres and confirms /ready correctly flags it (and only
    it -- RabbitMQ/Redis stay reported healthy, proving health.py's
    three checks are actually independent of each other, not one
    check that fails closed on anything). Restarts Postgres and
    confirms the system resumes normal operation (a fresh task
    submitted after recovery completes) without needing anything
    restarted by hand -- SQLAlchemy's connection pool reconnects on
    its own once the database is reachable again.
    """
    print("=== Scenario: Postgres outage ===")
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        print("  stopping postgres...")
        _compose("stop", "postgres")

        status_code, detail = _ready_detail(client)
        if status_code == 503 and "database unavailable" in detail:
            _ok("/ready correctly reports database unavailable")
        else:
            _fail(f"/ready did not flag the outage (status={status_code}, detail={detail!r})")
        if "rabbitmq unavailable" not in detail and "redis unavailable" not in detail:
            _ok("/ready did not falsely flag rabbitmq/redis -- the three checks are independent")
        else:
            _fail(f"/ready over-reported unrelated outages: {detail!r}")

        print("  restarting postgres...")
        _compose("start", "postgres")

        deadline = time.monotonic() + 60.0
        recovered = False
        while time.monotonic() < deadline:
            status_code, _ = _ready_detail(client)
            if status_code == 200:
                recovered = True
                break
            time.sleep(2)
        if recovered:
            _ok("/ready returned to 200 after Postgres came back")
        else:
            _fail("/ready did not recover within 60s of Postgres restarting")

        task_id = _create_task(client, task_type="echo", payload={"after": "postgres-outage"})
        final = _poll_until(client, task_id, statuses={"success", "dead_lettered"}, timeout=30.0)
    if final["status"].upper() == "SUCCESS":
        _ok("a task submitted after recovery completed SUCCESS with no manual intervention")
    else:
        _fail(f"post-recovery task ended as {final['status']!r}, not success")


@app.command("redis-outage")
def redis_outage() -> None:
    """
    Stops Redis and confirms /ready flags it in isolation (same
    independence check as postgres-outage), then -- the actual point
    of this scenario -- submits a task *while Redis is still down*
    and confirms it still completes. Before Phase 21,
    coordination/worker_registry.py's increment_load()/
    decrement_load() had no failure handling at all, so this exact
    scenario would have crashed services/worker/consumer.py's
    handle() before the handler ever ran, for every single message,
    for as long as Redis stayed down -- a real gap this chaos test
    exists to catch, now fixed (see that module's Phase 21 comments
    and docs/chaos-testing.md).
    """
    print("=== Scenario: Redis outage ===")
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        print("  stopping redis...")
        _compose("stop", "redis")

        status_code, detail = _ready_detail(client)
        if status_code == 503 and "redis unavailable" in detail:
            _ok("/ready correctly reports redis unavailable")
        else:
            _fail(f"/ready did not flag the outage (status={status_code}, detail={detail!r})")
        if "database unavailable" not in detail and "rabbitmq unavailable" not in detail:
            _ok("/ready did not falsely flag postgres/rabbitmq -- the three checks are independent")
        else:
            _fail(f"/ready over-reported unrelated outages: {detail!r}")

        task_id = _create_task(client, task_type="echo", payload={"during": "redis-outage"})
        final = _poll_until(client, task_id, statuses={"success", "dead_lettered"}, timeout=30.0)
        if final["status"].upper() == "SUCCESS":
            _ok(
                "a task submitted WHILE Redis was down still completed SUCCESS "
                "(load-tracking degraded gracefully instead of crashing task handling)"
            )
        else:
            _fail(f"task ended as {final['status']!r}, not success, with Redis down")

        print("  restarting redis...")
        _compose("start", "redis")

        deadline = time.monotonic() + 30.0
        recovered = False
        while time.monotonic() < deadline:
            status_code, _ = _ready_detail(client)
            if status_code == 200:
                recovered = True
                break
            time.sleep(2)
    if recovered:
        _ok("/ready returned to 200 after Redis came back")
    else:
        _fail("/ready did not recover within 30s of Redis restarting")


@app.command("run-all")
def run_all() -> None:
    """Runs every scenario in sequence, stopping at the first failure."""
    scenarios = [worker_crash, rabbitmq_outage, postgres_outage, redis_outage]
    for scenario in scenarios:
        scenario()
        print()
    print("All chaos scenarios passed.")


if __name__ == "__main__":
    app()
