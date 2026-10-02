"""
Application-level worker selection -- strategies for "if something
needs to choose one specific worker, which one should it pick."

This is a deliberately separate concern from RabbitMQ's own consumer
dispatch (round-robin across connected consumers on a queue,
modulated by prefetch -- see docs/architecture.md's "RabbitMQ
dispatch vs. application-level load balancing" section). RabbitMQ has
no concept of "worker capacity" when it decides who gets the next
message; these strategies are for the cases where something *does*
need that concept -- an admin view of which worker should pick up
the next item, or (from Phase 16 onward) the scheduler choosing where
scheduled work should land. Nothing in this phase wires a strategy
into actual message dispatch yet; GET /workers/select (Phase 11)
exposes it as a read-only "who would be picked" view precisely so the
selection logic can be built, documented, and tested on its own,
before anything depends on it for real routing decisions.

Both strategies operate on the same worker-dict shape
WorkerRegistry.list_active_workers() returns (string-valued fields,
since that's what Redis hashes give back) and are otherwise stateless
functions of "the current snapshot of alive workers" -- neither one
talks to Redis directly, which is what makes them trivial to unit
test without a broker or cache running at all.
"""

import itertools
from abc import ABC, abstractmethod
from typing import Any


class LoadBalancingStrategy(ABC):
    @abstractmethod
    def select(self, workers: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Pick one worker from the given alive-workers snapshot, or
        return None if the list is empty (nobody to pick)."""


class RoundRobinStrategy(LoadBalancingStrategy):
    """
    Cycles through the alive workers in a fixed order, independent of
    how loaded each one currently is -- the simplest possible
    fairness guarantee ("everyone gets a turn eventually"), and the
    right baseline to compare LeastLoadedStrategy against.

    The worker list is sorted by worker_id before indexing into it.
    Redis's SCAN (which list_active_workers() uses) makes no
    ordering guarantee between calls, so without an explicit sort,
    "round-robin" would really be "pick whichever worker SCAN
    happened to return at this position this time" -- not round-robin
    at all. Sorting makes the order deterministic across calls so the
    internal counter actually rotates through the full set evenly.

    One instance should be reused across calls (held on app.state,
    not constructed fresh per-request) -- a fresh instance resets the
    counter to 0 every time, which would make it pick the same
    (alphabetically first) worker on every single call instead of
    rotating.
    """

    def __init__(self) -> None:
        self._counter = itertools.count()

    def select(self, workers: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not workers:
            return None
        ordered = sorted(workers, key=lambda w: w["worker_id"])
        index = next(self._counter) % len(ordered)
        return ordered[index]


class LeastLoadedStrategy(LoadBalancingStrategy):
    """
    Picks the worker with the fewest in-flight tasks right now
    (WorkerRegistry's active_task_count, Phase 11). Unlike
    RoundRobinStrategy, this actually looks at current load, so it
    can route around a worker that happens to be busy with a few
    slow tasks even if it "should" be next in a strict rotation.

    Ties (e.g. several freshly-started, all-idle workers) break on
    worker_id for determinism -- "pick arbitrarily" would make this
    strategy's output non-reproducible from the same input, which
    makes it needlessly hard to reason about or test.

    Stateless: a fresh instance is equivalent to any other, unlike
    RoundRobinStrategy, since nothing here depends on call history.
    """

    def select(self, workers: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not workers:
            return None
        return min(
            workers,
            key=lambda w: (int(w.get("active_task_count", 0)), w["worker_id"]),
        )


STRATEGIES: dict[str, type[LoadBalancingStrategy]] = {
    "round_robin": RoundRobinStrategy,
    "least_loaded": LeastLoadedStrategy,
}
