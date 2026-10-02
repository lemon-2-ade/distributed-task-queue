"""
Decides which priority queue a worker should pull its *next* message
from (Phase 15) -- the "worker's consumption strategy" that
messaging/queues.py's module docstring names as the deliberate home
for the starvation tradeoff, and that services/worker/main.py's own
docstring has flagged as "not yet implemented" since Phase 6.

## Strict priority would starve lower tiers

The naive reading of "process high priority first" is: always drain
high_priority.queue completely before touching normal_priority.queue
at all, and normal completely before low. That's strict priority
scheduling, and it has an obvious failure mode under sustained load:
if high-priority tasks keep arriving as fast as (or faster than) they
can be processed, normal- and low-priority tasks never get a turn at
all, no matter how long they wait. "Priority" in this project's
requirements means *tasks that matter more get served sooner*, not
*tasks that matter less never get served while anything else exists*
-- those are different guarantees, and only the first one is the
actual goal.

## Smooth Weighted Round-Robin (SWRR)

This is the same algorithm nginx uses to pick an upstream server
among weighted candidates: give each priority a weight
(PRIORITY_WEIGHT_HIGH=4, _NORMAL=2, _LOW=1 by default -- high gets
picked roughly 4x as often as low, proportionally), and on every
pick, add each priority's weight to its running "current" score, then
select whichever has the highest current score and subtract the
total weight from it. Over any `sum(weights)`-length window this
produces exactly `weight` picks per priority, interleaved as evenly
as possible rather than in weight-sized blocks -- e.g. with
weights (4, 2, 1), the sequence looks like
`H, N, H, L, H, N, H` (total 7), not `H, H, H, H, N, N, L`. The
interleaved version matters for *latency*, not just throughput: a
normal-priority task waiting behind a block of 4 high-priority picks
in a row waits much longer on average than one waiting behind a
smooth, spread-out rotation, even though the long-run throughput
split is identical either way.

Every priority still gets picked eventually, on a bounded, predictable
schedule, regardless of how much high-priority traffic exists -- that
bound is exactly what strict priority scheduling doesn't give you.
"""

from domain.states import TaskPriority


class WeightedQueueSelector:
    def __init__(self, weights: dict[TaskPriority, int]) -> None:
        if not weights or any(w <= 0 for w in weights.values()):
            raise ValueError("all priority weights must be positive")
        self._weights = dict(weights)
        self._current = dict.fromkeys(self._weights, 0)
        self._total = sum(self._weights.values())

    def next_priority(self) -> TaskPriority:
        for priority, weight in self._weights.items():
            self._current[priority] += weight
        chosen = max(self._current, key=lambda p: self._current[p])
        self._current[chosen] -= self._total
        return chosen

    def fallback_order(self, preferred: TaskPriority) -> list[TaskPriority]:
        """
        The other priorities, in weight order (highest weight first),
        used when the SWRR-preferred queue turns out to be empty this
        turn. Falling through by weight (rather than, say, insertion
        order) means an empty high-priority queue's slot gets offered
        to normal before low, keeping the same "more important first"
        intent even on the fallback path -- not just on the common
        path. See services/worker/main.py's dispatch loop for why
        falling through at all matters: without it, an empty
        high-priority queue would stall normal/low throughput on
        every turn SWRR happens to prefer high, even though there's
        real work waiting elsewhere.
        """
        others = [p for p in self._weights if p != preferred]
        return sorted(others, key=lambda p: self._weights[p], reverse=True)
