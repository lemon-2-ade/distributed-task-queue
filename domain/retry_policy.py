"""
Exponential backoff with jitter -- pure computation, no I/O, no
framework imports, so it's trivially unit-testable and reusable from
both the worker (Phase 8) and, if needed later, an admin tool.

    attempt 1 -> base * 2^0 = base
    attempt 2 -> base * 2^1
    attempt 3 -> base * 2^2
    attempt 4 -> base * 2^3
    ...

capped at `max_delay` so a task that's failed many times doesn't end
up waiting hours between attempts. Jitter is then applied as a
random +/- `jitter_fraction` adjustment on top of the capped delay --
see docs/retries.md for why this matters (retry storms): without
jitter, every task that failed at the same moment (e.g. because a
downstream dependency went down) would also *retry* at the same
moment, turning one outage into a synchronized thundering herd
against whatever's already struggling.
"""

import random

DEFAULT_BASE_DELAY_SECONDS = 1.0
DEFAULT_MAX_DELAY_SECONDS = 60.0
DEFAULT_JITTER_FRACTION = 0.2  # +/- 20% of the capped delay


def compute_backoff_seconds(
    attempt: int,
    *,
    base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    max_delay: float = DEFAULT_MAX_DELAY_SECONDS,
    jitter_fraction: float = DEFAULT_JITTER_FRACTION,
) -> float:
    if attempt < 1:
        raise ValueError("attempt must be >= 1")

    raw_delay = base_delay * (2 ** (attempt - 1))
    capped_delay = min(raw_delay, max_delay)

    jitter_span = capped_delay * jitter_fraction
    jittered_delay = capped_delay + random.uniform(-jitter_span, jitter_span)

    return max(0.0, jittered_delay)
