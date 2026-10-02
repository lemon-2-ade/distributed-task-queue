"""
Redis-backed rate limiter for POST /tasks (Phase 14).

Why system-wide (one shared counter), not per-client: rate limiting
that matters needs to identify *who* is making requests, so one
caller's burst doesn't penalize everyone else's. This project has no
per-caller identity yet -- `API_KEY` (config.py) is a single shared
secret, not a per-client credential, and there's no concept of
tenants or API consumers. A per-client limiter built on top of that
would be theater: every caller shares the same key, so "per-client"
and "system-wide" would be the same thing anyway. This limiter is
honestly scoped to what the system actually has today -- protecting
the service as a whole from being overwhelmed, not fair-sharing
between callers it can't yet tell apart. A real per-client limiter
is a natural extension once there's an auth phase that gives each
caller its own identity to key the counter on.

## Algorithm: fixed window, not a token bucket or sliding window

A fixed window counter is the simplest version of this idea: pick
a window of `rate_limit_window_seconds`, count requests that land in
it with a single `INCR`, reject once the count passes
`rate_limit_requests_per_window`, and let the whole counter expire
and reset at the window boundary. It's one Redis round trip per
check and trivial to reason about.

Its well-known weakness: a window boundary lets up to 2x the nominal
rate through in a short burst (e.g. a window of
[00:00.5-00:01.0] can be nearly full right as a new window
[00:01.0-00:01.5] opens, so a flood centered on 00:01.0 sees close to
double the configured limit). A sliding-window log or a token bucket
avoids this by tracking request timestamps (or a continuously-refilling
budget) instead of discrete windows, at the cost of more Redis state
and a slightly more involved implementation (a token bucket typically
needs a Lua script for atomicity across its "refill, then spend"
steps). This project takes the simpler option and documents the
tradeoff rather than reaching for the fancier algorithm by default --
a fixed window is good enough to protect the service from sustained
overload, which is the actual goal here, even though it isn't a
perfectly smooth rate limit.
"""

import time

import redis.asyncio as redis

from config import get_settings


class RateLimiter:
    def __init__(self, redis_client: "redis.Redis | None" = None) -> None:
        settings = get_settings()
        self._redis = redis_client or redis.from_url(settings.redis_url, decode_responses=True)
        self._limit = settings.rate_limit_requests_per_window
        self._window_seconds = settings.rate_limit_window_seconds

    async def check(self, key: str = "global") -> tuple[bool, int]:
        """
        Increments this window's counter for `key` and reports
        whether the request is allowed. Returns (allowed,
        retry_after_seconds) -- retry_after_seconds is 0 when
        allowed, and otherwise how long until the current window
        rolls over (a reasonable, if not exact, value for a
        `Retry-After` response header).
        """
        now = time.time()
        window_start = int(now // self._window_seconds) * self._window_seconds
        redis_key = f"ratelimit:{key}:{window_start}"

        count = await self._redis.incr(redis_key)
        if count == 1:
            # First request to land in this window -- set the
            # window's own expiry so Redis cleans up old window keys
            # on its own rather than this ever needing a sweep.
            await self._redis.expire(redis_key, self._window_seconds)

        if count > self._limit:
            retry_after = self._window_seconds - (now - window_start)
            return False, max(1, int(retry_after) + 1)
        return True, 0

    async def close(self) -> None:
        await self._redis.aclose()
