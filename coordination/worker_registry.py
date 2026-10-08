"""
Redis-backed worker registry: how the system knows which worker
processes currently exist and are alive, without any worker having
to tell anyone when it dies.

Why Redis over Postgres for this: "is this worker still alive" is a
liveness question with a natural TTL answer -- a worker is alive as
long as it keeps refreshing its own entry, and the moment it stops
(clean shutdown, crash, OOM-kill, lost network), Redis expires the
entry on its own after WORKER_HEARTBEAT_TTL_SECONDS. Postgres has no
built-in "expire this row" primitive; doing the same thing there
would mean a periodic reaper job scanning for stale last_seen
timestamps and deleting them -- a whole extra moving part to build,
test, and keep running, for a value (worker liveness) that's
inherently transient and doesn't need durability across a restart
the way a Task row does. This is exactly the kind of state Redis is
for in this system: fast-changing, ephemeral, coordination data --
not the source of truth (that's still Postgres for tasks).

Each worker owns a single Redis hash key, `worker:{worker_id}`,
holding its metadata (queues it consumes, its concurrency limit, when
it started, when it last beat). The *whole key* carries a TTL,
refreshed on every heartbeat via EXPIRE. Two settings control this
(config.py, populated back in Phase 1's skeleton for exactly this):
  - WORKER_HEARTBEAT_INTERVAL_SECONDS: how often a worker refreshes.
  - WORKER_HEARTBEAT_TTL_SECONDS: how long an entry survives with no
    refresh before Redis deletes it on its own.
TTL is set well above the interval (15s vs 5s here) so that one or
two missed beats -- a GC pause, a brief Redis blip -- don't make a
live worker look dead. A worker only disappears from the registry
after it's missed several consecutive beats in a row.

Listing active workers uses SCAN (redis-py's async scan_iter), never
KEYS: KEYS blocks Redis's single-threaded event loop for the entire
scan on a large keyspace, so this project never uses it even though
at this project's scale it would "work fine" -- the point is to
demonstrate the production-correct pattern, not whatever happens not
to break here.
"""

import time
from typing import Any

import redis.asyncio as redis

from config import get_settings

WORKER_KEY_PREFIX = "worker:"


def _worker_key(worker_id: str) -> str:
    return f"{WORKER_KEY_PREFIX}{worker_id}"


class WorkerRegistry:
    """
    One instance per process that needs it: each worker owns one to
    register/heartbeat/deregister itself; the API owns one (read-only
    in practice) to answer GET /workers.
    """

    def __init__(self, redis_client: "redis.Redis | None" = None) -> None:
        settings = get_settings()
        self._redis = redis_client or redis.from_url(settings.redis_url, decode_responses=True)
        self._ttl_seconds = settings.worker_heartbeat_ttl_seconds

    async def register(self, worker_id: str, *, queues: list[str], concurrency: int) -> None:
        now = time.time()
        key = _worker_key(worker_id)
        await self._redis.hset(
            key,
            mapping={
                "worker_id": worker_id,
                "queues": ",".join(queues),
                "concurrency": concurrency,
                "started_at": now,
                "last_heartbeat_at": now,
                # active_task_count (Phase 11): how many messages this
                # worker currently has "in flight" through its handler
                # body -- see increment_load()/decrement_load() below,
                # and services/worker/consumer.py for where these are
                # called. Starts at 0 on every fresh registration.
                "active_task_count": 0,
            },
        )
        await self._redis.expire(key, self._ttl_seconds)

    async def heartbeat(self, worker_id: str) -> None:
        key = _worker_key(worker_id)
        # HSET on a key Redis has already reaped (this worker missed
        # its whole TTL window -- e.g. a long GC pause) silently
        # recreates a bare hash with just this field. That's fine:
        # the entry is momentarily missing `queues`/`concurrency`/
        # `started_at` until the *next* full register() call would
        # refill them, but it's alive and counted again immediately,
        # which is what matters for liveness. Nothing here requires
        # register() to be called again for heartbeat() to keep working.
        await self._redis.hset(key, "last_heartbeat_at", time.time())
        await self._redis.expire(key, self._ttl_seconds)

    async def increment_load(self, worker_id: str) -> None:
        # HINCRBY is a single atomic Redis command -- safe to call
        # concurrently from every in-flight handler on this worker
        # (up to WORKER_CONCURRENCY of them, see consumer.py) without
        # a read-modify-write race, which a naive "read the field,
        # add one, write it back" would have.
        #
        # Phase 21 (chaos testing) found that this call, unguarded,
        # sat *before* services/worker/consumer.py's main try/finally
        # block -- so a Redis outage raised here before a single
        # message of any task_type was ever processed, took down the
        # whole handle() call (the message left unacked), and did so
        # for every message this worker received for as long as Redis
        # stayed down. That's strictly worse than this project's
        # existing stance elsewhere (heartbeat()'s docstring already
        # treats a Redis blip as something to absorb, not crash over):
        # load tracking is a load-balancing *signal*, not something
        # task correctness depends on, so it should degrade (stale/
        # inaccurate load numbers) rather than take task processing
        # down with it. See docs/chaos-testing.md's Redis-outage
        # scenario for how this was found.
        try:
            await self._redis.hincrby(_worker_key(worker_id), "active_task_count", 1)
        except Exception:
            pass

    async def decrement_load(self, worker_id: str) -> None:
        # Never let this go negative -- e.g. a decrement arriving after
        # the key already expired and got silently recreated by a
        # heartbeat (see heartbeat()'s docstring for that same
        # recreate-on-expiry behavior), which would start this field
        # at -1 instead of 0. A negative count would make this worker
        # look *more* available than an idle one to
        # LeastLoadedStrategy, which is the opposite of correct.
        #
        # Same Phase 21 reasoning as increment_load() above: this call
        # sits in services/worker/consumer.py's outer `finally`, so an
        # unguarded failure here would mask whatever the handler
        # actually did (success or failure) behind a Redis exception
        # raised while just trying to clean up bookkeeping -- best-
        # effort, same as increment_load().
        try:
            new_value = await self._redis.hincrby(_worker_key(worker_id), "active_task_count", -1)
            if new_value < 0:
                await self._redis.hset(_worker_key(worker_id), "active_task_count", 0)
        except Exception:
            pass

    async def deregister(self, worker_id: str) -> None:
        # Explicit removal on graceful shutdown, so a clean stop
        # doesn't leave a stale-looking entry sitting around for the
        # rest of the TTL window before it would have expired anyway.
        await self._redis.delete(_worker_key(worker_id))

    async def list_active_workers(self) -> list[dict[str, Any]]:
        workers = []
        async for key in self._redis.scan_iter(match=f"{WORKER_KEY_PREFIX}*", count=100):
            data = await self._redis.hgetall(key)
            if data:
                # Between SCAN yielding this key and HGETALL reading
                # it, the key can expire out from under us -- an
                # empty result here just means "gone now," not an
                # error, so it's skipped rather than raised.
                workers.append(data)
        return workers

    async def ping(self) -> bool:
        try:
            return await self._redis.ping()
        except Exception:
            return False

    async def close(self) -> None:
        await self._redis.aclose()
