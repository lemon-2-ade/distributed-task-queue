"""
Shared application settings.

Why this lives at the repo root rather than inside services/api/:
the API, worker, and scheduler are three separate deployable
processes, but they all read the *same* environment (one Postgres,
one RabbitMQ, one Redis, one .env file — see docker-compose.yml).
Giving each service its own copy of this class would mean three
places to keep in sync every time an env var is added or renamed.
A single Settings class, imported by whichever service needs it, is
the simpler and less error-prone option here.

pydantic-settings (BaseSettings) is used instead of plain
os.environ.get(...) calls for two reasons:
  1. Type coercion + validation happen once, at process startup,
     instead of being rediscovered at the moment a bad value is used.
  2. It doubles as executable documentation of every configuration
     value the system depends on.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- General ----
    environment: str = "development"
    log_level: str = "INFO"

    # ---- PostgreSQL ----
    # Populated in Phase 3 alongside SQLAlchemy/Alembic setup.
    database_url: str = "postgresql+asyncpg://dtq:change-me@localhost:5432/dtq"

    # ---- RabbitMQ ----
    # Populated in Phase 4 alongside the aio-pika publisher.
    rabbitmq_url: str = "amqp://dtq:change-me@localhost:5672/"

    # ---- Redis ----
    # Populated in Phase 10 alongside the worker registry.
    redis_url: str = "redis://localhost:6379/0"

    # ---- API ----
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: str = "change-me"

    # ---- Worker ----
    worker_concurrency: int = 10
    worker_heartbeat_interval_seconds: int = 5
    worker_heartbeat_ttl_seconds: int = 15
    # Populated in Phase 12 alongside graceful shutdown: how long
    # SIGTERM waits for in-flight handlers to finish on their own
    # before giving up on them and closing connections anyway.
    worker_shutdown_grace_period_seconds: int = 30

    # ---- Priority scheduling (Phase 15) ----
    # Relative weights for scheduling/priority_scheduler.py's
    # Smooth Weighted Round-Robin selector -- see its module
    # docstring for why these are weights (proportional share), not
    # a strict ordering.
    priority_weight_high: int = 4
    priority_weight_normal: int = 2
    priority_weight_low: int = 1
    # How long the dispatch loop sleeps when every priority queue
    # came up empty on the same turn, before trying again -- avoids
    # a tight busy-loop hammering RabbitMQ with empty polls when
    # there's genuinely no work anywhere.
    priority_poll_idle_sleep_seconds: float = 0.05

    # ---- Rate limiting and backpressure ----
    # Populated in Phase 14. Fixed-window counter in Redis: at most
    # rate_limit_requests_per_window POST /tasks calls per
    # rate_limit_window_seconds, system-wide (see
    # coordination/rate_limiter.py for why "system-wide" rather than
    # per-client -- this project has no per-caller identity yet).
    rate_limit_requests_per_window: int = 100
    rate_limit_window_seconds: int = 1
    # Admission control on top of the rate limiter: reject new task
    # submissions outright once the combined depth of the three
    # priority queues passes this, rather than letting an
    # already-overwhelmed broker's backlog grow without bound. See
    # messaging/backpressure.py.
    backpressure_max_queue_depth: int = 10000

    # ---- Scheduler (Phase 16) ----
    # How often services/scheduler/main.py polls Postgres for PENDING
    # tasks whose scheduled_at has arrived, and how many it claims
    # per poll. See docs/scheduling.md.
    scheduler_poll_interval_seconds: float = 1.0
    scheduler_batch_size: int = 50

    # ---- Outbox relay (Phase 17) ----
    # How often services/outbox_relay/main.py polls Postgres for
    # unpublished outbox_messages rows, and how many it relays to
    # RabbitMQ per poll. See docs/outbox.md.
    outbox_relay_poll_interval_seconds: float = 0.5
    outbox_relay_batch_size: int = 100

    # ---- Retries ----
    # See domain/retry_policy.py for how these combine.
    retry_base_delay_seconds: float = 1.0
    retry_max_delay_seconds: float = 60.0
    retry_jitter_fraction: float = 0.2


@lru_cache
def get_settings() -> Settings:
    """
    Settings is cached (constructed once per process) rather than
    re-read on every access. Re-parsing the environment on every call
    would be wasted work for values that never change during a
    process's lifetime, and would make it easy to accidentally read
    a stale/inconsistent .env mid-request if the file changed on disk.
    """
    return Settings()
