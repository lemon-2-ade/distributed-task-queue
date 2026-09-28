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
