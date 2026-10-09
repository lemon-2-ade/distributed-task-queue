# Phase 16: scheduler service image. Same one-Dockerfile-per-service
# rationale as docker/api.Dockerfile and docker/worker.Dockerfile.
#
# Phase 17: this image copies neither `messaging` nor `coordination`
# -- the scheduler claims due tasks and writes outbox rows, entirely
# within Postgres (see services/scheduler/dispatcher.py); it never
# touches RabbitMQ or Redis at all. services/outbox_relay/'s own
# Dockerfile is the one that needs `messaging`.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY README.md ./
COPY config.py ./
COPY alembic.ini ./
COPY domain ./domain
COPY persistence ./persistence
COPY observability ./observability
COPY services/scheduler ./services/scheduler
COPY services/__init__.py ./services/__init__.py

RUN pip install --no-cache-dir .

# Phase 23: run as a dedicated non-root user rather than the
# container default (root) -- see docs/security.md for the full
# reasoning. --no-create-home/--shell nologin because this user only
# ever needs to own and run these files, never log in or have a home
# directory of its own.
RUN groupadd --system dtq \
    && useradd --system --gid dtq --no-create-home --shell /usr/sbin/nologin dtq \
    && chown -R dtq:dtq /app
USER dtq

CMD ["python", "-m", "services.scheduler.main"]
