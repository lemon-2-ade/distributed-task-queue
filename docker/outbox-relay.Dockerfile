# Phase 17: outbox relay service image. Same one-Dockerfile-per-
# service rationale as the other services' Dockerfiles. This is the
# only one of the four that needs both `persistence` (reads the
# outbox table) and `messaging` (the only process left that publishes
# to RabbitMQ) -- see services/outbox_relay/main.py.
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
COPY messaging ./messaging
COPY persistence ./persistence
COPY observability ./observability
COPY services/outbox_relay ./services/outbox_relay
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

CMD ["python", "-m", "services.outbox_relay.main"]
