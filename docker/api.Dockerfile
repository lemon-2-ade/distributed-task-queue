# Phase 2: API service image.
# One Dockerfile per service (docker/api.Dockerfile, later
# docker/worker.Dockerfile, docker/scheduler.Dockerfile) rather than
# one shared Dockerfile with a runtime ENTRYPOINT switch, so each
# service's image only contains what it actually needs and each can
# evolve (e.g. different base image, different extra deps) without
# touching the others.
FROM python:3.12-slim

WORKDIR /app

# System deps for building asyncpg/etc wheels if no manylinux wheel
# is available for the target platform.
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
COPY coordination ./coordination
COPY observability ./observability
COPY services/api ./services/api
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

EXPOSE 8000

CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
