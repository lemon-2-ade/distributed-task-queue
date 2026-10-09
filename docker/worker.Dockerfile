# Phase 5: worker service image. Same rationale as docker/api.Dockerfile
# for keeping one Dockerfile per service.
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
COPY coordination ./coordination
COPY scheduling ./scheduling
COPY observability ./observability
COPY task_handlers ./task_handlers
COPY services/worker ./services/worker
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

CMD ["python", "-m", "services.worker.main"]
