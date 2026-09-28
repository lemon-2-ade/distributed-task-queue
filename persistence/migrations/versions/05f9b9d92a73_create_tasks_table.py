"""create tasks table

Revision ID: 05f9b9d92a73
Revises:
Create Date: 2026-09-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "05f9b9d92a73"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tasks",
        sa.Column("task_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_type", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("priority", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", sa.String(length=64), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("max_retries", sa.Integer(), nullable=False),
        sa.Column("timeout", sa.Integer(), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_tasks_idempotency_key"),
    )
    op.create_index("ix_tasks_status_priority_scheduled_at", "tasks", ["status", "priority", "scheduled_at"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_task_type", "tasks", ["task_type"])


def downgrade() -> None:
    op.drop_index("ix_tasks_task_type", table_name="tasks")
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_index("ix_tasks_status_priority_scheduled_at", table_name="tasks")
    op.drop_table("tasks")
