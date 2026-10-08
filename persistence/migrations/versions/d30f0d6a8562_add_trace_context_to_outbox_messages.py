"""add trace_context to outbox_messages

Phase 19 (distributed tracing): a nullable JSONB column holding the
W3C traceparent/tracestate pair captured when the row was written,
so the outbox relay and worker can continue the same trace across the
Postgres row that otherwise carries no live call-stack context. See
observability/tracing.py and persistence/models.py's OutboxMessage
for the full rationale.

Revision ID: d30f0d6a8562
Revises: 682eb72badfb
Create Date: 2026-10-08 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd30f0d6a8562'
down_revision: Union[str, Sequence[str], None] = '682eb72badfb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'outbox_messages',
        sa.Column('trace_context', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('outbox_messages', 'trace_context')
