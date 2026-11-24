"""add operations retention scan indexes on expires_at and completed_at

Revision ID: 20261124_0012
Revises: 20261123_0011
Create Date: 2026-11-24 11:15:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20261124_0012"
down_revision: str | Sequence[str] | None = "20261123_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Support cursor-paginated retention scans over expired receipts and terminal summaries."""
    op.create_index(
        "ix_operations_expires_at_id",
        "operations",
        ["expires_at", "id"],
        unique=False,
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )
    op.create_index(
        "ix_operations_completed_at_id",
        "operations",
        ["completed_at", "id"],
        unique=False,
        postgresql_where=sa.text("completed_at IS NOT NULL"),
    )


def downgrade() -> None:
    """Drop operations retention scan indexes."""
    op.drop_index("ix_operations_completed_at_id", table_name="operations")
    op.drop_index("ix_operations_expires_at_id", table_name="operations")
