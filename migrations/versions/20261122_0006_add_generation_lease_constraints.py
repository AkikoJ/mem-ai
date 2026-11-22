"""add generation active-lease uniqueness, expiry index, and lease state constraint

Revision ID: 20261122_0006
Revises: 20261122_0005
Create Date: 2026-11-22 13:00:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20261122_0006"
down_revision: str | Sequence[str] | None = "20261122_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Enforce single active lease per conversation and index expired running leases."""
    op.create_check_constraint(
        op.f("ck_generations_lease_state_valid"),
        "generations",
        "(status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
        "OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL)",
    )
    op.create_index(
        "uq_generations_active_conversation",
        "generations",
        ["conversation_id"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_generations_running_lease_expires_at",
        "generations",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    """Remove generation lease uniqueness, expiry index, and lease state constraint."""
    op.drop_index(
        "ix_generations_running_lease_expires_at",
        table_name="generations",
        postgresql_where=sa.text("status = 'running'"),
    )
    op.drop_index(
        "uq_generations_active_conversation",
        table_name="generations",
        postgresql_where=sa.text("status = 'running'"),
    )
    op.drop_constraint(
        op.f("ck_generations_lease_state_valid"),
        "generations",
        type_="check",
    )
