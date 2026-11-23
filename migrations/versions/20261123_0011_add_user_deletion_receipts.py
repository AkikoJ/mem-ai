"""add protected operation-scoped user deletion receipts

Revision ID: 20261123_0011
Revises: 20261123_0010
Create Date: 2026-11-23 18:00:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20261123_0011"
down_revision: str | Sequence[str] | None = "20261123_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Retain only protected receipt material and one deletion per bound user."""
    op.add_column(
        "operations", sa.Column("deletion_receipt_digest", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "operations",
        sa.Column("deletion_receipt_revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_operations_deletion_receipt_binding_valid"),
        "operations",
        "deletion_receipt_digest IS NULL OR "
        "(operation_type = 'user_delete' AND expires_at IS NOT NULL "
        "AND length(deletion_receipt_digest) = 64)",
    )
    op.create_index(
        "uq_operations_user_delete_owner",
        "operations",
        ["owner_id"],
        unique=True,
        postgresql_where=sa.text("operation_type = 'user_delete' AND owner_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove deletion receipt binding while preserving existing operation records."""
    op.drop_index("uq_operations_user_delete_owner", table_name="operations")
    op.drop_constraint(
        op.f("ck_operations_deletion_receipt_binding_valid"), "operations", type_="check"
    )
    op.drop_column("operations", "deletion_receipt_revoked_at")
    op.drop_column("operations", "deletion_receipt_digest")
