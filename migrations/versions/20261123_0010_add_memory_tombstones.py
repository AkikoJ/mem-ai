"""add memory_tombstones table for deletion recall filtering and verification tracking

Revision ID: 20261123_0010
Revises: 20261123_0009
Create Date: 2026-11-23 16:20:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261123_0010"
down_revision: str | Sequence[str] | None = "20261123_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `memory_tombstones` table with target invariants and partial unique indexes."""
    op.create_table(
        "memory_tombstones",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", sa.String(length=64), nullable=False),
        sa.Column("target_kind", sa.String(length=32), nullable=False),
        sa.Column("memory_id", sa.String(length=255), nullable=True),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("memory_epoch", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("external_ref", sa.String(length=255), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'reconciling', 'verified')",
            name=op.f("ck_memory_tombstones_status_valid"),
        ),
        sa.CheckConstraint(
            "target_kind IN ('memory', 'conversation', 'user')",
            name=op.f("ck_memory_tombstones_target_kind_valid"),
        ),
        sa.CheckConstraint(
            "memory_epoch >= 0",
            name=op.f("ck_memory_tombstones_memory_epoch_non_negative"),
        ),
        sa.CheckConstraint(
            "(target_kind = 'memory' AND memory_id IS NOT NULL AND conversation_id IS NULL) "
            "OR (target_kind = 'conversation' AND conversation_id IS NOT NULL "
            "AND memory_id IS NULL) "
            "OR (target_kind = 'user' AND memory_id IS NULL AND conversation_id IS NULL)",
            name=op.f("ck_memory_tombstones_target_binding_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["operations.id"],
            name=op.f("fk_memory_tombstones_operation_id_operations"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_memory_tombstones_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_memory_tombstones")),
    )
    op.create_index(
        "ix_memory_tombstones_user_provider_kind_status",
        "memory_tombstones",
        ["user_id", "provider_id", "target_kind", "status"],
        unique=False,
    )
    op.create_index(
        "uq_memory_tombstones_user_provider_memory",
        "memory_tombstones",
        ["user_id", "provider_id", "memory_id"],
        unique=True,
        postgresql_where=sa.text("target_kind = 'memory'"),
    )
    op.create_index(
        "uq_memory_tombstones_user_provider_conversation",
        "memory_tombstones",
        ["user_id", "provider_id", "conversation_id"],
        unique=True,
        postgresql_where=sa.text("target_kind = 'conversation'"),
    )
    op.create_index(
        "ix_memory_tombstones_operation_id",
        "memory_tombstones",
        ["operation_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop `memory_tombstones` table and indexes."""
    op.drop_index("ix_memory_tombstones_operation_id", table_name="memory_tombstones")
    op.drop_index(
        "uq_memory_tombstones_user_provider_conversation",
        table_name="memory_tombstones",
        postgresql_where=sa.text("target_kind = 'conversation'"),
    )
    op.drop_index(
        "uq_memory_tombstones_user_provider_memory",
        table_name="memory_tombstones",
        postgresql_where=sa.text("target_kind = 'memory'"),
    )
    op.drop_index(
        "ix_memory_tombstones_user_provider_kind_status",
        table_name="memory_tombstones",
    )
    op.drop_table("memory_tombstones")
