"""add idempotency_records table for user/method/route/key-scoped chat idempotency

Revision ID: 20261123_0009
Revises: 20261123_0008
Create Date: 2026-11-23 13:50:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261123_0009"
down_revision: str | Sequence[str] | None = "20261123_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `idempotency_records` with scope uniqueness and terminal-expiry invariants."""
    op.create_table(
        "idempotency_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("method", sa.String(length=16), nullable=False),
        sa.Column("canonical_route", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="running", nullable=False),
        sa.Column("response_status_code", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=500), nullable=True),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
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
            "status IN ('running', 'succeeded', 'failed', 'cancelled')",
            name=op.f("ck_idempotency_records_status_valid"),
        ),
        sa.CheckConstraint(
            "(status = 'running' AND terminal_at IS NULL AND expires_at IS NULL) "
            "OR (status <> 'running' AND terminal_at IS NOT NULL "
            "AND expires_at IS NOT NULL AND expires_at >= terminal_at)",
            name=op.f("ck_idempotency_records_terminal_expiry_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_idempotency_records_conversation_id_conversations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["generations.id"],
            name=op.f("fk_idempotency_records_generation_id_generations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_idempotency_records_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_idempotency_records")),
        sa.UniqueConstraint(
            "user_id",
            "method",
            "canonical_route",
            "idempotency_key",
            name="uq_idempotency_records_scope",
        ),
        sa.UniqueConstraint(
            "generation_id",
            name="uq_idempotency_records_generation_id",
        ),
    )
    op.create_index(
        "ix_idempotency_records_conversation_id",
        "idempotency_records",
        ["conversation_id"],
        unique=False,
    )
    op.create_index(
        "ix_idempotency_records_user_id",
        "idempotency_records",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_idempotency_records_expires_at",
        "idempotency_records",
        ["expires_at"],
        unique=False,
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )


def downgrade() -> None:
    """Drop `idempotency_records` table and indexes."""
    op.drop_index(
        "ix_idempotency_records_expires_at",
        table_name="idempotency_records",
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )
    op.drop_index("ix_idempotency_records_user_id", table_name="idempotency_records")
    op.drop_index("ix_idempotency_records_conversation_id", table_name="idempotency_records")
    op.drop_table("idempotency_records")
