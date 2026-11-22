"""add message_recalled_memories metadata reference table

Revision ID: 20261122_0007
Revises: 20261122_0006
Create Date: 2026-11-22 14:40:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261122_0007"
down_revision: str | Sequence[str] | None = "20261122_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create metadata-only `message_recalled_memories` table and indexes."""
    op.create_table(
        "message_recalled_memories",
        sa.Column("user_message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", sa.String(length=64), nullable=False),
        sa.Column("memory_id", sa.String(length=255), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("relevance", sa.Float(), nullable=True),
        sa.Column("prompt_position", sa.Integer(), nullable=False),
        sa.Column("memory_epoch", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "recalled_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope IN ('user', 'conversation')",
            name=op.f("ck_message_recalled_memories_scope_valid"),
        ),
        sa.CheckConstraint(
            "relevance IS NULL OR (relevance >= 0.0 AND relevance <= 1.0)",
            name=op.f("ck_message_recalled_memories_relevance_range"),
        ),
        sa.CheckConstraint(
            "prompt_position >= 1",
            name=op.f("ck_message_recalled_memories_prompt_position_positive"),
        ),
        sa.CheckConstraint(
            "memory_epoch >= 0",
            name=op.f("ck_message_recalled_memories_memory_epoch_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["user_message_id"],
            ["messages.id"],
            name=op.f("fk_message_recalled_memories_user_message_id_messages"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_message_recalled_memories")),
        sa.UniqueConstraint(
            "user_message_id",
            "provider_id",
            "memory_id",
            "scope",
            name="uq_message_recalled_memories_msg_provider_mem_scope",
        ),
    )
    op.create_index(
        "ix_message_recalled_memories_user_message_id_pos",
        "message_recalled_memories",
        ["user_message_id", "prompt_position"],
        unique=False,
    )
    op.create_index(
        "ix_message_recalled_memories_provider_id_memory_id",
        "message_recalled_memories",
        ["provider_id", "memory_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop `message_recalled_memories` table and indexes."""
    op.drop_index(
        "ix_message_recalled_memories_provider_id_memory_id",
        table_name="message_recalled_memories",
    )
    op.drop_index(
        "ix_message_recalled_memories_user_message_id_pos",
        table_name="message_recalled_memories",
    )
    op.drop_table("message_recalled_memories")
