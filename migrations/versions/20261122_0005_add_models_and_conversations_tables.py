"""add model_configs, conversations, generations, and messages tables

Revision ID: 20261122_0005
Revises: 20261121_0004
Create Date: 2026-11-22 10:55:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261122_0005"
down_revision: str | Sequence[str] | None = "20261121_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `model_configs`, `conversations`, `generations`, and `messages` tables."""
    op.create_table(
        "model_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("model_id", sa.String(length=128), nullable=False),
        sa.Column(
            "provider_id",
            sa.String(length=64),
            server_default="openai_compatible",
            nullable=False,
        ),
        sa.Column("upstream_model", sa.String(length=128), nullable=False),
        sa.Column("display_name", sa.String(length=128), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "supports_streaming",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("max_context_tokens", sa.Integer(), nullable=True),
        sa.Column("max_output_tokens", sa.Integer(), nullable=True),
        sa.Column("required_permission", sa.String(length=128), nullable=True),
        sa.Column("credential_ref", sa.String(length=128), nullable=True),
        sa.Column(
            "capabilities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
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
        sa.CheckConstraint("model_id <> ''", name=op.f("ck_model_configs_model_id_non_empty")),
        sa.CheckConstraint(
            "provider_id <> ''",
            name=op.f("ck_model_configs_provider_id_non_empty"),
        ),
        sa.CheckConstraint(
            "upstream_model <> ''",
            name=op.f("ck_model_configs_upstream_model_non_empty"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_configs")),
        sa.UniqueConstraint("model_id", name=op.f("uq_model_configs_model_id")),
    )
    op.create_index(
        "ix_model_configs_enabled_model_id",
        "model_configs",
        ["enabled", "model_id"],
        unique=False,
    )

    op.create_table(
        "conversations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("persona_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("persona_version_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="active", nullable=False),
        sa.Column(
            "next_message_sequence",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
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
            "status IN ('active', 'deleting')",
            name=op.f("ck_conversations_status_valid"),
        ),
        sa.CheckConstraint(
            "persona_version_number >= 1",
            name=op.f("ck_conversations_persona_version_number_positive"),
        ),
        sa.CheckConstraint(
            "next_message_sequence >= 1",
            name=op.f("ck_conversations_next_message_sequence_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_conversations_owner_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_conversations")),
    )
    op.create_index(
        "ix_conversations_owner_id_created_at_id",
        "conversations",
        ["owner_id", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_conversations_owner_id_status",
        "conversations",
        ["owner_id", "status"],
        unique=False,
    )

    op.create_table(
        "generations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("persona_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_message_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("assistant_message_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("model_id", sa.String(length=128), nullable=False),
        sa.Column("memory_epoch", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="running", nullable=False),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=500), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.Integer(), nullable=True),
        sa.Column("finish_reason", sa.String(length=64), nullable=True),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
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
            name=op.f("ck_generations_status_valid"),
        ),
        sa.CheckConstraint(
            "memory_epoch >= 0",
            name=op.f("ck_generations_memory_epoch_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_generations_conversation_id_conversations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_generations_owner_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_generations")),
        sa.UniqueConstraint("user_message_id", name=op.f("uq_generations_user_message_id")),
        sa.UniqueConstraint(
            "assistant_message_id",
            name=op.f("uq_generations_assistant_message_id"),
        ),
    )
    op.create_index(
        "ix_generations_conversation_id_created_at_id",
        "generations",
        ["conversation_id", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_generations_conversation_id_status",
        "generations",
        ["conversation_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_generations_owner_id_created_at_id",
        "generations",
        ["owner_id", "created_at", "id"],
        unique=False,
    )

    op.create_table(
        "messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('user', 'assistant')",
            name=op.f("ck_messages_role_valid"),
        ),
        sa.CheckConstraint("sequence >= 1", name=op.f("ck_messages_sequence_positive")),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_messages_conversation_id_conversations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["generations.id"],
            name=op.f("fk_messages_generation_id_generations"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_messages")),
        sa.UniqueConstraint(
            "conversation_id",
            "sequence",
            name=op.f("uq_messages_conversation_id_sequence"),
        ),
        sa.UniqueConstraint(
            "generation_id",
            "role",
            name=op.f("uq_messages_generation_id_role"),
        ),
    )
    op.create_index(
        "ix_messages_conversation_id_sequence",
        "messages",
        ["conversation_id", "sequence"],
        unique=False,
    )
    op.create_index("ix_messages_generation_id", "messages", ["generation_id"], unique=False)

    op.create_foreign_key(
        "fk_generations_user_message_id_messages",
        "generations",
        "messages",
        ["user_message_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_generations_assistant_message_id_messages",
        "generations",
        "messages",
        ["assistant_message_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Drop `messages`, `generations`, `conversations`, and `model_configs` tables."""
    op.drop_constraint(
        "fk_generations_assistant_message_id_messages",
        "generations",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_generations_user_message_id_messages",
        "generations",
        type_="foreignkey",
    )

    op.drop_index("ix_messages_generation_id", table_name="messages")
    op.drop_index("ix_messages_conversation_id_sequence", table_name="messages")
    op.drop_table("messages")

    op.drop_index("ix_generations_owner_id_created_at_id", table_name="generations")
    op.drop_index("ix_generations_conversation_id_status", table_name="generations")
    op.drop_index("ix_generations_conversation_id_created_at_id", table_name="generations")
    op.drop_table("generations")

    op.drop_index("ix_conversations_owner_id_status", table_name="conversations")
    op.drop_index("ix_conversations_owner_id_created_at_id", table_name="conversations")
    op.drop_table("conversations")

    op.drop_index("ix_model_configs_enabled_model_id", table_name="model_configs")
    op.drop_table("model_configs")
