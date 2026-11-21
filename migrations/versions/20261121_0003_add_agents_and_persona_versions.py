"""add agents and persona_versions tables

Revision ID: 20261121_0003
Revises: 20261121_0002
Create Date: 2026-11-21 18:18:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261121_0003"
down_revision: str | Sequence[str] | None = "20261121_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `agents` and `persona_versions` tables with constraints and indexes."""
    op.create_table(
        "agents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="active", nullable=False),
        sa.Column("draft_system_prompt", sa.Text(), nullable=False),
        sa.Column(
            "draft_behavior_rules",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("draft_tone", sa.String(length=64), nullable=True),
        sa.Column("draft_language", sa.String(length=32), nullable=True),
        sa.Column("draft_default_model_id", sa.String(length=128), nullable=True),
        sa.Column(
            "draft_memory_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("current_published_version_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "latest_version_number",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "revision",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
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
            "status IN ('active', 'retired')",
            name=op.f("ck_agents_status_valid"),
        ),
        sa.CheckConstraint(
            "latest_version_number >= 0",
            name=op.f("ck_agents_latest_version_non_negative"),
        ),
        sa.CheckConstraint(
            "revision >= 1",
            name=op.f("ck_agents_revision_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_agents_owner_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agents")),
    )
    op.create_index(
        "ix_agents_owner_id_created_at_id",
        "agents",
        ["owner_id", "created_at", "id"],
        unique=False,
    )
    op.create_index("ix_agents_owner_id_status", "agents", ["owner_id", "status"], unique=False)

    op.create_table(
        "persona_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("system_prompt", sa.Text(), nullable=False),
        sa.Column(
            "behavior_rules",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("tone", sa.String(length=64), nullable=True),
        sa.Column("language", sa.String(length=32), nullable=True),
        sa.Column("default_model_id", sa.String(length=128), nullable=True),
        sa.Column(
            "memory_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "version >= 1",
            name=op.f("ck_persona_versions_version_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            ["agents.id"],
            name=op.f("fk_persona_versions_agent_id_agents"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_persona_versions")),
        sa.UniqueConstraint(
            "agent_id",
            "version",
            name=op.f("uq_persona_versions_agent_id_version"),
        ),
        sa.UniqueConstraint(
            "id",
            "agent_id",
            name=op.f("uq_persona_versions_id_agent_id"),
        ),
    )

    op.create_foreign_key(
        op.f("fk_agents_current_published_version_id_persona_versions"),
        "agents",
        "persona_versions",
        ["current_published_version_id", "id"],
        ["id", "agent_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Drop `persona_versions` and `agents` tables cleanly."""
    op.drop_constraint(
        op.f("fk_agents_current_published_version_id_persona_versions"),
        "agents",
        type_="foreignkey",
    )
    op.drop_table("persona_versions")
    op.drop_index("ix_agents_owner_id_status", table_name="agents")
    op.drop_index("ix_agents_owner_id_created_at_id", table_name="agents")
    op.drop_table("agents")
