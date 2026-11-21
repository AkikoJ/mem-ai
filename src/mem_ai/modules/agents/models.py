"""SQLAlchemy ORM models for `agents` and `persona_versions`."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDPrimaryKeyMixin

PERSONA_VERSION_UNIQUE_CONSTRAINT = "uq_persona_versions_agent_id_version"
AGENT_CURRENT_PUBLISHED_VERSION_FK = "fk_agents_current_published_version_id_persona_versions"


class Agent(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Owner-scoped private Agent holding the sole mutable draft and current published pointer."""

    __tablename__ = "agents"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'retired')", name="status_valid"),
        CheckConstraint("latest_version_number >= 0", name="latest_version_non_negative"),
        CheckConstraint("revision >= 1", name="revision_positive"),
        ForeignKeyConstraint(
            ["current_published_version_id", "id"],
            ["persona_versions.id", "persona_versions.agent_id"],
            ondelete="RESTRICT",
            use_alter=True,
            name=AGENT_CURRENT_PUBLISHED_VERSION_FK,
        ),
        Index("ix_agents_owner_id_created_at_id", "owner_id", "created_at", "id"),
        Index("ix_agents_owner_id_status", "owner_id", "status"),
    )

    owner_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default="active",
    )

    # Single mutable draft stored directly on the agents row.
    draft_system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    draft_behavior_rules: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    draft_tone: Mapped[str | None] = mapped_column(String(64), nullable=True, default=None)
    draft_language: Mapped[str | None] = mapped_column(String(32), nullable=True, default=None)
    draft_default_model_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        default=None,
    )
    draft_memory_policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    current_published_version_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=True,
        default=None,
    )
    latest_version_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    revision: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    retired_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )


class PersonaVersion(UUIDPrimaryKeyMixin, CreatedAtMixin, Base):
    """Immutable persona version snapshot; content columns never mutate once created."""

    __tablename__ = "persona_versions"
    __table_args__ = (
        UniqueConstraint("agent_id", "version"),
        UniqueConstraint("id", "agent_id"),
        CheckConstraint("version >= 1", name="version_positive"),
    )

    agent_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("agents.id", ondelete="RESTRICT"),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    behavior_rules: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    tone: Mapped[str | None] = mapped_column(String(64), nullable=True, default=None)
    language: Mapped[str | None] = mapped_column(String(32), nullable=True, default=None)
    default_model_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        default=None,
    )
    memory_policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
