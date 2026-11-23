"""SQLAlchemy ORM models for `memory_scopes`, `memory_tombstones`, and `memory_list_snapshots`."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDPrimaryKeyMixin

MEMORY_SCOPE_BARRIER_STATES = ("idle", "clearing", "reconciling")
MEMORY_SCOPE_UNIQUE_CONSTRAINT = "uq_memory_scopes_user_id_provider_id"
MEMORY_TOMBSTONE_STATUSES = ("pending", "reconciling", "verified")
MEMORY_TOMBSTONE_TARGET_KINDS = ("memory", "conversation", "user")
MEMORY_TOMBSTONE_USER_MEMORY_UNIQUE_INDEX = "uq_memory_tombstones_user_provider_memory"
MEMORY_TOMBSTONE_USER_CONVERSATION_UNIQUE_INDEX = "uq_memory_tombstones_user_provider_conversation"


class MemoryScopeState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Per-(user, provider) monotonic memory epoch and deletion barrier state."""

    __tablename__ = "memory_scopes"
    __table_args__ = (
        UniqueConstraint("user_id", "provider_id", name=MEMORY_SCOPE_UNIQUE_CONSTRAINT),
        CheckConstraint("epoch >= 0", name="epoch_non_negative"),
        CheckConstraint(
            "barrier_state IN ('idle', 'clearing', 'reconciling')",
            name="barrier_state_valid",
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_id: Mapped[str] = mapped_column(String(64), nullable=False)
    epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    barrier_state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="idle",
        server_default="idle",
    )
    barrier_operation_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("operations.id", ondelete="SET NULL"),
        nullable=True,
    )


class MemoryListSnapshot(UUIDPrimaryKeyMixin, CreatedAtMixin, Base):
    """PostgreSQL-backed bounded pagination snapshot for page-based memory providers."""

    __tablename__ = "memory_list_snapshots"
    __table_args__ = (
        CheckConstraint("total_items >= 0", name="total_items_non_negative"),
        Index(
            "ix_memory_list_snapshots_user_provider_filters",
            "user_id",
            "provider_id",
            "filters_hash",
        ),
        Index("ix_memory_list_snapshots_expires_at", "expires_at"),
    )

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    provider_id: Mapped[str] = mapped_column(String(64), nullable=False)
    filters_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    items_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    total_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MemoryTombstone(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Per-user/provider deletion marker blocking recall and tracking external verification."""

    __tablename__ = "memory_tombstones"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'reconciling', 'verified')",
            name="status_valid",
        ),
        CheckConstraint(
            "target_kind IN ('memory', 'conversation', 'user')",
            name="target_kind_valid",
        ),
        CheckConstraint("memory_epoch >= 0", name="memory_epoch_non_negative"),
        CheckConstraint(
            "(target_kind = 'memory' AND memory_id IS NOT NULL AND conversation_id IS NULL) "
            "OR (target_kind = 'conversation' AND conversation_id IS NOT NULL "
            "AND memory_id IS NULL) "
            "OR (target_kind = 'user' AND memory_id IS NULL AND conversation_id IS NULL)",
            name="target_binding_valid",
        ),
        Index(
            "ix_memory_tombstones_user_provider_kind_status",
            "user_id",
            "provider_id",
            "target_kind",
            "status",
        ),
        Index(
            MEMORY_TOMBSTONE_USER_MEMORY_UNIQUE_INDEX,
            "user_id",
            "provider_id",
            "memory_id",
            unique=True,
            postgresql_where=text("target_kind = 'memory'"),
        ),
        Index(
            MEMORY_TOMBSTONE_USER_CONVERSATION_UNIQUE_INDEX,
            "user_id",
            "provider_id",
            "conversation_id",
            unique=True,
            postgresql_where=text("target_kind = 'conversation'"),
        ),
        Index("ix_memory_tombstones_operation_id", "operation_id"),
    )

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_id: Mapped[str] = mapped_column(String(64), nullable=False)
    target_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    memory_id: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    conversation_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=True,
        default=None,
    )
    memory_epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    operation_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("operations.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        server_default="pending",
    )
    external_ref: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
