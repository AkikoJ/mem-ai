"""SQLAlchemy ORM models for `memory_scopes` and `memory_list_snapshots`."""

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
