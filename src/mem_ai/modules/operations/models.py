"""SQLAlchemy ORM model for `operations` tracking async memory and deletion lifecycles."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

OPERATION_STATUSES = ("pending", "running", "reconciling", "succeeded", "failed")


class Operation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Queryable operation progress record for async memory creation and deletion flows."""

    __tablename__ = "operations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'reconciling', 'succeeded', 'failed')",
            name="status_valid",
        ),
        CheckConstraint(
            "deletion_receipt_digest IS NULL OR "
            "(operation_type = 'user_delete' AND expires_at IS NOT NULL "
            "AND length(deletion_receipt_digest) = 64)",
            name="deletion_receipt_binding_valid",
        ),
        Index("ix_operations_owner_id_created_at", "owner_id", "created_at"),
        Index("ix_operations_status_updated_at", "status", "updated_at"),
        Index(
            "uq_operations_user_delete_owner",
            "owner_id",
            unique=True,
            postgresql_where=text("operation_type = 'user_delete' AND owner_id IS NOT NULL"),
        ),
    )

    owner_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        server_default="pending",
    )
    target_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    job_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="SET NULL"),
        nullable=True,
    )
    progress: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    result_summary: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(500), nullable=True)
    deletion_receipt_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deletion_receipt_revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
