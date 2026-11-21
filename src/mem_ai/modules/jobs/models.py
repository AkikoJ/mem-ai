"""SQLAlchemy ORM model for the unified PostgreSQL `jobs` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

JOB_STATUSES = (
    "pending",
    "running",
    "retry_wait",
    "reconciling",
    "succeeded",
    "failed",
    "cancelled",
)
JOB_DEDUPE_KEY_UNIQUE_CONSTRAINT = "uq_jobs_dedupe_key"


class Job(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Unified durable job row shared by explicit writes, turn writes, deletions, and retention."""

    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'reconciling', "
            "'succeeded', 'failed', 'cancelled')",
            name="status_valid",
        ),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        Index("ix_jobs_status_next_run_at", "status", "next_run_at"),
        Index("ix_jobs_user_id_job_type_status", "user_id", "job_type", "status"),
        Index("ix_jobs_business_id", "business_id"),
        Index("ix_jobs_external_ref", "external_ref"),
    )

    job_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    user_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    dedupe_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
        server_default="pending",
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=5,
        server_default=text("5"),
    )
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    next_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
