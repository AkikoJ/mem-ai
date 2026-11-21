"""SQLAlchemy ORM models for `users` and `user_identities`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDPrimaryKeyMixin

USER_IDENTITY_UNIQUE_CONSTRAINT = "uq_user_identities_issuer_subject"


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Application-owned local user subject with lifecycle status."""

    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'disabled', 'deleting')", name="status_valid"),
        Index("ix_users_status", "status"),
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default="active",
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )


class UserIdentity(UUIDPrimaryKeyMixin, CreatedAtMixin, Base):
    """Unique mapping from a verified external `(issuer, subject)` to a local user."""

    __tablename__ = "user_identities"
    __table_args__ = (
        UniqueConstraint("issuer", "subject"),
        Index("ix_user_identities_user_id", "user_id"),
    )

    issuer: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[str] = mapped_column(String(255), nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
