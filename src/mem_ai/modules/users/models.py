"""SQLAlchemy ORM models for `users` and `user_identities`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDPrimaryKeyMixin

USER_IDENTITY_UNIQUE_CONSTRAINT = "uq_user_identities_issuer_subject"
USER_EMAIL_UNIQUE_INDEX = "ix_users_email_unique"


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Application-owned local user subject with lifecycle status and built-in credentials."""

    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'disabled', 'deleting')", name="status_valid"),
        CheckConstraint("email IS NULL OR email = lower(email)", name="email_lowercase"),
        Index("ix_users_status", "status"),
        Index(
            USER_EMAIL_UNIQUE_INDEX,
            "email",
            unique=True,
            postgresql_where=text("email IS NOT NULL"),
        ),
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
    # 内置认证凭据：规范化小写邮箱与强哈希密码
    email: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,  # 兼容历史行升级
        default=None,
    )
    password_hash: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,  # 兼容历史行升级
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
