"""SQLAlchemy ORM model for `users`."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

USER_EMAIL_UNIQUE_INDEX = "ix_users_email_unique"


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Application-owned local user subject with lifecycle status and built-in credentials."""

    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'disabled', 'deleting')", name="status_valid"),
        CheckConstraint("email <> '' AND email = lower(email)", name="email_lowercase"),
        Index("ix_users_status", "status"),
        Index(USER_EMAIL_UNIQUE_INDEX, "email", unique=True),
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
    # 内置认证凭据：规范化小写唯一邮箱与强哈希密码
    email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    password_hash: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
