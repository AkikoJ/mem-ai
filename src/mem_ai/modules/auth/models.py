"""SQLAlchemy ORM models for `auth_sessions`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, UUIDPrimaryKeyMixin


class AuthSession(UUIDPrimaryKeyMixin, CreatedAtMixin, Base):
    """
    持久化登录会话，存储可撤销 Refresh Token 的 SHA-256 摘要及轮换家族追踪。

    - token_hash: Refresh Token 的 SHA-256 摘要（十六进制字符串，64 字符）
    - expires_at: 会话过期时间
    - revoked_at: 会话吊销时间（NULL 表示活跃会话）
    - replaced_by_session_id: 轮换时指向新会话，用于检测重放攻击
    - last_used_at: 最近一次使用时间（刷新或验证）
    """

    __tablename__ = "auth_sessions"
    __table_args__ = (
        Index("ix_auth_sessions_token_hash", "token_hash", unique=True),
        Index("ix_auth_sessions_user_id_revoked_at", "user_id", "revoked_at"),
        Index("ix_auth_sessions_expires_at_id", "expires_at", "id"),
        Index(
            "ix_auth_sessions_revoked_at_id",
            "revoked_at",
            "id",
            postgresql_where=text("revoked_at IS NOT NULL"),
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    token_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
    replaced_by_session_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("auth_sessions.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
