"""add built-in auth: users email/password_hash and auth_sessions table

Revision ID: 20261126_0013
Revises: 20261124_0012
Create Date: 2026-11-26 11:07:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261126_0013"
down_revision: str | Sequence[str] | None = "20261124_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """
    扩展本地用户凭据与登录会话：
    1. 在 users 表新增 email 与 password_hash 列（兼容历史行升级）
    2. 创建 auth_sessions 持久化登录会话表及索引
    """
    # 1. 在 users 表添加 email 列（规范化小写存储，唯一约束，可空以兼容历史行）
    op.add_column(
        "users",
        sa.Column("email", sa.String(length=255), nullable=True),
    )

    # 2. 在 users 表添加 password_hash 列（可空以兼容历史行）
    op.add_column(
        "users",
        sa.Column("password_hash", sa.String(length=255), nullable=True),
    )

    # 3. 创建 email 的部分唯一索引（仅对非空值唯一）
    # 使用部分索引确保大小写不敏感的唯一性：存储时已规范化为小写
    op.create_index(
        "ix_users_email_unique",
        "users",
        ["email"],
        unique=True,
        postgresql_where=sa.text("email IS NOT NULL"),
    )

    # 3b. 数据库级大小写规范化约束：邮箱必须以小写形式保存
    op.create_check_constraint(
        op.f("ck_users_email_lowercase"),
        "users",
        "email IS NULL OR email = lower(email)",
    )

    # 4. 创建 auth_sessions 表
    op.create_table(
        "auth_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        # token_hash 存储 Refresh Token 的 SHA-256 摘要（十六进制字符串，64 字符）
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        # replaced_by_session_id 用于追踪令牌轮换家族与重放检测
        sa.Column("replaced_by_session_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_auth_sessions_user_id_users"),
            ondelete="CASCADE",  # 用户删除时级联清理登录会话
        ),
        sa.ForeignKeyConstraint(
            ["replaced_by_session_id"],
            ["auth_sessions.id"],
            name=op.f("fk_auth_sessions_replaced_by_session_id_auth_sessions"),
            ondelete="SET NULL",  # 新会话删除时旧会话的 replaced_by 置空
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_auth_sessions")),
    )

    # 5. token_hash 唯一索引：用于快速验证 Refresh Token
    op.create_index(
        "ix_auth_sessions_token_hash",
        "auth_sessions",
        ["token_hash"],
        unique=True,
    )

    # 6. (user_id, revoked_at) 复合索引：用于查询用户的活跃会话
    op.create_index(
        "ix_auth_sessions_user_id_revoked_at",
        "auth_sessions",
        ["user_id", "revoked_at"],
        unique=False,
    )

    # 7. (expires_at, id) 复合索引：用于保留清理按过期时间游标扫描
    op.create_index(
        "ix_auth_sessions_expires_at_id",
        "auth_sessions",
        ["expires_at", "id"],
        unique=False,
    )

    # 8. (revoked_at, id) 部分索引：用于已吊销会话的保留期扫描
    op.create_index(
        "ix_auth_sessions_revoked_at_id",
        "auth_sessions",
        ["revoked_at", "id"],
        unique=False,
        postgresql_where=sa.text("revoked_at IS NOT NULL"),
    )


def downgrade() -> None:
    """
    回滚内置认证扩展：
    1. 删除 auth_sessions 表及其索引
    2. 删除 users 表的 email 和 password_hash 列
    """
    # 删除 auth_sessions 表（索引会自动删除）
    op.drop_index("ix_auth_sessions_revoked_at_id", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_expires_at_id", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_user_id_revoked_at", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_token_hash", table_name="auth_sessions")
    op.drop_table("auth_sessions")

    # 删除 users 表的新增列
    op.drop_constraint(op.f("ck_users_email_lowercase"), "users", type_="check")
    op.drop_index("ix_users_email_unique", table_name="users")
    op.drop_column("users", "password_hash")
    op.drop_column("users", "email")
