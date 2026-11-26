"""PostgreSQL repository for `users`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.users.models import User
from mem_ai.modules.users.schemas import UserStatus


async def get_user_by_id(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_share: bool = False,
    for_update: bool = False,
) -> User | None:
    """Fetch a local user row with an optional shared or exclusive transaction lock."""
    stmt = select(User).where(User.id == user_id)
    if for_update:
        stmt = stmt.with_for_update()
    elif for_share:
        stmt = stmt.with_for_update(read=True)
    return await session.scalar(stmt)


async def update_user_status(
    session: AsyncSession,
    *,
    user_id: UUID,
    status: UserStatus,
    deleted_at: datetime | None = None,
) -> User | None:
    """Update a user's lifecycle status within the caller's session."""
    user = await get_user_by_id(session, user_id=user_id, for_update=True)
    if user is None:
        return None
    user.status = status
    if deleted_at is not None:
        user.deleted_at = deleted_at
    await session.flush()
    await session.refresh(user)
    return user


async def delete_deleting_user(session: AsyncSession, *, user_id: UUID) -> None:
    """Remove the deleting root last, after resource owners have confirmed cleanup."""
    removed = await session.scalar(
        delete(User).where(User.id == user_id, User.status == "deleting").returning(User.id)
    )
    if removed is None:
        raise RuntimeError("user cleanup lost its deleting root")


async def get_user_by_email(
    session: AsyncSession,
    *,
    email: str,
    for_update: bool = False,
) -> User | None:
    """根据邮箱查找用户。"""
    stmt = select(User).where(User.email == email)
    if for_update:
        stmt = stmt.with_for_update()
    return await session.scalar(stmt)


async def create_user_with_email(
    session: AsyncSession,
    *,
    email: str,
    password_hash: str,
    status: UserStatus = "active",
) -> User:
    """创建带邮箱密码的新用户。"""
    user = User(
        status=status,
        email=email,
        password_hash=password_hash,
    )
    session.add(user)
    await session.flush()
    await session.refresh(user)
    return user


async def update_user_password(
    session: AsyncSession,
    *,
    user_id: UUID,
    password_hash: str,
) -> User | None:
    """更新用户密码哈希。"""
    user = await get_user_by_id(session, user_id=user_id, for_update=True)
    if user is None:
        return None
    user.password_hash = password_hash
    await session.flush()
    await session.refresh(user)
    return user
