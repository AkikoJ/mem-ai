"""PostgreSQL repository for `users` and `user_identities`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.users.models import User, UserIdentity
from mem_ai.modules.users.schemas import UserStatus


async def get_user_by_id(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_share: bool = False,
) -> User | None:
    """Fetch a local user row by primary key, optionally acquiring a `FOR SHARE` row lock."""
    stmt = select(User).where(User.id == user_id)
    if for_share:
        stmt = stmt.with_for_update(read=True)
    return await session.scalar(stmt)


async def get_user_by_identity(
    session: AsyncSession,
    *,
    issuer: str,
    subject: str,
) -> User | None:
    """Fetch the local user bound to a verified `(issuer, subject)` identity."""
    stmt = (
        select(User)
        .join(UserIdentity, UserIdentity.user_id == User.id)
        .where(
            UserIdentity.issuer == issuer,
            UserIdentity.subject == subject,
        )
    )
    return await session.scalar(stmt)


async def create_user_with_identity(
    session: AsyncSession,
    *,
    issuer: str,
    subject: str,
) -> User:
    """Insert a new active local user and bind `(issuer, subject)` in the current session."""
    user = User(status="active")
    session.add(user)
    await session.flush()

    identity = UserIdentity(
        issuer=issuer,
        subject=subject,
        user_id=user.id,
    )
    session.add(identity)
    await session.flush()
    await session.refresh(user)
    return user


async def update_user_status(
    session: AsyncSession,
    *,
    user_id: UUID,
    status: UserStatus,
    deleted_at: datetime | None = None,
) -> User | None:
    """Update a user's lifecycle status within the caller's session."""
    user = await get_user_by_id(session, user_id=user_id)
    if user is None:
        return None
    user.status = status
    if deleted_at is not None:
        user.deleted_at = deleted_at
    await session.flush()
    await session.refresh(user)
    return user
