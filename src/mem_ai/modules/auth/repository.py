"""PostgreSQL repository for `auth_sessions`，所有函数显式接收调用方的 `AsyncSession`。"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, delete, or_, select, tuple_, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import normalize_utc
from mem_ai.modules.auth.models import AuthSession

_MAX_FAMILY_SESSIONS = 10_000


async def create_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    token_hash: str,
    expires_at: datetime,
    last_used_at: datetime,
) -> AuthSession:
    """插入新的登录会话并刷新主键。"""
    auth_session = AuthSession(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=expires_at,
        last_used_at=last_used_at,
    )
    session.add(auth_session)
    await session.flush()
    return auth_session


async def get_session_by_token_hash(
    session: AsyncSession,
    *,
    token_hash: str,
    for_update: bool = False,
) -> AuthSession | None:
    """按 Refresh Token 摘要读取会话，可选行锁以串行化并发轮换。"""
    stmt = select(AuthSession).where(AuthSession.token_hash == token_hash)
    if for_update:
        stmt = stmt.with_for_update()
    return await session.scalar(stmt)


async def mark_session_rotated(
    session: AsyncSession,
    *,
    old_session: AuthSession,
    new_session_id: UUID,
    revoked_at: datetime,
) -> None:
    """将旧会话标记为已轮换并指向后继会话。"""
    old_session.revoked_at = revoked_at
    old_session.replaced_by_session_id = new_session_id
    old_session.last_used_at = revoked_at
    await session.flush()


async def revoke_session_by_id(
    session: AsyncSession,
    *,
    session_id: UUID,
    revoked_at: datetime,
) -> int:
    """吊销单个仍活跃的会话，返回受影响行数。"""
    result = await session.execute(
        update(AuthSession)
        .where(AuthSession.id == session_id, AuthSession.revoked_at.is_(None))
        .values(revoked_at=revoked_at)
    )
    assert isinstance(result, CursorResult)
    return int(result.rowcount or 0)


async def revoke_session_family(
    session: AsyncSession,
    *,
    root_session_id: UUID,
    revoked_at: datetime,
) -> int:
    """沿 `replaced_by_session_id` 双向遍历轮换链并吊销整个会话家族。"""
    visited: set[UUID] = set()
    frontier: set[UUID] = {root_session_id}
    while frontier and len(visited) < _MAX_FAMILY_SESSIONS:
        batch = tuple(frontier - visited)
        if not batch:
            break
        visited.update(batch)
        rows = (
            await session.execute(
                select(AuthSession.id, AuthSession.replaced_by_session_id).where(
                    or_(
                        AuthSession.id.in_(batch),
                        AuthSession.replaced_by_session_id.in_(batch),
                    )
                )
            )
        ).all()
        next_frontier: set[UUID] = set()
        for row_id, replaced_by_id in rows:
            if row_id not in visited:
                next_frontier.add(row_id)
            if replaced_by_id is not None and replaced_by_id not in visited:
                next_frontier.add(replaced_by_id)
        frontier = next_frontier

    result = await session.execute(
        update(AuthSession)
        .where(AuthSession.id.in_(tuple(visited)), AuthSession.revoked_at.is_(None))
        .values(revoked_at=revoked_at)
    )
    assert isinstance(result, CursorResult)
    return int(result.rowcount or 0)


async def revoke_user_sessions(
    session: AsyncSession,
    *,
    user_id: UUID,
    revoked_at: datetime,
    exclude_session_id: UUID | None = None,
) -> int:
    """吊销用户全部活跃会话，可排除当前会话。"""
    conditions = [AuthSession.user_id == user_id, AuthSession.revoked_at.is_(None)]
    if exclude_session_id is not None:
        conditions.append(AuthSession.id != exclude_session_id)
    result = await session.execute(
        update(AuthSession).where(and_(*conditions)).values(revoked_at=revoked_at)
    )
    assert isinstance(result, CursorResult)
    return int(result.rowcount or 0)


async def purge_expired_auth_sessions_batch(
    session: AsyncSession,
    *,
    current_time: datetime,
    revoked_cutoff: datetime,
    batch_size: int,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[int, int, tuple[datetime, UUID] | None, bool]:
    """按 `(expires_at, id)` 游标分批删除已过期或长期已吊销的会话，保护活跃会话。"""
    eligible = or_(
        AuthSession.expires_at <= current_time,
        and_(AuthSession.revoked_at.is_not(None), AuthSession.revoked_at <= revoked_cutoff),
    )
    stmt = select(AuthSession.id, AuthSession.expires_at).where(eligible)
    if cursor is not None:
        stmt = stmt.where(
            tuple_(AuthSession.expires_at, AuthSession.id) > (normalize_utc(cursor[0]), cursor[1])
        )
    rows = (
        await session.execute(
            stmt.order_by(AuthSession.expires_at.asc(), AuthSession.id.asc()).limit(batch_size + 1)
        )
    ).all()
    has_more = len(rows) > batch_size
    page = rows[:batch_size]
    if not page:
        return 0, 0, None, False

    result = await session.execute(
        delete(AuthSession).where(AuthSession.id.in_([row_id for row_id, _ in page]))
    )
    assert isinstance(result, CursorResult)
    last_id, last_expires_at = page[-1]
    next_cursor = (normalize_utc(last_expires_at), last_id)
    return int(result.rowcount or 0), len(page), next_cursor, has_more
