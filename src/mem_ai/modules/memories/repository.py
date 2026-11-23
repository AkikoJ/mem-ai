"""SQLAlchemy repository for `memory_scopes` and `memory_list_snapshots`."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import is_unique_violation
from mem_ai.modules.memories.models import (
    MEMORY_SCOPE_UNIQUE_CONSTRAINT,
    MemoryListSnapshot,
    MemoryScopeState,
)


async def get_scope(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    for_share: bool = False,
) -> MemoryScopeState | None:
    """Read the current `(user_id, provider_id)` scope state, optionally with `FOR SHARE`."""
    stmt = select(MemoryScopeState).where(
        MemoryScopeState.user_id == user_id,
        MemoryScopeState.provider_id == provider_id,
    )
    if for_share:
        stmt = stmt.with_for_update(read=True)
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_or_create_scope_for_update(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
) -> MemoryScopeState:
    """Lock `(user_id, provider_id)` row with `FOR UPDATE`, creating it safely on first use."""
    lock_stmt = (
        select(MemoryScopeState)
        .where(
            MemoryScopeState.user_id == user_id,
            MemoryScopeState.provider_id == provider_id,
        )
        .with_for_update()
    )
    existing = (await session.execute(lock_stmt)).scalar_one_or_none()
    if existing is not None:
        return existing

    try:
        async with session.begin_nested():
            created = MemoryScopeState(
                user_id=user_id,
                provider_id=provider_id,
                epoch=0,
                barrier_state="idle",
            )
            session.add(created)
            await session.flush()
    except IntegrityError as exc:
        if not is_unique_violation(exc, constraint_name=MEMORY_SCOPE_UNIQUE_CONSTRAINT):
            raise

    locked = (await session.execute(lock_stmt)).scalar_one()
    return locked


async def create_list_snapshot(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    filters_hash: str,
    items_json: list[dict[str, Any]],
    expires_at: datetime,
) -> MemoryListSnapshot:
    """Persist a bounded sorted snapshot for multi-replica cursor pagination."""
    snapshot = MemoryListSnapshot(
        user_id=user_id,
        provider_id=provider_id,
        filters_hash=filters_hash,
        items_json=items_json,
        total_items=len(items_json),
        expires_at=expires_at,
    )
    session.add(snapshot)
    await session.flush()
    return snapshot


async def get_list_snapshot(
    session: AsyncSession,
    *,
    snapshot_id: UUID,
    user_id: UUID,
    provider_id: str,
    filters_hash: str,
) -> MemoryListSnapshot | None:
    """Load a snapshot strictly bound to `(snapshot_id, user_id, provider_id, filters_hash)`."""
    stmt = select(MemoryListSnapshot).where(
        MemoryListSnapshot.id == snapshot_id,
        MemoryListSnapshot.user_id == user_id,
        MemoryListSnapshot.provider_id == provider_id,
        MemoryListSnapshot.filters_hash == filters_hash,
    )
    return (await session.execute(stmt)).scalar_one_or_none()
