"""SQLAlchemy repository for `memory_scopes`, `memory_tombstones`, and `memory_list_snapshots`."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select, union, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import get_database_now as get_database_now
from mem_ai.db.base import is_unique_violation
from mem_ai.db.base import normalize_utc as normalize_utc
from mem_ai.modules.memories.models import (
    MEMORY_SCOPE_UNIQUE_CONSTRAINT,
    MemoryListSnapshot,
    MemoryScopeState,
    MemoryTombstone,
)

_UNSET: Any = object()


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


async def delete_list_snapshots_for_scope(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
) -> int:
    """Invalidate cached pagination snapshots for `(user_id, provider_id)` after delete/clear."""
    stmt = (
        delete(MemoryListSnapshot)
        .where(
            MemoryListSnapshot.user_id == user_id,
            MemoryListSnapshot.provider_id == provider_id,
        )
        .returning(MemoryListSnapshot.id)
    )
    rows = (await session.execute(stmt)).all()
    return len(rows)


async def conditional_transition_scope_barrier(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    allowed_from_states: Sequence[str],
    new_barrier_state: str,
    updated_at: datetime,
    new_epoch: int | None = None,
    barrier_operation_id: UUID | None = _UNSET,
    expected_barrier_operation_id: UUID | None = _UNSET,
) -> MemoryScopeState | None:
    """Conditionally transition `memory_scopes.barrier_state` guarded by allowed source states."""
    if not allowed_from_states:
        return None
    conditions = [
        MemoryScopeState.user_id == user_id,
        MemoryScopeState.provider_id == provider_id,
        MemoryScopeState.barrier_state.in_(tuple(allowed_from_states)),
    ]
    if expected_barrier_operation_id is not _UNSET:
        if expected_barrier_operation_id is None:
            conditions.append(MemoryScopeState.barrier_operation_id.is_(None))
        else:
            conditions.append(
                MemoryScopeState.barrier_operation_id == expected_barrier_operation_id
            )

    values: dict[str, Any] = {
        "barrier_state": new_barrier_state,
        "updated_at": updated_at,
    }
    if new_epoch is not None:
        values["epoch"] = new_epoch
    if barrier_operation_id is not _UNSET:
        values["barrier_operation_id"] = barrier_operation_id

    stmt = (
        update(MemoryScopeState)
        .where(*conditions)
        .values(**values)
        .returning(MemoryScopeState)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def create_tombstone(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    target_kind: str,
    memory_id: str | None = None,
    conversation_id: UUID | None = None,
    memory_epoch: int = 0,
    operation_id: UUID | None = None,
    status: str = "pending",
    external_ref: str | None = None,
) -> MemoryTombstone:
    """Insert and flush a `memory_tombstones` row inside the active transaction."""
    row = MemoryTombstone(
        user_id=user_id,
        provider_id=provider_id,
        target_kind=target_kind,
        memory_id=memory_id,
        conversation_id=conversation_id,
        memory_epoch=memory_epoch,
        operation_id=operation_id,
        status=status,
        external_ref=external_ref,
    )
    session.add(row)
    await session.flush()
    return row


async def get_tombstone_by_id(
    session: AsyncSession,
    *,
    tombstone_id: UUID,
    for_update: bool = False,
) -> MemoryTombstone | None:
    """Load a tombstone row by primary key."""
    stmt = select(MemoryTombstone).where(MemoryTombstone.id == tombstone_id)
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_memory_tombstone(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    memory_id: str,
    for_update: bool = False,
) -> MemoryTombstone | None:
    """Load the unique single-memory tombstone for `(user_id, provider_id, memory_id)`."""
    stmt = select(MemoryTombstone).where(
        MemoryTombstone.user_id == user_id,
        MemoryTombstone.provider_id == provider_id,
        MemoryTombstone.target_kind == "memory",
        MemoryTombstone.memory_id == memory_id,
    )
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def attach_operation_to_tombstone(
    session: AsyncSession,
    *,
    tombstone_id: UUID,
    operation_id: UUID,
    external_ref: str | None,
    updated_at: datetime,
) -> MemoryTombstone | None:
    """Re-bind a non-verified tombstone to a newly created retry operation."""
    stmt = (
        update(MemoryTombstone)
        .where(
            MemoryTombstone.id == tombstone_id,
            MemoryTombstone.status.in_(("pending", "reconciling")),
        )
        .values(
            operation_id=operation_id,
            external_ref=external_ref,
            updated_at=updated_at,
        )
        .returning(MemoryTombstone)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def conditional_transition_tombstone(
    session: AsyncSession,
    *,
    tombstone_id: UUID,
    new_status: str,
    allowed_from_statuses: Sequence[str],
    updated_at: datetime,
    external_ref: str | None = None,
    verified_at: datetime | None = None,
) -> MemoryTombstone | None:
    """Conditionally transition `memory_tombstones.status` following §11.3.1."""
    if not allowed_from_statuses:
        return None
    values: dict[str, Any] = {
        "status": new_status,
        "updated_at": updated_at,
    }
    if external_ref is not None:
        values["external_ref"] = external_ref
    if verified_at is not None:
        values["verified_at"] = verified_at

    stmt = (
        update(MemoryTombstone)
        .where(
            MemoryTombstone.id == tombstone_id,
            MemoryTombstone.status.in_(tuple(allowed_from_statuses)),
        )
        .values(**values)
        .returning(MemoryTombstone)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def delete_tombstone_by_id(
    session: AsyncSession,
    *,
    tombstone_id: UUID,
) -> None:
    """Remove a pending tombstone when the provider confirms the target was never owned."""
    stmt = delete(MemoryTombstone).where(
        MemoryTombstone.id == tombstone_id,
        MemoryTombstone.status == "pending",
    )
    await session.execute(stmt)


async def load_active_tombstones(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    conversation_id: UUID | None = None,
) -> tuple[frozenset[str], bool]:
    """Load all tombstoned memory IDs (any status) and optional conversation tombstone flag."""
    mem_stmt = select(MemoryTombstone.memory_id).where(
        MemoryTombstone.user_id == user_id,
        MemoryTombstone.provider_id == provider_id,
        MemoryTombstone.target_kind == "memory",
        MemoryTombstone.memory_id.is_not(None),
    )
    memory_ids = frozenset(
        str(mid) for mid in (await session.execute(mem_stmt)).scalars().all() if mid is not None
    )

    conv_tombstoned = False
    if conversation_id is not None:
        conv_stmt = select(MemoryTombstone.id).where(
            MemoryTombstone.user_id == user_id,
            MemoryTombstone.provider_id == provider_id,
            MemoryTombstone.target_kind == "conversation",
            MemoryTombstone.conversation_id == conversation_id,
        )
        conv_tombstoned = (await session.execute(conv_stmt)).scalar_one_or_none() is not None

    return memory_ids, conv_tombstoned


async def list_provider_ids(session: AsyncSession, *, user_id: UUID) -> list[str]:
    """Find persisted provider scopes, including providers unavailable in the current process."""
    return list(
        await session.scalars(
            union(
                select(MemoryScopeState.provider_id).where(MemoryScopeState.user_id == user_id),
                select(MemoryTombstone.provider_id).where(MemoryTombstone.user_id == user_id),
                select(MemoryListSnapshot.provider_id).where(MemoryListSnapshot.user_id == user_id),
            ).order_by("provider_id")
        )
    )


async def list_conversation_tombstones(
    session: AsyncSession, *, user_id: UUID, conversation_id: UUID
) -> Sequence[MemoryTombstone]:
    """Read retained range barriers for one immutable conversation ID."""
    return (
        await session.scalars(
            select(MemoryTombstone)
            .where(
                MemoryTombstone.user_id == user_id,
                MemoryTombstone.target_kind == "conversation",
                MemoryTombstone.conversation_id == conversation_id,
            )
            .order_by(MemoryTombstone.provider_id)
        )
    ).all()


async def load_tombstoned_conversation_ids(
    session: AsyncSession, *, user_id: UUID, provider_id: str
) -> frozenset[UUID]:
    """Keep deleted ranges isolated even in unfiltered lists after local root removal."""
    return frozenset(
        await session.scalars(
            select(MemoryTombstone.conversation_id).where(
                MemoryTombstone.user_id == user_id,
                MemoryTombstone.provider_id == provider_id,
                MemoryTombstone.target_kind == "conversation",
                MemoryTombstone.conversation_id.is_not(None),
            )
        )
    )


async def _delete_user_models_in_batch(
    session: AsyncSession,
    *,
    user_id: UUID,
    models: tuple[type[MemoryListSnapshot] | type[MemoryTombstone] | type[MemoryScopeState], ...],
    batch_size: int = 50,
) -> bool:
    """Delete one bounded batch across `models`; return False while matching rows remain."""
    remaining = max(1, min(100, batch_size))
    for model in models:
        selected = list(
            await session.scalars(
                select(model.id)
                .where(model.user_id == user_id)
                .order_by(model.id)
                .limit(remaining)
                .with_for_update(skip_locked=True)
            )
        )
        if selected:
            await session.execute(delete(model).where(model.id.in_(selected)))
            remaining -= len(selected)
        if await session.scalar(select(select(model.id).where(model.user_id == user_id).exists())):
            return False
        if remaining == 0:
            return False
    return True


async def delete_user_list_snapshots(
    session: AsyncSession, *, user_id: UUID, batch_size: int = 50
) -> bool:
    """Remove cached pagination snapshots in bounded batches during local user cleanup."""
    return await _delete_user_models_in_batch(
        session,
        user_id=user_id,
        models=(MemoryListSnapshot,),
        batch_size=batch_size,
    )


async def delete_user_memory_state(
    session: AsyncSession, *, user_id: UUID, batch_size: int = 50
) -> bool:
    """Remove a finite batch after remote confirmation; remaining rows defer completion."""
    return await _delete_user_models_in_batch(
        session,
        user_id=user_id,
        models=(MemoryListSnapshot, MemoryTombstone, MemoryScopeState),
        batch_size=batch_size,
    )
