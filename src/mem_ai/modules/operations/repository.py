"""SQLAlchemy repository for operation persistence and owner-scoped queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, delete, exists, or_, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import normalize_utc
from mem_ai.modules.operations.models import Operation


async def create_operation(
    session: AsyncSession,
    *,
    owner_id: UUID | None,
    operation_type: str,
    status: str,
    target_type: str | None,
    target_id: str | None,
    job_id: UUID | None,
    progress: dict[str, Any],
    result_summary: dict[str, Any],
    error_code: str | None,
    error_message: str | None,
    expires_at: datetime | None,
    completed_at: datetime | None,
) -> Operation:
    """Insert and flush an operation row inside the active transaction."""
    operation = Operation(
        owner_id=owner_id,
        operation_type=operation_type,
        status=status,
        target_type=target_type,
        target_id=target_id,
        job_id=job_id,
        progress=dict(progress),
        result_summary=dict(result_summary),
        error_code=error_code,
        error_message=error_message,
        expires_at=expires_at,
        completed_at=completed_at,
    )
    session.add(operation)
    await session.flush()
    return operation


async def get_operation_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    operation_id: UUID,
) -> Operation | None:
    """Load an operation only if it is owned by `owner_id`."""
    stmt = select(Operation).where(
        Operation.id == operation_id,
        Operation.owner_id == owner_id,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_operation_by_id(
    session: AsyncSession,
    *,
    operation_id: UUID,
    for_update: bool = False,
) -> Operation | None:
    """Load an operation by primary key inside the active transaction."""
    stmt = select(Operation).where(Operation.id == operation_id)
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def find_operation_for_target(
    session: AsyncSession, *, owner_id: UUID, operation_type: str, target_id: str
) -> Operation | None:
    """Find the latest owner-bound operation for an immutable business target."""
    return await session.scalar(
        select(Operation)
        .where(
            Operation.owner_id == owner_id,
            Operation.operation_type == operation_type,
            Operation.target_id == target_id,
        )
        .order_by(Operation.created_at.desc(), Operation.id.desc())
        .limit(1)
    )


async def attach_job_id(
    session: AsyncSession,
    *,
    operation_id: UUID,
    job_id: UUID,
    updated_at: datetime,
) -> Operation | None:
    """Attach the backing durable job ID to an operation inside the active transaction."""
    stmt = (
        update(Operation)
        .where(Operation.id == operation_id)
        .values(job_id=job_id, updated_at=updated_at)
        .returning(Operation)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def conditional_transition_operation(
    session: AsyncSession,
    *,
    operation_id: UUID,
    new_status: str,
    allowed_from_statuses: Sequence[str],
    progress: dict[str, Any] | None,
    result_summary: dict[str, Any] | None,
    error_code: str | None,
    error_message: str | None,
    completed_at: datetime | None,
    updated_at: datetime,
) -> Operation | None:
    """Conditionally advance an operation strictly according to allowed source statuses."""
    if not allowed_from_statuses:
        return None
    values: dict[str, Any] = {
        "status": new_status,
        "error_code": error_code,
        "error_message": error_message,
        "completed_at": completed_at,
        "updated_at": updated_at,
    }
    if progress is not None:
        values["progress"] = dict(progress)
    if result_summary is not None:
        values["result_summary"] = dict(result_summary)

    stmt = (
        update(Operation)
        .where(
            Operation.id == operation_id,
            Operation.status.in_(tuple(allowed_from_statuses)),
        )
        .values(**values)
        .returning(Operation)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def set_deletion_receipt(
    session: AsyncSession,
    *,
    operation_id: UUID,
    digest: str,
) -> None:
    """Bind a protected receipt digest without exposing it in application DTOs."""
    await session.execute(
        update(Operation)
        .where(Operation.id == operation_id, Operation.operation_type == "user_delete")
        .values(deletion_receipt_digest=digest)
    )


async def revoke_deletion_receipt(
    session: AsyncSession, *, operation_id: UUID, revoked_at: datetime
) -> bool:
    """Permanently revoke a user deletion receipt inside the caller's transaction."""
    changed = await session.scalar(
        update(Operation)
        .where(
            Operation.id == operation_id,
            Operation.operation_type == "user_delete",
            Operation.deletion_receipt_revoked_at.is_(None),
        )
        .values(deletion_receipt_revoked_at=revoked_at)
        .returning(Operation.id)
    )
    return changed is not None


def _clamp_batch_size(batch_size: int) -> int:
    return max(1, min(100, batch_size))


def _other_user_operations_filter(owner_id: UUID, deletion_operation_id: UUID) -> tuple[Any, Any]:
    return (
        Operation.owner_id == owner_id,
        Operation.id != deletion_operation_id,
    )


async def _has_unsettled_user_operations(
    session: AsyncSession, other_operations: tuple[Any, Any]
) -> bool:
    unsettled = await session.scalar(
        select(Operation.id)
        .where(*other_operations, Operation.status.not_in(("succeeded", "failed")))
        .limit(1)
    )
    return unsettled is not None


async def list_non_terminal_operations_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    operation_type: str,
    batch_size: int = 50,
) -> list[Operation]:
    """Read one bounded batch of non-terminal operations owned by `owner_id`."""
    stmt = (
        select(Operation)
        .where(
            Operation.owner_id == owner_id,
            Operation.operation_type == operation_type,
            Operation.status.not_in(("succeeded", "failed")),
        )
        .order_by(Operation.id)
        .limit(_clamp_batch_size(batch_size))
        .with_for_update(skip_locked=True)
    )
    return list((await session.scalars(stmt)).all())


async def can_finalize_user_operations_in_batch(
    session: AsyncSession,
    *,
    owner_id: UUID,
    deletion_operation_id: UUID,
    batch_size: int = 50,
) -> bool:
    """Return True iff all remaining child operations are settled and fit in one batch."""
    other_operations = _other_user_operations_filter(owner_id, deletion_operation_id)
    if await _has_unsettled_user_operations(session, other_operations):
        return False
    bounded = _clamp_batch_size(batch_size)
    extra = list(
        await session.scalars(select(Operation.id).where(*other_operations).limit(bounded + 1))
    )
    return len(extra) <= bounded


async def finalize_user_operations(
    session: AsyncSession, *, owner_id: UUID, deletion_operation_id: UUID, batch_size: int = 50
) -> bool:
    """Remove settled business operation payloads, preserving the minimal deletion receipt."""
    deletion = await get_operation_by_id(
        session, operation_id=deletion_operation_id, for_update=True
    )
    if (
        deletion is None
        or deletion.owner_id != owner_id
        or deletion.operation_type != "user_delete"
    ):
        return False
    other_operations = _other_user_operations_filter(owner_id, deletion_operation_id)
    if await _has_unsettled_user_operations(session, other_operations):
        return False
    batch = list(
        await session.scalars(
            select(Operation.id)
            .where(*other_operations)
            .order_by(Operation.id)
            .limit(_clamp_batch_size(batch_size))
            .with_for_update(skip_locked=True)
        )
    )
    if batch:
        await session.execute(delete(Operation).where(Operation.id.in_(batch)))
    remaining = await session.scalar(select(Operation.id).where(*other_operations).limit(1))
    if remaining is not None:
        return False
    deletion.target_id = None
    deletion.result_summary = {}
    deletion.error_code = None
    deletion.error_message = None
    await session.flush()
    return True


async def find_non_terminal_operation_links(
    session: AsyncSession,
    *,
    job_ids: Sequence[UUID] = (),
    operation_ids: Sequence[UUID] = (),
) -> tuple[frozenset[UUID], frozenset[UUID]]:
    """Return `(protected_job_ids, protected_operation_ids)` still tied to non-terminal ops."""
    unique_jobs = tuple(set(job_ids))
    unique_ops = tuple(set(operation_ids))
    protected_jobs: set[UUID] = set()
    protected_ops: set[UUID] = set()

    if unique_jobs:
        job_stmt = select(Operation.job_id).where(
            Operation.job_id.in_(unique_jobs),
            Operation.status.not_in(("succeeded", "failed")),
        )
        for job_id in (await session.scalars(job_stmt)).all():
            if job_id is not None:
                protected_jobs.add(job_id)

    if unique_ops:
        op_stmt = select(Operation.id).where(
            Operation.id.in_(unique_ops),
            Operation.status.not_in(("succeeded", "failed")),
        )
        protected_ops.update((await session.scalars(op_stmt)).all())

    return frozenset(protected_jobs), frozenset(protected_ops)


async def purge_expired_user_delete_operations_batch(
    session: AsyncSession,
    *,
    current_time: datetime,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[int, int, tuple[datetime, UUID] | None, bool]:
    """Delete finalized `user_delete` operations whose receipt TTL has elapsed.

    Non-terminal, failed, or still-owner-bound `user_delete` operations are strictly retained.
    """
    utc_now = normalize_utc(current_time)
    bounded = max(1, min(500, batch_size))
    base_conditions = [
        Operation.operation_type == "user_delete",
        Operation.status == "succeeded",
        Operation.owner_id.is_(None),
        Operation.expires_at.is_not(None),
        Operation.expires_at <= utc_now,
    ]
    conditions = list(base_conditions)
    if cursor is not None:
        cursor_ts, cursor_id = normalize_utc(cursor[0]), cursor[1]
        conditions.append(tuple_(Operation.expires_at, Operation.id) > (cursor_ts, cursor_id))

    stmt = (
        select(Operation.id, Operation.expires_at)
        .where(*conditions)
        .order_by(Operation.expires_at.asc(), Operation.id.asc())
        .limit(bounded)
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).all())
    if not rows:
        return 0, 0, None, False

    ids_to_delete = [row.id for row in rows]
    await session.execute(delete(Operation).where(Operation.id.in_(ids_to_delete)))

    last_row = rows[-1]
    assert last_row.expires_at is not None
    next_cursor = (normalize_utc(last_row.expires_at), last_row.id)
    has_more = bool(
        await session.scalar(
            select(
                exists(
                    select(Operation.id).where(
                        *base_conditions,
                        tuple_(Operation.expires_at, Operation.id) > next_cursor,
                    )
                )
            )
        )
    )
    return len(ids_to_delete), len(rows), (next_cursor if has_more else None), has_more


async def scan_expired_operation_summaries_batch(
    session: AsyncSession,
    *,
    current_time: datetime,
    retention_days: int,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[list[tuple[UUID, UUID | None, datetime]], tuple[datetime, UUID] | None, bool]:
    """Lock and return one bounded batch of settled non-user-delete operations for purge."""
    utc_now = normalize_utc(current_time)
    cutoff = utc_now - timedelta(days=retention_days)
    bounded = max(1, min(500, batch_size))
    base_conditions = [
        Operation.operation_type != "user_delete",
        Operation.status.in_(("succeeded", "failed")),
        Operation.completed_at.is_not(None),
        or_(
            and_(Operation.expires_at.is_not(None), Operation.expires_at <= utc_now),
            and_(Operation.expires_at.is_(None), Operation.completed_at <= cutoff),
        ),
    ]
    conditions = list(base_conditions)
    if cursor is not None:
        cursor_ts, cursor_id = normalize_utc(cursor[0]), cursor[1]
        conditions.append(tuple_(Operation.completed_at, Operation.id) > (cursor_ts, cursor_id))

    stmt = (
        select(Operation.id, Operation.job_id, Operation.completed_at)
        .where(*conditions)
        .order_by(Operation.completed_at.asc(), Operation.id.asc())
        .limit(bounded)
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).all())
    if not rows:
        return [], None, False

    items: list[tuple[UUID, UUID | None, datetime]] = []
    for row in rows:
        assert row.completed_at is not None
        items.append((row.id, row.job_id, normalize_utc(row.completed_at)))

    last_id, _, last_completed_at = items[-1]
    next_cursor = (last_completed_at, last_id)
    has_more = bool(
        await session.scalar(
            select(
                exists(
                    select(Operation.id).where(
                        *base_conditions,
                        tuple_(Operation.completed_at, Operation.id) > next_cursor,
                    )
                )
            )
        )
    )
    return items, (next_cursor if has_more else None), has_more


async def delete_operations_by_ids(session: AsyncSession, *, operation_ids: Sequence[UUID]) -> int:
    """Delete a vetted batch of operation rows by ID and return the deleted count."""
    unique_ids = tuple(set(operation_ids))
    if not unique_ids:
        return 0
    result = await session.execute(
        delete(Operation).where(Operation.id.in_(unique_ids)).returning(Operation.id)
    )
    return len(result.all())
