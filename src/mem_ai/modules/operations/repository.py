"""SQLAlchemy repository for operation persistence and owner-scoped queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

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
