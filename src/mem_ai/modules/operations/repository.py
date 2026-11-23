"""SQLAlchemy repository for operation persistence and owner-scoped queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
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
