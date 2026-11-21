"""SQLAlchemy repository for operation persistence and owner-scoped queries."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
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
