"""User job projections, bounded coordination, and final removal after resource settlement."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.core.logging import sanitize_log_value
from mem_ai.modules.jobs import repository
from mem_ai.modules.jobs.schemas import JobRecord


async def cleanup_user_jobs_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    exclude_job_id: UUID,
    batch_size: int = 50,
    now: datetime | None = None,
) -> bool:
    """Remove terminal jobs while retaining the coordinator and every unsettled job.

    The caller has persisted the irreversible deleting state and resource owners settled jobs.
    Unknown nonterminal jobs and valid leases block completion rather than being cancelled.
    """
    current_time = (
        await repository.get_database_now(session) if now is None else repository.normalize_utc(now)
    )
    return await repository.delete_terminal_user_job_batch(
        session,
        user_id=user_id,
        exclude_job_id=exclude_job_id,
        batch_size=batch_size,
        current_time=current_time,
    )


async def list_user_jobs_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    job_types: Sequence[str],
    statuses: Sequence[str],
    for_update: bool = False,
    metadata_match: dict[str, Any] | None = None,
    metadata_exclude: dict[str, Any] | None = None,
    after_job_id: UUID | None = None,
    order_by_job_id: bool = False,
    limit: int | None = None,
    skip_locked: bool = False,
) -> list[JobRecord]:
    """List user jobs matching `job_types` and `statuses` inside the caller's transaction."""
    rows = await repository.list_jobs_for_user_and_types(
        session,
        user_id=user_id,
        job_types=job_types,
        statuses=statuses,
        for_update=for_update,
        metadata_match=metadata_match,
        metadata_exclude=metadata_exclude,
        after_job_id=after_job_id,
        order_by_job_id=order_by_job_id,
        limit=limit,
        skip_locked=skip_locked,
    )
    return [JobRecord.model_validate(row) for row in rows]


async def list_user_job_metadata_values_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    job_types: Sequence[str],
    field: str,
) -> list[str]:
    """Read the complete distinct metadata inventory without loading historical job rows."""
    return await repository.list_user_job_metadata_values(
        session, user_id=user_id, job_types=job_types, field=field
    )


async def has_user_jobs_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    job_types: Sequence[str],
    statuses: Sequence[str],
    metadata_match: dict[str, Any] | None = None,
    metadata_exclude: dict[str, Any] | None = None,
    active_lease_at: datetime | None = None,
) -> bool:
    """Check existence without materializing user job history or overlooking locked rows."""
    return await repository.has_user_jobs(
        session,
        user_id=user_id,
        job_types=job_types,
        statuses=statuses,
        metadata_match=metadata_match,
        metadata_exclude=metadata_exclude,
        active_lease_at=active_lease_at,
    )


def sanitize_summary_dict(summary: dict[str, Any]) -> dict[str, Any]:
    """Sanitize a JSON summary or metadata mapping before persisting it on a job."""
    sanitized = sanitize_log_value(None, summary)
    return sanitized if isinstance(sanitized, dict) else {}


async def patch_job_metadata_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    metadata_patch: dict[str, Any],
    now: datetime | None = None,
) -> None:
    """Merge sanitized coordination metadata inside the caller's job-locking transaction."""
    current_time = (
        await repository.get_database_now(session) if now is None else repository.normalize_utc(now)
    )
    await repository.patch_job_metadata(
        session,
        job_id=job_id,
        metadata_patch=sanitize_summary_dict(metadata_patch),
        current_time=current_time,
    )
