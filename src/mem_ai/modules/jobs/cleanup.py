"""User job projections, bounded coordination, retention purges, and final user cleanup."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.core.exceptions import ConflictError
from mem_ai.core.logging import sanitize_log_value
from mem_ai.modules.jobs import cleanup_repository, repository
from mem_ai.modules.jobs.schemas import JobRecord

JobAttemptProtectionFilter = Callable[
    [AsyncSession, Sequence[UUID], Sequence[UUID]],
    Awaitable[tuple[frozenset[UUID], frozenset[UUID]]],
]


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


async def checkpoint_job_metadata_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    metadata_patch: dict[str, Any],
    now: datetime | None = None,
) -> JobRecord:
    """Conditionally persist sanitized progress metadata only while `lease_token` is active."""
    current_time = (
        await repository.get_database_now(session) if now is None else repository.normalize_utc(now)
    )
    updated = await cleanup_repository.conditional_checkpoint_job_metadata(
        session,
        job_id=job_id,
        lease_token=lease_token,
        metadata_patch=sanitize_summary_dict(metadata_patch),
        current_time=current_time,
    )
    if updated is None:
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer active or has expired",
        )
    return JobRecord.model_validate(updated)


async def find_active_or_unsettled_job_ids_in_session(
    session: AsyncSession,
    *,
    job_ids: Sequence[UUID],
    current_time: datetime,
) -> frozenset[UUID]:
    """Return job IDs that are still non-terminal or hold an unexpired lease."""
    return await cleanup_repository.find_active_or_unsettled_job_ids(
        session,
        job_ids=job_ids,
        current_time=current_time,
    )


@dataclass(frozen=True, slots=True)
class JobAttemptPurgeBatch:
    """Result of one bounded expired `job_attempts` retention scan batch."""

    deleted_count: int
    protected_skipped: int
    scanned_count: int
    next_cursor: tuple[datetime, UUID] | None
    has_more: bool


def _extract_operation_id(metadata_json: dict[str, Any] | None) -> UUID | None:
    if not isinstance(metadata_json, dict):
        return None
    raw = metadata_json.get("operation_id")
    if not isinstance(raw, str):
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


async def purge_expired_job_attempts_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
    protected_job_filter: JobAttemptProtectionFilter | None = None,
) -> JobAttemptPurgeBatch:
    """Delete one bounded batch of expired `job_attempts` while protecting active/reconciling work.

    Never deletes running/unfinished attempts, attempts of non-terminal (`pending`, `running`,
    `retry_wait`, `reconciling`) or actively leased jobs, or attempts whose parent job is linked
    to a non-terminal operation. Parent `jobs` rows (carrying `dedupe_key` and `external_ref`)
    remain intact.
    """
    candidates, next_cursor, has_more = await cleanup_repository.scan_expired_job_attempts_batch(
        session,
        current_time=current_time,
        batch_size=batch_size,
        cursor=cursor,
    )
    if not candidates:
        return JobAttemptPurgeBatch(
            deleted_count=0,
            protected_skipped=0,
            scanned_count=0,
            next_cursor=None,
            has_more=False,
        )

    protected_job_ids: frozenset[UUID] = frozenset()
    protected_op_ids: frozenset[UUID] = frozenset()
    if protected_job_filter is not None:
        candidate_job_ids = tuple({job_id for _, job_id, _, _ in candidates})
        candidate_op_ids = tuple(
            {
                op_id
                for _, _, _, metadata_json in candidates
                if (op_id := _extract_operation_id(metadata_json)) is not None
            }
        )
        protected_job_ids, protected_op_ids = await protected_job_filter(
            session, candidate_job_ids, candidate_op_ids
        )

    ids_to_delete: list[UUID] = []
    protected_skipped = 0
    for attempt_id, job_id, _, metadata_json in candidates:
        op_id = _extract_operation_id(metadata_json)
        if job_id in protected_job_ids or (op_id is not None and op_id in protected_op_ids):
            protected_skipped += 1
            continue
        ids_to_delete.append(attempt_id)

    deleted_count = await cleanup_repository.delete_job_attempts_by_ids(
        session,
        attempt_ids=ids_to_delete,
    )
    return JobAttemptPurgeBatch(
        deleted_count=deleted_count,
        protected_skipped=protected_skipped,
        scanned_count=len(candidates),
        next_cursor=next_cursor,
        has_more=has_more,
    )


@dataclass(frozen=True, slots=True)
class PeriodicJobPurgeBatch:
    """Result of one bounded expired periodic system job cleanup batch."""

    deleted_count: int
    scanned_count: int
    next_cursor: tuple[datetime, UUID] | None
    has_more: bool


async def purge_expired_periodic_jobs_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    retention_days: int,
    allowed_job_types: Sequence[str],
    exclude_job_id: UUID | None = None,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> PeriodicJobPurgeBatch:
    """Prune old settled system periodic jobs while preserving all user/provider/failed jobs."""
    (
        deleted_count,
        scanned_count,
        next_cursor,
        has_more,
    ) = await cleanup_repository.purge_expired_periodic_jobs_batch(
        session,
        current_time=current_time,
        retention_days=retention_days,
        allowed_job_types=allowed_job_types,
        exclude_job_id=exclude_job_id,
        batch_size=batch_size,
        cursor=cursor,
    )
    return PeriodicJobPurgeBatch(
        deleted_count=deleted_count,
        scanned_count=scanned_count,
        next_cursor=next_cursor,
        has_more=has_more,
    )
