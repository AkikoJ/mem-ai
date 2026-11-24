"""SQLAlchemy repository for lease-fenced checkpoints and bounded retention purges on `jobs`."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, delete, exists, or_, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import normalize_utc
from mem_ai.modules.jobs.models import Job, JobAttempt


async def conditional_checkpoint_job_metadata(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    metadata_patch: dict[str, Any],
    current_time: datetime,
) -> Job | None:
    """Merge sanitized progress metadata only while `(job_id, lease_token)` holds a live lease."""
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == "running",
            Job.lease_token == lease_token,
            or_(Job.lease_expires_at.is_(None), Job.lease_expires_at > current_time),
        )
        .values(
            metadata_json=Job.metadata_json.concat(metadata_patch),
            updated_at=current_time,
        )
        .returning(Job)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def find_active_or_unsettled_job_ids(
    session: AsyncSession,
    *,
    job_ids: Sequence[UUID],
    current_time: datetime,
) -> frozenset[UUID]:
    """Return job IDs that are still non-terminal or hold an unexpired lease."""
    unique_ids = tuple(set(job_ids))
    if not unique_ids:
        return frozenset()
    utc_now = normalize_utc(current_time)
    stmt = select(Job.id).where(
        Job.id.in_(unique_ids),
        or_(
            Job.status.in_(("pending", "running", "retry_wait", "reconciling")),
            and_(Job.lease_expires_at.is_not(None), Job.lease_expires_at > utc_now),
        ),
    )
    return frozenset((await session.scalars(stmt)).all())


async def scan_expired_job_attempts_batch(
    session: AsyncSession,
    *,
    current_time: datetime,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[
    list[tuple[UUID, UUID, datetime, dict[str, Any] | None]],
    tuple[datetime, UUID] | None,
    bool,
]:
    """Lock and return one bounded batch of expired terminal `job_attempts` using keyset order.

    Concurrently locked rows are skipped via `FOR UPDATE OF job_attempts SKIP LOCKED` so
    retention never blocks live job commits; skipped rows are swept on the next periodic window.
    """
    utc_now = normalize_utc(current_time)
    bounded = max(1, min(500, batch_size))
    base_conditions = [
        JobAttempt.status != "running",
        JobAttempt.finished_at.is_not(None),
        JobAttempt.expires_at <= utc_now,
        Job.status.in_(("succeeded", "failed", "cancelled")),
        or_(Job.lease_expires_at.is_(None), Job.lease_expires_at <= utc_now),
    ]
    conditions = list(base_conditions)
    if cursor is not None:
        cursor_ts, cursor_id = normalize_utc(cursor[0]), cursor[1]
        conditions.append(tuple_(JobAttempt.expires_at, JobAttempt.id) > (cursor_ts, cursor_id))

    stmt = (
        select(JobAttempt.id, JobAttempt.job_id, JobAttempt.expires_at, Job.metadata_json)
        .join(Job, Job.id == JobAttempt.job_id)
        .where(*conditions)
        .order_by(JobAttempt.expires_at.asc(), JobAttempt.id.asc())
        .limit(bounded)
        .with_for_update(of=JobAttempt, skip_locked=True)
    )
    rows = list((await session.execute(stmt)).all())
    if not rows:
        return [], None, False

    items: list[tuple[UUID, UUID, datetime, dict[str, Any] | None]] = [
        (row.id, row.job_id, normalize_utc(row.expires_at), row.metadata_json) for row in rows
    ]
    last_id, _, last_expires_at, _ = items[-1]
    next_cursor = (last_expires_at, last_id)
    has_more = bool(
        await session.scalar(
            select(
                exists(
                    select(JobAttempt.id)
                    .join(Job, Job.id == JobAttempt.job_id)
                    .where(
                        *base_conditions,
                        tuple_(JobAttempt.expires_at, JobAttempt.id) > next_cursor,
                    )
                )
            )
        )
    )
    return items, (next_cursor if has_more else None), has_more


async def delete_job_attempts_by_ids(
    session: AsyncSession,
    *,
    attempt_ids: Sequence[UUID],
) -> int:
    """Delete a vetted batch of `job_attempts` rows by ID and return the deleted count."""
    unique_ids = tuple(set(attempt_ids))
    if not unique_ids:
        return 0
    result = await session.execute(
        delete(JobAttempt).where(JobAttempt.id.in_(unique_ids)).returning(JobAttempt.id)
    )
    return len(result.all())


async def purge_expired_periodic_jobs_batch(
    session: AsyncSession,
    *,
    current_time: datetime,
    retention_days: int,
    allowed_job_types: Sequence[str],
    exclude_job_id: UUID | None = None,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> tuple[int, int, tuple[datetime, UUID] | None, bool]:
    """Delete one bounded batch of old settled system periodic jobs, preserving all business jobs.

    Concurrently locked rows are skipped via `SKIP LOCKED` and swept on a subsequent window.
    """
    if not allowed_job_types:
        return 0, 0, None, False
    utc_now = normalize_utc(current_time)
    cutoff = utc_now - timedelta(days=retention_days)
    bounded = max(1, min(500, batch_size))
    base_conditions = [
        Job.job_type.in_(tuple(allowed_job_types)),
        Job.user_id.is_(None),
        Job.external_ref.is_(None),
        Job.dedupe_key.like("periodic:%"),
        Job.status.in_(("succeeded", "cancelled")),
        Job.finished_at.is_not(None),
        Job.finished_at <= cutoff,
    ]
    if exclude_job_id is not None:
        base_conditions.append(Job.id != exclude_job_id)

    conditions = list(base_conditions)
    if cursor is not None:
        cursor_ts, cursor_id = normalize_utc(cursor[0]), cursor[1]
        conditions.append(tuple_(Job.finished_at, Job.id) > (cursor_ts, cursor_id))

    stmt = (
        select(Job.id, Job.finished_at)
        .where(*conditions)
        .order_by(Job.finished_at.asc(), Job.id.asc())
        .limit(bounded)
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).all())
    if not rows:
        return 0, 0, None, False

    ids_to_delete = [row.id for row in rows]
    await session.execute(delete(Job).where(Job.id.in_(ids_to_delete)))

    last_row = rows[-1]
    assert last_row.finished_at is not None
    next_cursor = (normalize_utc(last_row.finished_at), last_row.id)
    has_more = bool(
        await session.scalar(
            select(
                exists(
                    select(Job.id).where(
                        *base_conditions,
                        tuple_(Job.finished_at, Job.id) > next_cursor,
                    )
                )
            )
        )
    )
    return len(ids_to_delete), len(rows), (next_cursor if has_more else None), has_more
