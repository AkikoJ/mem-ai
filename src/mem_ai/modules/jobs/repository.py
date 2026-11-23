"""SQLAlchemy repository for jobs, attempts, SKIP LOCKED claiming, and `memory_jobs` view."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, column, func, or_, select, table, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.db.base import get_database_now as get_database_now
from mem_ai.db.base import is_unique_violation
from mem_ai.db.base import normalize_utc as normalize_utc
from mem_ai.modules.jobs.models import JOB_DEDUPE_KEY_UNIQUE_CONSTRAINT, Job, JobAttempt

_MEMORY_JOBS_VIEW = table(
    "memory_jobs",
    column("id"),
    column("job_type"),
    column("business_id"),
    column("user_id"),
    column("dedupe_key"),
    column("status"),
    column("attempt_count"),
    column("max_attempts"),
    column("lease_token"),
    column("lease_expires_at"),
    column("next_run_at"),
    column("started_at"),
    column("finished_at"),
    column("external_ref"),
    column("last_error_code"),
    column("last_error_summary"),
    column("write_id"),
    column("provider_id"),
    column("scope"),
    column("conversation_id"),
    column("generation_id"),
    column("memory_epoch"),
    column("metadata_json"),
    column("created_at"),
    column("updated_at"),
)


async def create_job(
    session: AsyncSession,
    *,
    job_type: str,
    business_id: str | None,
    user_id: UUID | None,
    dedupe_key: str | None,
    status: str,
    attempt_count: int,
    max_attempts: int,
    lease_token: str | None,
    lease_expires_at: datetime | None,
    next_run_at: datetime,
    started_at: datetime | None,
    metadata_json: dict[str, Any],
) -> Job:
    """Insert a new job row and flush it to obtain DB defaults."""
    job = Job(
        job_type=job_type,
        business_id=business_id,
        user_id=user_id,
        dedupe_key=dedupe_key,
        status=status,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        lease_token=lease_token,
        lease_expires_at=lease_expires_at,
        next_run_at=next_run_at,
        started_at=started_at,
        metadata_json=dict(metadata_json),
    )
    session.add(job)
    await session.flush()
    return job


async def get_job_by_dedupe_key(session: AsyncSession, *, dedupe_key: str) -> Job | None:
    """Load an existing job row by its unique `dedupe_key`."""
    stmt = select(Job).where(Job.dedupe_key == dedupe_key)
    return (await session.execute(stmt)).scalar_one_or_none()


async def enqueue_job_idempotent(
    session: AsyncSession,
    *,
    job_type: str,
    business_id: str | None,
    user_id: UUID | None,
    dedupe_key: str | None,
    max_attempts: int,
    next_run_at: datetime,
    metadata_json: dict[str, Any],
) -> tuple[Job, bool]:
    """Insert a pending job idempotently by `dedupe_key` inside the caller's transaction."""
    if dedupe_key is None:
        created = await create_job(
            session,
            job_type=job_type,
            business_id=business_id,
            user_id=user_id,
            dedupe_key=None,
            status="pending",
            attempt_count=0,
            max_attempts=max_attempts,
            lease_token=None,
            lease_expires_at=None,
            next_run_at=next_run_at,
            started_at=None,
            metadata_json=metadata_json,
        )
        return created, True

    try:
        async with session.begin_nested():
            created = await create_job(
                session,
                job_type=job_type,
                business_id=business_id,
                user_id=user_id,
                dedupe_key=dedupe_key,
                status="pending",
                attempt_count=0,
                max_attempts=max_attempts,
                lease_token=None,
                lease_expires_at=None,
                next_run_at=next_run_at,
                started_at=None,
                metadata_json=metadata_json,
            )
        return created, True
    except IntegrityError as exc:
        if not is_unique_violation(exc, constraint_name=JOB_DEDUPE_KEY_UNIQUE_CONSTRAINT):
            raise
        existing = await get_job_by_dedupe_key(session, dedupe_key=dedupe_key)
        if existing is None:
            raise
        return existing, False


async def get_job_by_id(
    session: AsyncSession,
    *,
    job_id: UUID,
    for_update: bool = False,
) -> Job | None:
    """Load a job row by primary key, optionally locking it with `FOR UPDATE`."""
    stmt = select(Job).where(Job.id == job_id)
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_jobs_for_user(
    session: AsyncSession,
    *,
    user_id: UUID,
    job_type: str | None = None,
) -> list[Job]:
    """List jobs belonging to `user_id`, optionally filtered by `job_type`."""
    stmt = select(Job).where(Job.user_id == user_id)
    if job_type is not None:
        stmt = stmt.where(Job.job_type == job_type)
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc())
    return list((await session.execute(stmt)).scalars().all())


async def create_job_attempt(
    session: AsyncSession,
    *,
    job_id: UUID,
    attempt_number: int,
    worker_id: str | None,
    lease_token: str | None,
    started_at: datetime,
    expires_at: datetime,
) -> JobAttempt:
    """Insert a `running` execution attempt row inside the active transaction."""
    attempt = JobAttempt(
        job_id=job_id,
        attempt_number=attempt_number,
        worker_id=worker_id,
        lease_token=lease_token,
        status="running",
        started_at=started_at,
        expires_at=expires_at,
        result_summary={},
    )
    session.add(attempt)
    await session.flush()
    return attempt


async def _next_attempt_number(session: AsyncSession, *, job_id: UUID) -> int:
    stmt = select(func.coalesce(func.max(JobAttempt.attempt_number), 0)).where(
        JobAttempt.job_id == job_id
    )
    current_max = int((await session.execute(stmt)).scalar_one())
    return current_max + 1


async def finish_job_attempt(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    lease_token: str | None,
    status: str,
    finished_at: datetime,
    external_ref: str | None,
    error_code: str | None,
    result_summary: dict[str, Any],
) -> JobAttempt | None:
    """Conditionally finalize an open `running` attempt row."""
    conditions = [
        JobAttempt.id == attempt_id,
        JobAttempt.status == "running",
    ]
    if lease_token is not None:
        conditions.append(JobAttempt.lease_token == lease_token)
    stmt = (
        update(JobAttempt)
        .where(*conditions)
        .values(
            status=status,
            finished_at=finished_at,
            external_ref=external_ref,
            error_code=error_code,
            result_summary=dict(result_summary),
        )
        .returning(JobAttempt)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def finish_running_attempts_for_lease(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    status: str,
    finished_at: datetime,
    external_ref: str | None = None,
    error_code: str | None = None,
    result_summary: dict[str, Any] | None = None,
) -> None:
    """Close any open `running` attempt rows matching `(job_id, lease_token)`."""
    stmt = (
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.lease_token == lease_token,
            JobAttempt.status == "running",
        )
        .values(
            status=status,
            finished_at=finished_at,
            external_ref=external_ref,
            error_code=error_code,
            result_summary=dict(result_summary) if result_summary is not None else {},
        )
    )
    await session.execute(stmt)


async def list_job_attempts(session: AsyncSession, *, job_id: UUID) -> list[JobAttempt]:
    """List execution attempt summaries for a job in ascending attempt order."""
    stmt = (
        select(JobAttempt)
        .where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.attempt_number.asc(), JobAttempt.created_at.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def claim_due_jobs(
    session: AsyncSession,
    *,
    job_types: Sequence[str] | None,
    limit: int,
    lease_token_factory: Callable[[], str],
    lease_expires_at: datetime,
    current_time: datetime,
    worker_id: str,
    attempt_retention_days: int,
) -> list[tuple[Job, JobAttempt]]:
    """Claim due or lease-expired jobs with `FOR UPDATE SKIP LOCKED` and open attempts.

    - Reconciling polls do NOT consume `job.attempt_count` retry budget; each execution still
      records an increasing `job_attempts.attempt_number`.
    - Expired `running` jobs with no `external_ref` that have already reached `max_attempts`
      converge directly to terminal `failed` rather than reclaiming indefinitely.
    """
    due_cond = and_(
        Job.status.in_(("pending", "retry_wait", "reconciling")),
        Job.next_run_at <= current_time,
    )
    expired_running_cond = and_(
        Job.status == "running",
        Job.lease_expires_at.is_not(None),
        Job.lease_expires_at <= current_time,
    )
    stmt = select(Job).where(or_(due_cond, expired_running_cond))
    if job_types:
        stmt = stmt.where(Job.job_type.in_(tuple(job_types)))
    stmt = (
        stmt.order_by(Job.next_run_at.asc(), Job.created_at.asc(), Job.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    locked_jobs = list((await session.execute(stmt)).scalars().all())
    if not locked_jobs:
        return []

    claimed: list[tuple[Job, JobAttempt]] = []
    attempt_expires_at = current_time + timedelta(days=attempt_retention_days)
    for job in locked_jobs:
        prior_status = job.status
        if prior_status == "running" and job.lease_token is not None:
            await finish_running_attempts_for_lease(
                session,
                job_id=job.id,
                lease_token=job.lease_token,
                status="lease_lost",
                finished_at=current_time,
                external_ref=job.external_ref,
                error_code="job_lease_expired",
                result_summary={"reason": "lease_expired_reclaimed"},
            )
            if job.attempt_count >= job.max_attempts and job.external_ref is None:
                job.status = "failed"
                job.lease_token = None
                job.lease_expires_at = None
                job.finished_at = current_time
                job.last_error_code = "job_lease_lost"
                job.last_error_summary = "job exceeded max_attempts after repeated lease expiry"
                job.updated_at = current_time
                continue

        lease_token = lease_token_factory()
        job.status = "running"
        if prior_status != "reconciling":
            job.attempt_count = job.attempt_count + 1
        job.lease_token = lease_token
        job.lease_expires_at = lease_expires_at
        job.started_at = current_time
        job.updated_at = current_time

        attempt_num = await _next_attempt_number(session, job_id=job.id)
        attempt = await create_job_attempt(
            session,
            job_id=job.id,
            attempt_number=attempt_num,
            worker_id=worker_id,
            lease_token=lease_token,
            started_at=current_time,
            expires_at=attempt_expires_at,
        )
        claimed.append((job, attempt))

    await session.flush()
    return claimed


async def conditional_renew_job_lease(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    current_time: datetime,
    new_lease_expires_at: datetime,
) -> Job | None:
    """Extend a running job's lease only if owned by `lease_token` and not yet expired."""
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == "running",
            Job.lease_token == lease_token,
            or_(Job.lease_expires_at.is_(None), Job.lease_expires_at > current_time),
        )
        .values(
            lease_expires_at=new_lease_expires_at,
            updated_at=current_time,
        )
        .returning(Job)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def conditional_mark_job_dispatched(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    current_time: datetime,
    external_ref: str,
    metadata_json: dict[str, Any],
) -> Job | None:
    """Record pre-dispatch `external_ref` & metadata only while `lease_token` is active."""
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == "running",
            Job.lease_token == lease_token,
            or_(Job.lease_expires_at.is_(None), Job.lease_expires_at > current_time),
        )
        .values(
            external_ref=external_ref,
            metadata_json=dict(metadata_json),
            updated_at=current_time,
        )
        .returning(Job)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def conditional_update_job_outcome(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    current_time: datetime,
    new_status: str,
    external_ref: str | None,
    last_error_code: str | None,
    last_error_summary: str | None,
    finished_at: datetime | None,
    next_run_at: datetime | None,
    metadata_json: dict[str, Any],
) -> Job | None:
    """Conditionally transition a claimed running job guarded by lease token and lease expiry."""
    values: dict[str, Any] = {
        "status": new_status,
        "lease_token": None,
        "lease_expires_at": None,
        "external_ref": external_ref,
        "last_error_code": last_error_code,
        "last_error_summary": last_error_summary,
        "finished_at": finished_at,
        "metadata_json": dict(metadata_json),
        "updated_at": current_time,
    }
    if next_run_at is not None:
        values["next_run_at"] = next_run_at

    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == "running",
            Job.lease_token == lease_token,
            or_(Job.lease_expires_at.is_(None), Job.lease_expires_at > current_time),
        )
        .values(**values)
        .returning(Job)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def conditional_requeue_failed_job(
    session: AsyncSession,
    *,
    job_id: UUID,
    new_max_attempts: int,
    new_attempt_count: int,
    next_run_at: datetime,
    current_time: datetime,
    metadata_json: dict[str, Any],
) -> Job | None:
    """Conditionally requeue a terminal `failed` job back to `pending` for controlled retry."""
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == "failed")
        .values(
            status="pending",
            attempt_count=new_attempt_count,
            max_attempts=new_max_attempts,
            lease_token=None,
            lease_expires_at=None,
            finished_at=None,
            next_run_at=next_run_at,
            metadata_json=dict(metadata_json),
            updated_at=current_time,
        )
        .returning(Job)
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_memory_jobs_view(
    session: AsyncSession,
    *,
    user_id: UUID | None = None,
    status: str | None = None,
    job_type: str | None = None,
    conversation_id: UUID | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Query the read-only `memory_jobs` PostgreSQL view via SQLAlchemy Core `select()`."""
    stmt = select(*_MEMORY_JOBS_VIEW.c)
    if user_id is not None:
        stmt = stmt.where(_MEMORY_JOBS_VIEW.c.user_id == user_id)
    if status is not None:
        stmt = stmt.where(_MEMORY_JOBS_VIEW.c.status == status)
    if job_type is not None:
        stmt = stmt.where(_MEMORY_JOBS_VIEW.c.job_type == job_type)
    if conversation_id is not None:
        stmt = stmt.where(_MEMORY_JOBS_VIEW.c.conversation_id == str(conversation_id))
    stmt = stmt.order_by(
        _MEMORY_JOBS_VIEW.c.created_at.desc(),
        _MEMORY_JOBS_VIEW.c.id.desc(),
    ).limit(limit)

    result = await session.execute(stmt)
    return [dict(row) for row in result.mappings().all()]


async def get_job_status_aggregates(
    session: AsyncSession,
    *,
    job_type: str | None = None,
) -> tuple[dict[str, int], datetime | None]:
    """Return status counts and oldest `next_run_at` among waiting/reconciling jobs."""
    count_stmt = select(Job.status, func.count(Job.id)).group_by(Job.status)
    due_stmt = select(func.min(Job.next_run_at)).where(
        Job.status.in_(("pending", "retry_wait", "reconciling"))
    )
    if job_type is not None:
        count_stmt = count_stmt.where(Job.job_type == job_type)
        due_stmt = due_stmt.where(Job.job_type == job_type)

    counts: dict[str, int] = {
        str(status): int(total) for status, total in (await session.execute(count_stmt)).all()
    }
    oldest_due = (await session.execute(due_stmt)).scalar_one_or_none()
    return counts, normalize_utc(oldest_due) if oldest_due is not None else None
