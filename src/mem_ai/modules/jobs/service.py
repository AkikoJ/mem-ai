"""Public transaction-participating and query entry points for unified `jobs`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ConflictError
from mem_ai.modules.jobs import repository
from mem_ai.modules.jobs.schemas import JobRecord, JobStatus

DEFAULT_SYNC_JOB_LEASE_SECONDS = 60


async def register_and_claim_sync_job_in_session(
    session: AsyncSession,
    *,
    job_type: str,
    business_id: str | None,
    user_id: UUID | None,
    dedupe_key: str | None,
    metadata_json: dict[str, Any],
    max_attempts: int = 5,
    lease_duration_seconds: int = DEFAULT_SYNC_JOB_LEASE_SECONDS,
    now: datetime | None = None,
) -> JobRecord:
    """Register a job in `running` state with an ownership lease inside caller's transaction."""
    current_time = now if now is not None else datetime.now(UTC)
    lease_token = f"lease_{uuid4().hex}"
    job = await repository.create_job(
        session,
        job_type=job_type,
        business_id=business_id,
        user_id=user_id,
        dedupe_key=dedupe_key,
        status="running",
        attempt_count=1,
        max_attempts=max_attempts,
        lease_token=lease_token,
        lease_expires_at=current_time + timedelta(seconds=lease_duration_seconds),
        next_run_at=current_time,
        started_at=current_time,
        metadata_json=metadata_json,
    )
    return JobRecord.model_validate(job)


async def complete_job_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    status: JobStatus,
    external_ref: str | None = None,
    error_code: str | None = None,
    error_summary: str | None = None,
    metadata_patch: dict[str, Any] | None = None,
    next_run_at: datetime | None = None,
    now: datetime | None = None,
) -> JobRecord:
    """Conditionally record a claimed job's execution outcome inside caller's transaction."""
    current_time = now if now is not None else datetime.now(UTC)
    existing = await repository.get_job_by_id(session, job_id=job_id)
    if existing is None or existing.lease_token != lease_token or existing.status != "running":
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer valid",
        )

    merged_metadata = dict(existing.metadata_json)
    if metadata_patch:
        merged_metadata.update(metadata_patch)

    is_terminal = status in ("succeeded", "failed", "cancelled")
    updated = await repository.conditional_update_job_outcome(
        session,
        job_id=job_id,
        lease_token=lease_token,
        current_time=current_time,
        new_status=status,
        external_ref=external_ref if external_ref is not None else existing.external_ref,
        last_error_code=error_code,
        last_error_summary=error_summary,
        finished_at=current_time if is_terminal else None,
        next_run_at=next_run_at,
        metadata_json=merged_metadata,
    )
    if updated is None:
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer valid",
        )
    return JobRecord.model_validate(updated)


async def get_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
) -> JobRecord | None:
    """Load a single job record in a short read session."""
    async with session_factory() as session:
        job = await repository.get_job_by_id(session, job_id=job_id)
        return JobRecord.model_validate(job) if job is not None else None


async def list_jobs_for_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    job_type: str | None = None,
) -> list[JobRecord]:
    """List durable jobs owned by `user_id`."""
    async with session_factory() as session:
        rows = await repository.list_jobs_for_user(session, user_id=user_id, job_type=job_type)
        return [JobRecord.model_validate(row) for row in rows]
