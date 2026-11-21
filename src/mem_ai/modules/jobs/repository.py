"""SQLAlchemy repository for durable job persistence and conditional outcome updates."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.jobs.models import Job


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


async def get_job_by_id(session: AsyncSession, *, job_id: UUID) -> Job | None:
    """Load a job row by primary key."""
    stmt = select(Job).where(Job.id == job_id)
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
