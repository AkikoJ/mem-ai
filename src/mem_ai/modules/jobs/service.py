"""Public transaction-participating, Worker claiming, backoff, and query entry points for `jobs`."""

from __future__ import annotations

import logging
import random
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ConflictError, NotFoundError
from mem_ai.modules.jobs import repository
from mem_ai.modules.jobs.cleanup import (
    cleanup_user_jobs_in_session as cleanup_user_jobs_in_session,
)
from mem_ai.modules.jobs.cleanup import (
    has_user_jobs_in_session as has_user_jobs_in_session,
)
from mem_ai.modules.jobs.cleanup import (
    list_user_job_metadata_values_in_session as list_user_job_metadata_values_in_session,
)
from mem_ai.modules.jobs.cleanup import (
    list_user_jobs_in_session as list_user_jobs_in_session,
)
from mem_ai.modules.jobs.cleanup import (
    patch_job_metadata_in_session as patch_job_metadata_in_session,
)
from mem_ai.modules.jobs.cleanup import (
    sanitize_summary_dict as _sanitize_summary_dict,
)
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.jobs.schemas import (
    ClaimedJob,
    JobAttemptRecord,
    JobBackoffPolicy,
    JobBeforeCommitHook,
    JobHandlerOutcome,
    JobObservabilitySnapshot,
    JobRecord,
    JobStatus,
    MemoryJobViewRecord,
    ResolvedJobTransition,
)

logger = logging.getLogger("mem_ai.jobs")

DEFAULT_SYNC_JOB_LEASE_SECONDS = 60
DEFAULT_JOB_ATTEMPT_RETENTION_DAYS = 30
_MAX_ERROR_SUMMARY_CHARS = 500


def _sanitize_error_summary(summary: str | None) -> str | None:
    if summary is None:
        return None
    cleaned = " ".join(summary.split()).strip()
    if not cleaned:
        return None
    return cleaned[:_MAX_ERROR_SUMMARY_CHARS]


def _merge_sanitized_metadata(
    existing_metadata: dict[str, Any],
    metadata_patch: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing_metadata)
    if metadata_patch:
        merged.update(_sanitize_summary_dict(metadata_patch))
    return merged


def _new_lease_token() -> str:
    return f"lease_{uuid4().hex}"


async def _resolve_current_time(session: AsyncSession, now: datetime | None) -> datetime:
    if now is None:
        return await repository.get_database_now(session)
    return repository.normalize_utc(now)


async def _load_claimed_job_for_update_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
) -> Job:
    """Lock `job_id` and confirm it is `running` under `lease_token`, else raise 409."""
    existing = await repository.get_job_by_id(session, job_id=job_id, for_update=True)
    if existing is None or existing.lease_token != lease_token or existing.status != "running":
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer valid",
        )
    return existing


def compute_backoff_delay_seconds(
    *,
    attempt_number: int,
    policy: JobBackoffPolicy,
    random_fn: Callable[[], float] | None = None,
) -> float:
    """Compute exponential backoff with bounded symmetric jitter."""
    exponent = max(0, attempt_number - 1)
    base = min(policy.max_delay_seconds, policy.base_delay_seconds * (2.0**exponent))
    if policy.jitter_ratio <= 0.0:
        return base

    rng = random_fn if random_fn is not None else random.random
    sample = max(0.0, min(1.0, float(rng())))
    jitter_factor = 1.0 + policy.jitter_ratio * (2.0 * sample - 1.0)
    return max(0.0, min(policy.max_delay_seconds, base * jitter_factor))


def resolve_job_transition(
    *,
    attempt_count: int,
    max_attempts: int,
    outcome: JobHandlerOutcome,
    backoff_policy: JobBackoffPolicy,
    current_time: datetime,
    random_fn: Callable[[], float] | None = None,
) -> ResolvedJobTransition:
    """Map a handler outcome and attempt count to the next durable job and attempt statuses."""
    if outcome.outcome == "succeeded":
        return ResolvedJobTransition(
            job_status="succeeded",
            attempt_status="succeeded",
            finished_at=current_time,
            next_run_at=None,
        )

    if outcome.outcome == "cancelled":
        return ResolvedJobTransition(
            job_status="cancelled",
            attempt_status="cancelled",
            finished_at=current_time,
            next_run_at=None,
        )

    if outcome.outcome == "failed":
        return ResolvedJobTransition(
            job_status="failed",
            attempt_status="failed",
            finished_at=current_time,
            next_run_at=None,
        )

    if outcome.outcome == "reconciling":
        delay = (
            outcome.retry_after_seconds
            if outcome.retry_after_seconds is not None
            else backoff_policy.reconcile_delay_seconds
        )
        return ResolvedJobTransition(
            job_status="reconciling",
            attempt_status="reconciling",
            finished_at=None,
            next_run_at=current_time + timedelta(seconds=delay),
        )

    # `outcome.outcome == "retryable"`: honor `max_attempts` exhaustion boundary.
    if attempt_count >= max_attempts:
        return ResolvedJobTransition(
            job_status="failed",
            attempt_status="failed",
            finished_at=current_time,
            next_run_at=None,
        )

    delay = (
        outcome.retry_after_seconds
        if outcome.retry_after_seconds is not None
        else compute_backoff_delay_seconds(
            attempt_number=attempt_count,
            policy=backoff_policy,
            random_fn=random_fn,
        )
    )
    return ResolvedJobTransition(
        job_status="retry_wait",
        attempt_status="retry_wait",
        finished_at=None,
        next_run_at=current_time + timedelta(seconds=delay),
    )


async def enqueue_job_in_session(
    session: AsyncSession,
    *,
    job_type: str,
    business_id: str | None,
    user_id: UUID | None,
    dedupe_key: str | None,
    metadata_json: dict[str, Any],
    max_attempts: int = 5,
    next_run_at: datetime | None = None,
    now: datetime | None = None,
) -> tuple[JobRecord, bool]:
    """Idempotently enqueue a pending durable job inside the caller's transaction."""
    current_time = await _resolve_current_time(session, now)
    scheduled_at = (
        repository.normalize_utc(next_run_at) if next_run_at is not None else current_time
    )
    sanitized_metadata = _sanitize_summary_dict(metadata_json)
    job, created = await repository.enqueue_job_idempotent(
        session,
        job_type=job_type,
        business_id=business_id,
        user_id=user_id,
        dedupe_key=dedupe_key,
        max_attempts=max_attempts,
        next_run_at=scheduled_at,
        metadata_json=sanitized_metadata,
    )
    return JobRecord.model_validate(job), created


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
    attempt_retention_days: int = DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    worker_id: str = "api-sync",
    now: datetime | None = None,
) -> JobRecord:
    """Register a job in `running` state with an ownership lease inside caller's transaction."""
    current_time = await _resolve_current_time(session, now)
    lease_token = _new_lease_token()
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
        metadata_json=_sanitize_summary_dict(metadata_json),
    )
    await repository.create_job_attempt(
        session,
        job_id=job.id,
        attempt_number=1,
        worker_id=worker_id,
        lease_token=lease_token,
        started_at=current_time,
        expires_at=current_time + timedelta(days=attempt_retention_days),
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
    attempt_retention_days: int = DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> JobRecord:
    """Conditionally record a claimed job's execution outcome inside caller's transaction."""
    current_time = await _resolve_current_time(session, now)
    existing = await _load_claimed_job_for_update_in_session(
        session,
        job_id=job_id,
        lease_token=lease_token,
    )
    merged_metadata = _merge_sanitized_metadata(existing.metadata_json, metadata_patch)
    is_terminal = status in ("succeeded", "failed", "cancelled")
    resolved_ext_ref = external_ref if external_ref is not None else existing.external_ref
    cleaned_summary = _sanitize_error_summary(error_summary)
    updated = await repository.conditional_update_job_outcome(
        session,
        job_id=job_id,
        lease_token=lease_token,
        current_time=current_time,
        new_status=status,
        external_ref=resolved_ext_ref,
        last_error_code=error_code,
        last_error_summary=cleaned_summary,
        finished_at=current_time if is_terminal else None,
        next_run_at=next_run_at,
        metadata_json=merged_metadata,
    )
    if updated is None:
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer valid",
        )

    attempt_status = status if status != "pending" else "retry_wait"
    await repository.finish_running_attempts_for_lease(
        session,
        job_id=job_id,
        lease_token=lease_token,
        status=attempt_status,
        finished_at=current_time,
        external_ref=resolved_ext_ref,
        error_code=error_code,
        result_summary={"status": status},
        expires_at=current_time + timedelta(days=attempt_retention_days),
    )
    return JobRecord.model_validate(updated)


async def claim_due_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    worker_id: str,
    job_types: Sequence[str] | None = None,
    limit: int = 10,
    lease_ttl_seconds: float = 60.0,
    attempt_retention_days: int = DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> list[ClaimedJob]:
    """Claim due or lease-expired jobs via `SKIP LOCKED` in a short transaction."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if lease_ttl_seconds <= 0:
        raise ValueError("lease_ttl_seconds must be positive")

    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        lease_expires_at = current_time + timedelta(seconds=lease_ttl_seconds)
        pairs = await repository.claim_due_jobs(
            session,
            job_types=job_types,
            limit=limit,
            lease_token_factory=_new_lease_token,
            lease_expires_at=lease_expires_at,
            current_time=current_time,
            worker_id=worker_id,
            attempt_retention_days=attempt_retention_days,
        )
        return [
            ClaimedJob(
                job=JobRecord.model_validate(job),
                attempt_id=attempt.id,
                attempt_number=attempt.attempt_number,
                lease_token=job.lease_token or "",
                worker_id=worker_id,
            )
            for job, attempt in pairs
        ]


async def renew_job_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    lease_token: str,
    lease_ttl_seconds: float = 60.0,
    now: datetime | None = None,
) -> JobRecord:
    """Conditionally renew a running job's lease in a short transaction."""
    if lease_ttl_seconds <= 0:
        raise ValueError("lease_ttl_seconds must be positive")

    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        new_expires_at = current_time + timedelta(seconds=lease_ttl_seconds)
        updated = await repository.conditional_renew_job_lease(
            session,
            job_id=job_id,
            lease_token=lease_token,
            current_time=current_time,
            new_lease_expires_at=new_expires_at,
        )
        if updated is None:
            raise ConflictError(
                code="job_lease_lost",
                message="job execution lease is no longer active or has expired",
            )
        return JobRecord.model_validate(updated)


async def mark_job_dispatched_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    lease_token: str,
    external_ref: str,
    metadata_patch: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> JobRecord:
    """Conditionally record pre-dispatch external correlation inside caller's transaction."""
    current_time = await _resolve_current_time(session, now)
    existing = await _load_claimed_job_for_update_in_session(
        session,
        job_id=job_id,
        lease_token=lease_token,
    )
    merged_metadata = _merge_sanitized_metadata(existing.metadata_json, metadata_patch)
    updated = await repository.conditional_mark_job_dispatched(
        session,
        job_id=job_id,
        lease_token=lease_token,
        current_time=current_time,
        external_ref=external_ref,
        metadata_json=merged_metadata,
    )
    if updated is None:
        raise ConflictError(
            code="job_lease_lost",
            message="job execution lease is no longer valid",
        )
    return JobRecord.model_validate(updated)


async def record_job_outcome(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    attempt_id: UUID,
    lease_token: str,
    outcome: JobHandlerOutcome,
    backoff_policy: JobBackoffPolicy | None = None,
    before_commit_hook: JobBeforeCommitHook | None = None,
    random_fn: Callable[[], float] | None = None,
    attempt_retention_days: int = DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> JobRecord | None:
    """Conditionally commit a handler outcome and optional atomic hook guarded by lease token."""
    policy = backoff_policy if backoff_policy is not None else JobBackoffPolicy()
    sanitized_summary = _sanitize_summary_dict(outcome.result_summary)
    cleaned_error_summary = _sanitize_error_summary(outcome.error_summary)
    effective_hook = before_commit_hook if before_commit_hook is not None else outcome.before_commit

    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        existing = await repository.get_job_by_id(session, job_id=job_id, for_update=True)
        if existing is None:
            return None

        attempt_expires_at = current_time + timedelta(days=attempt_retention_days)
        if existing.status != "running" or existing.lease_token != lease_token:
            await repository.finish_job_attempt(
                session,
                attempt_id=attempt_id,
                lease_token=lease_token,
                status="lease_lost",
                finished_at=current_time,
                external_ref=outcome.external_ref or existing.external_ref,
                error_code="job_lease_lost",
                result_summary={"reason": "lease_lost_before_commit"},
                expires_at=attempt_expires_at,
            )
            return None

        transition = resolve_job_transition(
            attempt_count=existing.attempt_count,
            max_attempts=existing.max_attempts,
            outcome=outcome,
            backoff_policy=policy,
            current_time=current_time,
            random_fn=random_fn,
        )

        merged_metadata = _merge_sanitized_metadata(existing.metadata_json, outcome.metadata_patch)
        resolved_ext_ref = (
            outcome.external_ref if outcome.external_ref is not None else existing.external_ref
        )

        updated = await repository.conditional_update_job_outcome(
            session,
            job_id=job_id,
            lease_token=lease_token,
            current_time=current_time,
            new_status=transition.job_status,
            external_ref=resolved_ext_ref,
            last_error_code=outcome.error_code,
            last_error_summary=cleaned_error_summary,
            finished_at=transition.finished_at,
            next_run_at=transition.next_run_at,
            metadata_json=merged_metadata,
        )
        if updated is None:
            await repository.finish_job_attempt(
                session,
                attempt_id=attempt_id,
                lease_token=lease_token,
                status="lease_lost",
                finished_at=current_time,
                external_ref=resolved_ext_ref,
                error_code="job_lease_lost",
                result_summary={"reason": "lease_expired_before_commit"},
                expires_at=attempt_expires_at,
            )
            return None

        await repository.finish_job_attempt(
            session,
            attempt_id=attempt_id,
            lease_token=lease_token,
            status=transition.attempt_status,
            finished_at=current_time,
            external_ref=resolved_ext_ref,
            error_code=outcome.error_code,
            result_summary=sanitized_summary,
            expires_at=attempt_expires_at,
        )
        record = JobRecord.model_validate(updated)

        if effective_hook is not None:
            await effective_hook(session, record)

    if outcome.after_commit is not None:
        await outcome.after_commit(record)

    if record.status in ("reconciling", "failed"):
        is_reconciling = record.status == "reconciling"
        fields: dict[str, Any] = {
            "alert": "job_reconciling" if is_reconciling else "job_failed",
            "job_id": str(record.id),
            "job_type": record.job_type,
            "attempt_count": record.attempt_count,
            "external_ref": record.external_ref,
            "error_code": record.last_error_code,
        }
        if is_reconciling:
            fields["next_run_at"] = record.next_run_at.isoformat()
            logger.warning(
                "Durable job entered reconciling state; external outcome requires verification",
                extra={"extra_fields": fields},
            )
        else:
            fields["max_attempts"] = record.max_attempts
            logger.error(
                "Durable job reached terminal failed state",
                extra={"extra_fields": fields},
            )

    return record


async def requeue_failed_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    extra_attempts: int = 1,
    reason: str | None = None,
    now: datetime | None = None,
) -> JobRecord:
    """Controlled operational entry point to retry an exhausted `failed` job after verification."""
    if extra_attempts < 1:
        raise ValueError("extra_attempts must be at least 1")

    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        existing = await repository.get_job_by_id(session, job_id=job_id, for_update=True)
        if existing is None:
            raise NotFoundError(code="job_not_found", message="job not found")
        if existing.status != "failed":
            raise ConflictError(
                code="job_not_failed",
                message="only failed jobs can be requeued for controlled retry",
            )

        new_max_attempts = max(existing.max_attempts, existing.attempt_count) + extra_attempts
        patch: dict[str, Any] = {
            "requeued_at": current_time.isoformat(),
            "requeue_count": int(existing.metadata_json.get("requeue_count", 0)) + 1,
        }
        if reason is not None:
            cleaned_reason = _sanitize_error_summary(reason)
            if cleaned_reason is not None:
                patch["requeue_reason"] = cleaned_reason
        merged_metadata = _merge_sanitized_metadata(existing.metadata_json, patch)

        updated = await repository.conditional_requeue_failed_job(
            session,
            job_id=job_id,
            new_max_attempts=new_max_attempts,
            new_attempt_count=existing.attempt_count,
            next_run_at=current_time,
            current_time=current_time,
            metadata_json=merged_metadata,
        )
        if updated is None:
            raise ConflictError(
                code="job_not_failed",
                message="job status changed concurrently before requeue",
            )
        record = JobRecord.model_validate(updated)

    logger.info(
        "Requeued failed durable job via controlled operational entry",
        extra={
            "extra_fields": {
                "job_id": str(record.id),
                "job_type": record.job_type,
                "attempt_count": record.attempt_count,
                "max_attempts": record.max_attempts,
            }
        },
    )
    return record


async def cancel_matching_jobs_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    job_types: Sequence[str],
    allowed_statuses: Sequence[JobStatus],
    predicate: Callable[[JobRecord], bool],
    error_code: str,
    error_summary: str,
    metadata_patch: dict[str, Any] | None = None,
    now: datetime | None = None,
    metadata_match: dict[str, Any] | None = None,
    metadata_exclude: dict[str, Any] | None = None,
    job_ids: Sequence[UUID] | None = None,
    limit: int | None = None,
    skip_locked: bool = False,
) -> list[JobRecord]:
    """Atomically cancel user jobs in `allowed_statuses` matching `predicate` in caller's Tx."""
    current_time = await _resolve_current_time(session, now)
    cleaned_summary = _sanitize_error_summary(error_summary)
    rows = await repository.list_jobs_for_user_and_types(
        session,
        user_id=user_id,
        job_types=job_types,
        statuses=allowed_statuses,
        for_update=True,
        metadata_match=metadata_match,
        metadata_exclude=metadata_exclude,
        job_ids=job_ids,
        limit=limit,
        skip_locked=skip_locked,
    )
    cancelled: list[JobRecord] = []
    for row in rows:
        record = JobRecord.model_validate(row)
        if not predicate(record):
            continue
        merged_metadata = _merge_sanitized_metadata(row.metadata_json, metadata_patch)
        updated = await repository.conditional_transition_job_by_status(
            session,
            job_id=row.id,
            allowed_from_statuses=allowed_statuses,
            new_status="cancelled",
            current_time=current_time,
            finished_at=current_time,
            next_run_at=None,
            last_error_code=error_code,
            last_error_summary=cleaned_summary,
            metadata_json=merged_metadata,
            clear_lease=True,
        )
        if updated is not None:
            cancelled.append(JobRecord.model_validate(updated))
    return cancelled


async def update_reconciling_job_in_session(
    session: AsyncSession,
    *,
    job_id: UUID,
    allowed_statuses: Sequence[JobStatus] = ("reconciling", "running"),
    error_code: str | None = None,
    error_summary: str | None = None,
    metadata_patch: dict[str, Any] | None = None,
    defer_next_run_seconds: float | None = None,
    now: datetime | None = None,
) -> JobRecord | None:
    """Update metadata/deferral on an existing reconciling/expired job in caller's Tx."""
    current_time = await _resolve_current_time(session, now)
    existing = await repository.get_job_by_id(session, job_id=job_id, for_update=True)
    if existing is None or existing.status not in allowed_statuses:
        return None
    if (
        existing.status == "running"
        and existing.lease_expires_at is not None
        and existing.lease_expires_at > current_time
    ):
        return None
    merged_metadata = _merge_sanitized_metadata(existing.metadata_json, metadata_patch)
    next_run = (
        current_time + timedelta(seconds=defer_next_run_seconds)
        if defer_next_run_seconds is not None
        else None
    )
    updated = await repository.conditional_transition_job_by_status(
        session,
        job_id=job_id,
        allowed_from_statuses=allowed_statuses,
        new_status="reconciling",
        current_time=current_time,
        finished_at=None,
        next_run_at=next_run,
        last_error_code=error_code if error_code is not None else existing.last_error_code,
        last_error_summary=(
            _sanitize_error_summary(error_summary)
            if error_summary is not None
            else existing.last_error_summary
        ),
        metadata_json=merged_metadata,
        clear_lease=True,
    )
    return JobRecord.model_validate(updated) if updated is not None else None


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


async def list_job_attempts(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
) -> list[JobAttemptRecord]:
    """List sanitized attempt summaries for a job ordered by attempt number."""
    async with session_factory() as session:
        rows = await repository.list_job_attempts(session, job_id=job_id)
        return [JobAttemptRecord.model_validate(row) for row in rows]


async def list_memory_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID | None = None,
    status: JobStatus | None = None,
    job_type: str | None = None,
    conversation_id: UUID | None = None,
    limit: int = 50,
) -> list[MemoryJobViewRecord]:
    """Read domain-projected memory write jobs from the read-only `memory_jobs` view."""
    async with session_factory() as session:
        rows = await repository.list_memory_jobs_view(
            session,
            user_id=user_id,
            status=status,
            job_type=job_type,
            conversation_id=conversation_id,
            limit=limit,
        )
        return [MemoryJobViewRecord.model_validate(row) for row in rows]


async def get_job_observability(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_type: str | None = None,
) -> JobObservabilitySnapshot:
    """Return backlog counts, oldest due timestamp, and latest success across durable jobs."""
    async with session_factory() as session:
        counts, oldest_due, last_success = await repository.get_job_status_aggregates(
            session, job_type=job_type
        )
        return JobObservabilitySnapshot(
            status_counts=counts,
            pending_count=counts.get("pending", 0),
            running_count=counts.get("running", 0),
            retry_wait_count=counts.get("retry_wait", 0),
            reconciling_count=counts.get("reconciling", 0),
            failed_count=counts.get("failed", 0),
            oldest_due_at=oldest_due,
            last_success_at=last_success,
        )
