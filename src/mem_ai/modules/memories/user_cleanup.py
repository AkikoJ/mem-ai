"""User-deletion coordination over the shared durable whole-user memory clear barrier."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ForbiddenError
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobHandlerOutcome, JobRecord, JobStatus
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    AUDIT_RECONCILING_DEFER_SECONDS,
    JOB_TYPE_MEMORY_CLEAR,
    JOB_TYPE_MEMORY_DELETE,
    WRITE_JOB_TYPES,
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
    fail_superseded_explicit_operation_in_session,
    resolve_current_time,
)
from mem_ai.modules.memories.clearing import prepare_clear_scope_in_session
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import DeletionAuditEvent
from mem_ai.modules.users import service as users_service

_MEMORY_JOB_TYPES = (*WRITE_JOB_TYPES, JOB_TYPE_MEMORY_DELETE, JOB_TYPE_MEMORY_CLEAR)
_ALL_JOB_STATUSES: tuple[JobStatus, ...] = (
    "pending",
    "running",
    "retry_wait",
    "reconciling",
    "succeeded",
    "failed",
    "cancelled",
)
_ACTIVE_JOB_STATUSES: tuple[JobStatus, ...] = (
    "pending",
    "retry_wait",
    "running",
    "reconciling",
)


@dataclass(frozen=True, slots=True)
class UserMemoryCleanupPlan:
    """Persist provider-to-operation bindings on the owning user cleanup operation."""

    provider_operations: dict[str, str]
    audit_events: tuple[DeletionAuditEvent, ...] = ()


@dataclass(frozen=True, slots=True)
class UserMemoryCleanupProgress:
    """Report remote verification state, retry requirements, and settlement readiness."""

    phase: str
    remote_complete: bool = False
    pending_jobs: int = 0
    retry_required: bool = False
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class UserMemoryClearBatch:
    """One bounded in-flight scan, with a durable cursor owned by the clear job."""

    phase: str
    writes: tuple[JobRecord, ...] = ()
    ready: bool = False


async def defer_write_to_user_cleanup(
    session_factory: async_sessionmaker[AsyncSession], *, job: JobRecord, outcome: JobHandlerOutcome
) -> JobHandlerOutcome:
    """Let deletion verify dispatched writes without competing autonomous poll leases."""
    if (
        outcome.outcome != "reconciling"
        or job.user_id is None
        or (outcome.external_ref or job.external_ref) is None
    ):
        return outcome
    async with session_factory() as session:
        try:
            await users_service.ensure_user_active_in_session(session, user_id=job.user_id)
        except ForbiddenError as exc:
            if exc.code != "user_deleting":
                return outcome
        else:
            return outcome
    return outcome.model_copy(update={"retry_after_seconds": AUDIT_RECONCILING_DEFER_SECONDS})


async def prepare_user_memory_clear_batch_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    after_job_id: UUID | None,
    now: datetime,
    batch_size: int = 50,
) -> UserMemoryClearBatch:
    """Await live leases, cancel a queued batch, and read one stable provider-write page."""
    metadata = {"provider_id": provider_id}
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        statuses=("running",),
        metadata_match=metadata,
        active_lease_at=now,
    ):
        return UserMemoryClearBatch(phase="await_memory_leases")
    await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        allowed_statuses=("pending", "retry_wait"),
        predicate=lambda _job: True,
        metadata_match=metadata,
        limit=max(1, min(100, batch_size)),
        skip_locked=True,
        error_code="memory_scope_clearing",
        error_summary="queued write cancelled by user cleanup barrier",
        now=now,
    )
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        statuses=("pending", "retry_wait"),
        metadata_match=metadata,
    ):
        return UserMemoryClearBatch(phase="cancel_queued_writes")
    jobs = await jobs_service.list_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        statuses=("running", "reconciling"),
        metadata_match=metadata,
        after_job_id=after_job_id,
        order_by_job_id=True,
        limit=max(1, min(100, batch_size)),
    )
    return UserMemoryClearBatch(phase="scan_memory_writes", writes=tuple(jobs), ready=not jobs)


async def _list_provider_ids(session: AsyncSession, user_id: UUID) -> set[str]:
    providers = set(await repository.list_provider_ids(session, user_id=user_id))
    providers.update(
        await jobs_service.list_user_job_metadata_values_in_session(
            session, user_id=user_id, job_types=_MEMORY_JOB_TYPES, field="provider_id"
        )
    )
    return providers


def _job_provider(job: JobRecord) -> str | None:
    value = job.metadata_json.get("provider_id")
    return value if isinstance(value, str) and value else None


async def begin_user_memory_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    operation_id: UUID,
    provider_id: str | None = None,
    max_attempts: int = 5,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    now: datetime | None = None,
) -> UserMemoryCleanupPlan:
    """Install or reuse all provider clears in the user's deleting transaction.

    The caller has irreversibly marked the user deleting. Persist the returned
    bindings in the same transaction; export the audit events after commit.
    No provider request is made here, including when no writes are in flight.
    """
    current_time = await resolve_current_time(session, now)
    providers = await _list_provider_ids(session, user_id)
    if provider_id is not None:
        providers.add(provider_id)
    bindings: dict[str, str] = {}
    events: list[DeletionAuditEvent] = []
    marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
    for target in sorted(providers):
        prepared = await prepare_clear_scope_in_session(
            session,
            user_id=user_id,
            provider_id=target,
            marker=marker,
            max_attempts=max_attempts,
            current_time=current_time,
            force_async=True,
            user_cleanup_operation_id=operation_id,
        )
        operation = prepared.operation
        if operation is None and prepared.shortcut is not None:
            operation = prepared.shortcut.operation
        if operation is None:
            raise RuntimeError("user memory cleanup did not retain its clear operation")
        bindings[target] = str(operation.id)
        events.extend(prepared.audit_events)
    return UserMemoryCleanupPlan(provider_operations=bindings, audit_events=tuple(events))


async def _settle_superseded_operations(
    session: AsyncSession,
    *,
    user_id: UUID,
    jobs: list[JobRecord],
    provider_operations: dict[str, str],
    now: datetime,
) -> bool:
    """A verified full wipe confirms old delete/clear intents and invalidates old creates."""
    if any(job.has_active_lease(now) for job in jobs):
        return False
    await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=user_id,
        job_types=_MEMORY_JOB_TYPES,
        allowed_statuses=_ACTIVE_JOB_STATUSES,
        predicate=lambda job: (
            _job_provider(job) in provider_operations and not job.has_active_lease(now)
        ),
        error_code="memory_scope_cleared",
        error_summary="memory operation superseded by verified user cleanup",
        metadata_patch={"superseded_by_user_cleanup": True},
        job_ids=[job.id for job in jobs],
        limit=len(jobs),
        now=now,
    )
    for job in jobs:
        provider = _job_provider(job)
        if provider not in provider_operations:
            continue
        assert provider is not None
        clear_id = UUID(provider_operations[provider])
        if job.job_type in WRITE_JOB_TYPES:
            await fail_superseded_explicit_operation_in_session(
                session,
                job=job,
                provider_id=provider,
                clear_operation_id=clear_id,
                error_message="memory creation was superseded by user deletion",
                now=now,
            )
            continue
        raw_operation_id = job.metadata_json.get("operation_id")
        if not isinstance(raw_operation_id, str):
            continue
        try:
            operation_id = UUID(raw_operation_id)
        except ValueError:
            continue
        operation = await operations_service.get_operation_in_session(
            session,
            operation_id=operation_id,
        )
        if operation is None or operation.status in ("succeeded", "failed"):
            continue
        if operation.status == "pending":
            await operations_service.transition_operation_in_session(
                session,
                operation_id=operation_id,
                status="running",
                now=now,
            )
        await operations_service.transition_operation_in_session(
            session,
            operation_id=operation_id,
            status="succeeded",
            progress={"phase": "verified_by_user_clear", "provider_id": provider},
            result_summary={"clear_operation_id": str(clear_id)},
            now=now,
        )
    for job in jobs:
        await jobs_service.patch_job_metadata_in_session(
            session,
            job_id=job.id,
            metadata_patch={"settled_by_user_cleanup": True},
            now=now,
        )
    return True


async def peek_user_memory_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_operations: dict[str, str],
) -> UserMemoryCleanupProgress | None:
    """Inspect child clear operations read-only; return None once every provider succeeded."""
    pending_count = 0
    retry_error: tuple[str, str] | None = None
    unavailable_error: tuple[str, str] | None = None
    pending_error: tuple[str | None, str | None] = (None, None)
    for provider, raw_operation_id in provider_operations.items():
        try:
            operation_id = UUID(raw_operation_id)
        except ValueError:
            return UserMemoryCleanupProgress(
                phase="memory_clear_mapping_invalid",
                pending_jobs=1,
                error_code="memory_clear_mapping_invalid",
                error_message="memory clear operation mapping is invalid",
            )
        operation = await operations_service.get_operation_in_session(
            session,
            operation_id=operation_id,
        )
        if (
            operation is None
            or operation.owner_id != user_id
            or operation.operation_type != "memory_clear"
            or operation.target_id != f"{user_id}:{provider}"
        ):
            pending_count += 1
            continue
        if operation.status == "succeeded":
            continue
        pending_count += 1
        child_phase = operation.progress.get("phase")
        if (
            operation.status == "failed"
            or operation.progress.get("retry_required") is True
            or child_phase == "awaiting_retry"
        ):
            if retry_error is None:
                retry_error = (
                    operation.error_code or "memory_clear_retry_required",
                    operation.error_message or "remote memory cleanup requires controlled retry",
                )
        elif (
            operation.error_code == "memory_provider_mismatch"
            or child_phase == "provider_unavailable"
        ):
            if unavailable_error is None:
                unavailable_error = (
                    "memory_provider_mismatch",
                    operation.error_message or "historical memory provider is unavailable",
                )
        elif pending_error == (None, None) and operation.error_code is not None:
            pending_error = (operation.error_code, operation.error_message)
    if pending_count == 0:
        return None
    if retry_error is not None:
        return UserMemoryCleanupProgress(
            phase="awaiting_retry",
            pending_jobs=pending_count,
            retry_required=True,
            error_code=retry_error[0],
            error_message=retry_error[1],
        )
    if unavailable_error is not None:
        return UserMemoryCleanupProgress(
            phase="provider_unavailable",
            pending_jobs=pending_count,
            error_code=unavailable_error[0],
            error_message=unavailable_error[1],
        )
    return UserMemoryCleanupProgress(
        phase="remote_memories_pending",
        pending_jobs=pending_count,
        error_code=pending_error[0],
        error_message=pending_error[1],
    )


async def inspect_user_memory_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_operations: dict[str, str],
    batch_size: int = 50,
    now: datetime | None = None,
) -> UserMemoryCleanupProgress:
    """Prove every provider cleared and every prior memory task can safely be settled.

    The committed deleting state prevents new dispatch under the active-user
    policy. Live leases are awaited even if a clear operation is already succeeded.
    Failed or missing child operations never count as remote confirmation.
    """
    current_time = await resolve_current_time(session, now)
    providers = await _list_provider_ids(session, user_id)
    if providers - provider_operations.keys():
        return UserMemoryCleanupProgress(phase="memory_provider_mapping_missing")
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=_MEMORY_JOB_TYPES,
        statuses=("running",),
        active_lease_at=current_time,
    ):
        return UserMemoryCleanupProgress(phase="await_memory_leases", pending_jobs=1)
    pending_remote = await peek_user_memory_cleanup_in_session(
        session, user_id=user_id, provider_operations=provider_operations
    )
    if pending_remote is not None:
        return pending_remote
    marker = {"settled_by_user_cleanup": True}
    jobs = await jobs_service.list_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=_MEMORY_JOB_TYPES,
        statuses=_ALL_JOB_STATUSES,
        metadata_exclude=marker,
        for_update=True,
        skip_locked=True,
        limit=max(1, min(100, batch_size)),
    )
    if any(_job_provider(job) not in provider_operations for job in jobs):
        return UserMemoryCleanupProgress(phase="memory_provider_mapping_missing")
    # The first existence check is only a fast path: a worker may have claimed
    # a task before this batch acquired its row locks. Recheck after locking;
    # these rows cannot be claimed again until the caller commits settlement.
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=_MEMORY_JOB_TYPES,
        statuses=("running",),
        active_lease_at=current_time,
    ):
        return UserMemoryCleanupProgress(phase="await_memory_leases", pending_jobs=1)
    settled = await _settle_superseded_operations(
        session,
        user_id=user_id,
        jobs=jobs,
        provider_operations=provider_operations,
        now=current_time,
    )
    if not settled:
        return UserMemoryCleanupProgress(phase="await_memory_leases", pending_jobs=1)
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=_MEMORY_JOB_TYPES,
        statuses=_ALL_JOB_STATUSES,
        metadata_exclude=marker,
    ):
        return UserMemoryCleanupProgress(phase="settling_memory_operations", pending_jobs=1)
    return UserMemoryCleanupProgress(phase="remote_memories_verified", remote_complete=True)


async def cleanup_user_memory_snapshots_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    batch_size: int = 50,
) -> bool:
    """Remove local cached pagination snapshots after provider barriers are persisted."""
    return await repository.delete_user_list_snapshots(
        session, user_id=user_id, batch_size=batch_size
    )


async def finalize_user_memory_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_operations: dict[str, str],
    batch_size: int = 50,
    now: datetime | None = None,
) -> bool:
    """Drop retained mappings only after rechecking full remote confirmation in the final Tx."""
    progress = await inspect_user_memory_cleanup_in_session(
        session,
        user_id=user_id,
        provider_operations=provider_operations,
        batch_size=batch_size,
        now=now,
    )
    if not progress.remote_complete:
        return False
    return await repository.delete_user_memory_state(
        session, user_id=user_id, batch_size=batch_size
    )
