"""Memory epoch barrier, tombstone §11.3.1 state machines, and shared convergence primitives."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.contracts.memory_provider import MemoryDeleteResult
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    GatewayTimeoutError,
    NotFoundError,
    ServiceUnavailableError,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobRecord
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.recall import MemoryBarrierState
from mem_ai.modules.memories.schemas import MemoryTombstoneRecord, MemoryTombstoneStatus
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import DeletionAuditEvent, DeletionAuditEventType
from mem_ai.modules.operations.schemas import OperationRecord

logger = logging.getLogger("mem_ai.memories.barrier")

OPERATION_TYPE_MEMORY_DELETE = "memory_delete"
OPERATION_TYPE_MEMORY_CLEAR = "memory_clear"
JOB_TYPE_MEMORY_DELETE = "memory_delete"
JOB_TYPE_MEMORY_CLEAR = "memory_clear"
WRITE_JOB_TYPES = ("memory_turn_write", "memory_explicit_create")

# Explicit Engineering Design §11.3.1 state machines.
ALLOWED_BARRIER_TRANSITIONS: dict[MemoryBarrierState, frozenset[MemoryBarrierState]] = {
    "idle": frozenset({"clearing"}),
    "clearing": frozenset({"clearing", "reconciling", "idle"}),
    "reconciling": frozenset({"reconciling", "idle"}),
}

ALLOWED_TOMBSTONE_TRANSITIONS: dict[MemoryTombstoneStatus, frozenset[MemoryTombstoneStatus]] = {
    "pending": frozenset({"reconciling", "verified"}),
    "reconciling": frozenset({"reconciling", "verified"}),
    "verified": frozenset(),
}

AUDIT_RECONCILING_DEFER_SECONDS = 30.0 * 86400.0


def is_valid_barrier_transition(
    current: MemoryBarrierState,
    target: MemoryBarrierState,
) -> bool:
    """Return True iff `current -> target` is permitted for `memory_scopes.barrier_state`."""
    return target in ALLOWED_BARRIER_TRANSITIONS[current]


def allowed_barrier_source_states(target: MemoryBarrierState) -> tuple[MemoryBarrierState, ...]:
    """Return all barrier states permitted to transition to `target`."""
    return tuple(
        source for source, allowed in ALLOWED_BARRIER_TRANSITIONS.items() if target in allowed
    )


def is_valid_tombstone_transition(
    current: MemoryTombstoneStatus,
    target: MemoryTombstoneStatus,
) -> bool:
    """Return True iff `current -> target` is permitted for `memory_tombstones.status`."""
    return target in ALLOWED_TOMBSTONE_TRANSITIONS[current]


def allowed_tombstone_source_statuses(
    target: MemoryTombstoneStatus,
) -> tuple[MemoryTombstoneStatus, ...]:
    """Return all tombstone statuses permitted to transition to `target`."""
    return tuple(
        source for source, allowed in ALLOWED_TOMBSTONE_TRANSITIONS.items() if target in allowed
    )


@runtime_checkable
class RecalledMemoryReferenceMarker(Protocol):
    """Seam decoupling `memories` deletion from `conversations.message_recalled_memories`."""

    async def mark_memory_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        memory_id: str,
        deleted_at: datetime,
    ) -> int:
        """Mark historical recalled references for one memory as deleted."""
        ...

    async def mark_user_memories_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        max_memory_epoch: int,
        deleted_at: datetime,
    ) -> int:
        """Mark historical recalled references up to `max_memory_epoch` as deleted."""
        ...


class NoopRecalledMemoryReferenceMarker:
    """Default no-op marker used when conversation reference marking is not wired."""

    async def mark_memory_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        memory_id: str,
        deleted_at: datetime,
    ) -> int:
        _ = (session, owner_id, provider_id, memory_id, deleted_at)
        return 0

    async def mark_user_memories_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        max_memory_epoch: int,
        deleted_at: datetime,
    ) -> int:
        _ = (session, owner_id, provider_id, max_memory_epoch, deleted_at)
        return 0


NON_RETRYABLE_MEMORY_PROVIDER_ERRORS: frozenset[str] = frozenset(
    {"memory_provider_auth_error", "memory_provider_bad_request", "memory_not_found"}
)


def is_retryable_memory_provider_error(error_code: str | None) -> bool:
    """Return True when a provider error code represents a transient/retryable failure."""
    normalized = error_code or "memory_provider_error"
    return normalized not in NON_RETRYABLE_MEMORY_PROVIDER_ERRORS


def map_memory_provider_error(error_code: str | None, *, default_message: str) -> AppError:
    """Translate a stable memory provider error code into an HTTP-mapped `AppError`."""
    normalized = error_code or "memory_provider_error"
    if normalized == "memory_not_found":
        return NotFoundError(code="memory_not_found", message="memory not found")
    if normalized in ("memory_provider_unavailable", "memory_provider_rate_limited"):
        return ServiceUnavailableError(
            code=normalized,
            message="memory provider is temporarily unavailable",
        )
    if normalized == "memory_provider_timeout":
        return GatewayTimeoutError(
            code="memory_provider_timeout",
            message="memory provider request timed out",
        )
    return BadGatewayError(code="memory_provider_error", message=default_message)


def make_delete_audit_event(
    event_type: DeletionAuditEventType,
    *,
    user_id: UUID,
    operation_id: UUID,
    provider_id: str,
    memory_id: str,
    status: str,
    memory_epoch: int | None = None,
    occurred_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> DeletionAuditEvent:
    """Build a standardized `memory_delete` `DeletionAuditEvent`."""
    return make_memory_audit_event(
        event_type,
        user_id=user_id,
        operation_id=operation_id,
        operation_type=OPERATION_TYPE_MEMORY_DELETE,
        provider_id=provider_id,
        target_type="memory",
        target_id=memory_id,
        memory_epoch=memory_epoch,
        status=status,
        occurred_at=occurred_at,
        metadata=metadata,
    )


def make_clear_audit_event(
    event_type: DeletionAuditEventType,
    *,
    user_id: UUID,
    operation_id: UUID,
    provider_id: str,
    memory_epoch: int,
    barrier_state: str,
    status: str | None = None,
    occurred_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> DeletionAuditEvent:
    """Build a standardized `memory_clear` `DeletionAuditEvent`."""
    return make_memory_audit_event(
        event_type,
        user_id=user_id,
        operation_id=operation_id,
        operation_type=OPERATION_TYPE_MEMORY_CLEAR,
        provider_id=provider_id,
        target_type="memory_scope",
        target_id=f"{user_id}:{provider_id}",
        memory_epoch=memory_epoch,
        barrier_state=barrier_state,
        status=status,
        occurred_at=occurred_at,
        metadata=metadata,
    )


def make_memory_audit_event(
    event_type: DeletionAuditEventType,
    *,
    user_id: UUID,
    operation_id: UUID,
    operation_type: str,
    provider_id: str,
    target_type: str,
    target_id: str,
    memory_epoch: int | None = None,
    status: str | None = None,
    barrier_state: str | None = None,
    occurred_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> DeletionAuditEvent:
    """Build a standardized deletion/epoch `DeletionAuditEvent` for independent archiving."""
    kwargs: dict[str, Any] = {
        "event_type": event_type,
        "user_id": user_id,
        "operation_id": operation_id,
        "operation_type": operation_type,
        "provider_id": provider_id,
        "target_type": target_type,
        "target_id": target_id,
        "memory_epoch": memory_epoch,
        "status": status,
        "barrier_state": barrier_state,
        "metadata": metadata or {},
    }
    if occurred_at is not None:
        kwargs["occurred_at"] = occurred_at
    return DeletionAuditEvent(**kwargs)


async def invoke_provider_delete_call(
    call: Callable[[], Awaitable[MemoryDeleteResult]],
    *,
    default_external_ref: str,
    provider_id: str,
    operation_id: UUID,
    job_id: UUID,
    action_label: str,
) -> MemoryDeleteResult:
    """Invoke a `MemoryProvider` delete/clear call outside DB tx, normalizing errors."""
    try:
        return await call()
    except GatewayTimeoutError as exc:
        return MemoryDeleteResult(
            status="reconciling",
            external_ref=default_external_ref,
            error_code=exc.code,
        )
    except AppError as exc:
        return MemoryDeleteResult(
            status="failed",
            external_ref=default_external_ref,
            error_code=exc.code,
        )
    except Exception:
        logger.exception(
            "Unexpected exception while executing provider %s",
            action_label,
            extra={
                "extra_fields": {
                    "provider": provider_id,
                    "operation_id": str(operation_id),
                    "job_id": str(job_id),
                }
            },
        )
        return MemoryDeleteResult(
            status="failed",
            external_ref=default_external_ref,
            error_code="memory_provider_error",
        )


def _extract_job_epoch(metadata: dict[str, Any]) -> int | None:
    raw = metadata.get("memory_epoch")
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def is_old_epoch_write_job(job: JobRecord, *, provider_id: str, max_epoch: int) -> bool:
    """Return True if `job` belongs to `provider_id` at `memory_epoch <= max_epoch`."""
    if job.job_type not in WRITE_JOB_TYPES:
        return False
    if job.metadata_json.get("provider_id") != provider_id:
        return False
    epoch = _extract_job_epoch(job.metadata_json)
    return epoch is not None and epoch <= max_epoch


def is_write_job_already_audited_for_clear(job: JobRecord) -> bool:
    """Return True if this write job was already retained as audited by a prior completed clear."""
    return bool(job.metadata_json.get("superseded_by_clear_operation_id"))


def is_write_job_settled_for_clear(job: JobRecord) -> bool:
    """Return True if an old-epoch write job has stabilized upstream and awaits final clear."""
    if job.status in ("succeeded", "failed", "cancelled"):
        return True
    if job.metadata_json.get("dispatch_state") == "confirmed_during_clear":
        return True
    if job.status not in ("reconciling", "running"):
        return False
    return (
        job.job_type == "memory_explicit_create"
        and job.last_error_code == "memory_scope_clearing"
        and job.metadata_json.get("memory_id") is not None
    )


async def cancel_queued_and_list_unsettled_writes_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    max_epoch: int,
    cancel_summary: str,
    now: datetime,
) -> tuple[list[JobRecord], list[JobRecord]]:
    """Cancel queued old-epoch write jobs and return `(cancelled_queued, unsettled_inflight)`."""
    cancelled_queued = await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        allowed_statuses=("pending", "retry_wait"),
        predicate=lambda job: is_old_epoch_write_job(
            job, provider_id=provider_id, max_epoch=max_epoch
        ),
        error_code="memory_scope_clearing",
        error_summary=cancel_summary,
        now=now,
    )
    active_writes = await jobs_service.list_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        statuses=("running", "reconciling"),
    )
    unsettled = [
        job
        for job in active_writes
        if is_old_epoch_write_job(job, provider_id=provider_id, max_epoch=max_epoch)
        and not is_write_job_settled_for_clear(job)
        and not is_write_job_already_audited_for_clear(job)
    ]
    return cancelled_queued, unsettled


async def resolve_current_time(session: AsyncSession, now: datetime | None) -> datetime:
    """Resolve current UTC time from database clock when `now` is omitted."""
    if now is None:
        return await repository.get_database_now(session)
    return repository.normalize_utc(now)


async def transition_tombstone_in_session(
    session: AsyncSession,
    *,
    tombstone_id: UUID,
    status: MemoryTombstoneStatus,
    external_ref: str | None = None,
    now: datetime | None = None,
) -> MemoryTombstoneRecord | None:
    """Conditionally transition a tombstone following §11.3.1."""
    current_time = await resolve_current_time(session, now)
    updated = await repository.conditional_transition_tombstone(
        session,
        tombstone_id=tombstone_id,
        new_status=status,
        allowed_from_statuses=allowed_tombstone_source_statuses(status),
        external_ref=external_ref,
        verified_at=current_time if status == "verified" else None,
        updated_at=current_time,
    )
    return MemoryTombstoneRecord.model_validate(updated) if updated is not None else None


async def transition_scope_barrier_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    new_barrier_state: MemoryBarrierState,
    new_epoch: int | None = None,
    barrier_operation_id: UUID | None = None,
    expected_barrier_operation_id: UUID | None = None,
    clear_barrier_operation_id: bool = False,
    now: datetime | None = None,
) -> bool:
    """Conditionally transition `memory_scopes.barrier_state` following §11.3.1."""
    current_time = await resolve_current_time(session, now)
    kwargs: dict[str, Any] = {
        "user_id": user_id,
        "provider_id": provider_id,
        "allowed_from_states": allowed_barrier_source_states(new_barrier_state),
        "new_barrier_state": new_barrier_state,
        "updated_at": current_time,
        "new_epoch": new_epoch,
    }
    if clear_barrier_operation_id:
        kwargs["barrier_operation_id"] = None
    elif barrier_operation_id is not None:
        kwargs["barrier_operation_id"] = barrier_operation_id
    if expected_barrier_operation_id is not None:
        kwargs["expected_barrier_operation_id"] = expected_barrier_operation_id

    updated = await repository.conditional_transition_scope_barrier(session, **kwargs)
    return updated is not None


async def fail_superseded_explicit_operation_in_session(
    session: AsyncSession,
    *,
    job: JobRecord,
    provider_id: str,
    clear_operation_id: UUID,
    error_message: str,
    now: datetime,
) -> None:
    """Transition an explicit memory create operation to `failed` when superseded by clear."""
    raw_op_id = job.metadata_json.get("operation_id")
    if not isinstance(raw_op_id, str):
        return
    try:
        explicit_op_id = UUID(raw_op_id)
    except ValueError:
        return
    await operations_service.transition_operation_in_session(
        session,
        operation_id=explicit_op_id,
        status="failed",
        progress={
            "phase": "superseded_by_clear",
            "provider_id": provider_id,
            "clear_operation_id": str(clear_operation_id),
        },
        error_code="memory_scope_cleared",
        error_message=error_message,
        now=now,
    )


async def finalize_clear_barrier_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    operation_id: UUID,
    tombstone_id: UUID | None,
    previous_epoch: int,
    new_epoch: int,
    external_ref: str | None,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    unsettled_audit_job_ids: tuple[UUID, ...] = (),
    compensation_executed: bool = False,
    defer_user_cleanup: bool = False,
    now: datetime | None = None,
) -> OperationRecord | None:
    """Atomically release barrier, verify tombstone, supersede old writes, and succeed op."""
    current_time = await resolve_current_time(session, now)

    await transition_scope_barrier_in_session(
        session,
        user_id=user_id,
        provider_id=provider_id,
        new_barrier_state="idle",
        expected_barrier_operation_id=operation_id,
        clear_barrier_operation_id=True,
        now=current_time,
    )
    if tombstone_id is not None:
        await transition_tombstone_in_session(
            session,
            tombstone_id=tombstone_id,
            status="verified",
            external_ref=external_ref,
            now=current_time,
        )

    # User deletion settles prior jobs and drops local memory state in bounded
    # owner-controlled phases after this remote confirmation commits.
    if not defer_user_cleanup:
        cancelled_settled = await jobs_service.cancel_matching_jobs_in_session(
            session,
            user_id=user_id,
            job_types=WRITE_JOB_TYPES,
            allowed_statuses=("pending", "retry_wait", "reconciling", "running"),
            predicate=lambda job: (
                is_old_epoch_write_job(job, provider_id=provider_id, max_epoch=previous_epoch)
                and (job.status in ("pending", "retry_wait") or is_write_job_settled_for_clear(job))
            ),
            error_code="memory_scope_cleared",
            error_summary="memory write superseded by completed scope clear",
            metadata_patch={"superseded_by_clear_operation_id": str(operation_id)},
            now=current_time,
        )
        for cancelled_job in cancelled_settled:
            await fail_superseded_explicit_operation_in_session(
                session,
                job=cancelled_job,
                provider_id=provider_id,
                clear_operation_id=operation_id,
                error_message="memory scope was cleared while creation was in flight",
                now=current_time,
            )

        for job_id in unsettled_audit_job_ids:
            await jobs_service.update_reconciling_job_in_session(
                session,
                job_id=job_id,
                allowed_statuses=("reconciling", "running"),
                error_code="memory_inflight_settle_timeout",
                error_summary=(
                    "old-epoch write exceeded settle timeout; retained for audit after clear"
                ),
                metadata_patch={
                    "superseded_by_clear_operation_id": str(operation_id),
                    "clear_settle_timeout_alerted": True,
                },
                defer_next_run_seconds=AUDIT_RECONCILING_DEFER_SECONDS,
                now=current_time,
            )

        marker = (
            recalled_memory_marker
            if recalled_memory_marker is not None
            else NoopRecalledMemoryReferenceMarker()
        )
        await marker.mark_user_memories_deleted_in_session(
            session,
            owner_id=user_id,
            provider_id=provider_id,
            max_memory_epoch=previous_epoch,
            deleted_at=current_time,
        )
        await repository.delete_list_snapshots_for_scope(
            session,
            user_id=user_id,
            provider_id=provider_id,
        )

    result_summary: dict[str, Any] = {
        "provider_id": provider_id,
        "previous_epoch": previous_epoch,
        "new_epoch": new_epoch,
        "barrier_state": "idle",
        "compensation_executed": compensation_executed,
    }
    if unsettled_audit_job_ids:
        result_summary["unsettled_write_job_ids"] = [str(jid) for jid in unsettled_audit_job_ids]

    return await operations_service.transition_operation_in_session(
        session,
        operation_id=operation_id,
        status="succeeded",
        progress={
            "phase": "succeeded",
            "provider_id": provider_id,
            "previous_epoch": previous_epoch,
            "new_epoch": new_epoch,
            "barrier_state": "idle",
            "inflight_writes_remaining": 0,
            "compensation_executed": compensation_executed,
        },
        result_summary=result_summary,
        now=current_time,
    )
