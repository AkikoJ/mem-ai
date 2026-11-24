"""Request-path user-scope memory clear orchestration (`DELETE /v1/memories?confirm=true`)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryProvider,
    build_clear_memories_external_ref,
)
from mem_ai.core.exceptions import UnprocessableEntityError
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobRecord, JobStatus
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_CLEAR as JOB_TYPE_MEMORY_CLEAR,
)
from mem_ai.modules.memories.barrier import (
    OPERATION_TYPE_MEMORY_CLEAR as OPERATION_TYPE_MEMORY_CLEAR,
)
from mem_ai.modules.memories.barrier import (
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
    cancel_queued_and_list_unsettled_writes_in_session,
    fail_superseded_explicit_operation_in_session,
    finalize_clear_barrier_in_session,
    invoke_provider_delete_call,
    is_retryable_memory_provider_error,
    make_clear_audit_event,
    map_memory_provider_error,
    resolve_current_time,
    transition_scope_barrier_in_session,
    transition_tombstone_in_session,
)
from mem_ai.modules.memories.recall import MemoryBarrierState
from mem_ai.modules.memories.schemas import (
    AcceptedMemoryDeletionOutcome,
    CompletedMemoryDeletionOutcome,
    MemoryDeletionOutcome,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    DeletionAuditEvent,
    export_deletion_audit_events_safely,
)
from mem_ai.modules.operations.schemas import OperationRecord, OperationStatus
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories.clearing")


@dataclass(frozen=True, slots=True)
class _PreparedClearScope:
    shortcut: MemoryDeletionOutcome | None
    operation: OperationRecord | None = None
    tombstone_id: UUID | None = None
    sync_job: JobRecord | None = None
    previous_epoch: int = 0
    new_epoch: int = 1
    unsettled_count: int = 0
    audit_events: tuple[DeletionAuditEvent, ...] = ()


async def prepare_clear_scope_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    marker: RecalledMemoryReferenceMarker,
    max_attempts: int,
    current_time: datetime,
    force_async: bool = False,
    user_cleanup_operation_id: UUID | None = None,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
) -> _PreparedClearScope:
    """Prepare a clear under the caller's resource-status policy.

    Request paths lock an active user before entering. User deletion owns the
    irreversible deleting transition and retains the user until cleanup completes.
    """
    scope_row = await repository.get_or_create_scope_for_update(
        session, user_id=user_id, provider_id=provider_id
    )
    if scope_row.barrier_state != "idle" and scope_row.barrier_operation_id is not None:
        existing_op = await operations_service.get_operation_in_session(
            session, operation_id=scope_row.barrier_operation_id
        )
        if existing_op is not None and existing_op.status in (
            "pending",
            "running",
            "reconciling",
        ):
            existing_is_cleanup = user_cleanup_operation_id is None
            if user_cleanup_operation_id is not None:
                existing_is_cleanup = bool(
                    await jobs_service.list_user_jobs_in_session(
                        session,
                        user_id=user_id,
                        job_types=(JOB_TYPE_MEMORY_CLEAR,),
                        statuses=("pending", "running", "retry_wait", "reconciling", "failed"),
                        metadata_match={
                            "operation_id": str(existing_op.id),
                            "user_cleanup_operation_id": str(user_cleanup_operation_id),
                        },
                        limit=1,
                    )
                )
            if existing_is_cleanup:
                return _PreparedClearScope(
                    shortcut=AcceptedMemoryDeletionOutcome(operation=existing_op)
                )

    if scope_row.barrier_state == "idle":
        previous_epoch, new_epoch = scope_row.epoch, scope_row.epoch + 1
        next_barrier: MemoryBarrierState = "clearing"
    else:
        new_epoch, previous_epoch = scope_row.epoch, max(0, scope_row.epoch - 1)
        next_barrier = cast(MemoryBarrierState, scope_row.barrier_state)

    cancelled_queued: list[JobRecord] = []
    unsettled: list[JobRecord] = []
    if user_cleanup_operation_id is None:
        cancelled_queued, unsettled = await cancel_queued_and_list_unsettled_writes_in_session(
            session,
            user_id=user_id,
            provider_id=provider_id,
            max_epoch=previous_epoch,
            cancel_summary="queued memory write cancelled by scope clearing barrier",
            now=current_time,
        )
    unsettled_count = len(unsettled)
    initial_status: OperationStatus = "pending" if force_async or unsettled_count > 0 else "running"

    operation = await operations_service.create_operation_in_session(
        session,
        owner_id=user_id,
        operation_type=OPERATION_TYPE_MEMORY_CLEAR,
        status=initial_status,
        target_type="memory_scope",
        target_id=f"{user_id}:{provider_id}",
        progress={
            "phase": "await_inflight" if unsettled_count > 0 else "clearing",
            "provider_id": provider_id,
            "previous_epoch": previous_epoch,
            "new_epoch": new_epoch,
            "barrier_state": next_barrier,
            "cancelled_queued_writes": len(cancelled_queued),
            "inflight_writes_remaining": unsettled_count,
            "compensation_scheduled": False,
        },
    )
    await transition_scope_barrier_in_session(
        session,
        user_id=user_id,
        provider_id=provider_id,
        new_barrier_state=next_barrier,
        new_epoch=new_epoch,
        barrier_operation_id=operation.id,
        now=current_time,
    )
    tombstone = await repository.create_tombstone(
        session,
        user_id=user_id,
        provider_id=provider_id,
        target_kind="user",
        memory_epoch=previous_epoch,
        operation_id=operation.id,
        status="pending",
    )
    clear_meta = {
        "operation_id": str(operation.id),
        "tombstone_id": str(tombstone.id),
        "provider_id": provider_id,
        "previous_epoch": previous_epoch,
        "new_epoch": new_epoch,
        "barrier_started_at": current_time.isoformat(),
        "clear_phase": (
            "await_inflight" if force_async or unsettled_count > 0 else "clear_dispatched"
        ),
        "settle_timeout_exceeded": False,
        "compensation_scheduled": False,
    }
    if user_cleanup_operation_id is not None:
        clear_meta["user_cleanup_operation_id"] = str(user_cleanup_operation_id)
    for cancelled in cancelled_queued:
        await fail_superseded_explicit_operation_in_session(
            session,
            job=cancelled,
            provider_id=provider_id,
            clear_operation_id=operation.id,
            error_message="memory creation was superseded by scope clearing",
            now=current_time,
        )
    sync_job: JobRecord | None = None
    if force_async or unsettled_count > 0:
        enqueued, _ = await jobs_service.enqueue_job_in_session(
            session,
            job_type=JOB_TYPE_MEMORY_CLEAR,
            business_id=str(operation.id),
            user_id=user_id,
            dedupe_key=f"{JOB_TYPE_MEMORY_CLEAR}:{operation.id}",
            max_attempts=max_attempts,
            now=current_time,
            metadata_json=clear_meta,
        )
        job_id = enqueued.id
    else:
        sync_job = await jobs_service.register_and_claim_sync_job_in_session(
            session,
            job_type=JOB_TYPE_MEMORY_CLEAR,
            business_id=str(operation.id),
            user_id=user_id,
            dedupe_key=f"{JOB_TYPE_MEMORY_CLEAR}:{operation.id}",
            max_attempts=max_attempts,
            attempt_retention_days=attempt_retention_days,
            now=current_time,
            metadata_json=clear_meta,
        )
        job_id = sync_job.id

    attached = await operations_service.attach_job_in_session(
        session, operation_id=operation.id, job_id=job_id, now=current_time
    )
    final_op = attached or operation
    if user_cleanup_operation_id is None:
        await marker.mark_user_memories_deleted_in_session(
            session,
            owner_id=user_id,
            provider_id=provider_id,
            max_memory_epoch=previous_epoch,
            deleted_at=current_time,
        )
        await repository.delete_list_snapshots_for_scope(
            session, user_id=user_id, provider_id=provider_id
        )
    events = (
        make_clear_audit_event(
            "operation_accepted",
            user_id=user_id,
            operation_id=final_op.id,
            provider_id=provider_id,
            memory_epoch=new_epoch,
            barrier_state=next_barrier,
            status=final_op.status,
            occurred_at=current_time,
            metadata={
                "previous_epoch": previous_epoch,
                "cancelled_queued_writes": len(cancelled_queued),
                "inflight_writes_remaining": unsettled_count,
            },
        ),
        make_clear_audit_event(
            "memory_epoch_advanced",
            user_id=user_id,
            operation_id=final_op.id,
            provider_id=provider_id,
            memory_epoch=new_epoch,
            barrier_state=next_barrier,
            occurred_at=current_time,
            metadata={"previous_epoch": previous_epoch},
        ),
    )
    return _PreparedClearScope(
        shortcut=None,
        operation=final_op,
        tombstone_id=tombstone.id,
        sync_job=sync_job,
        previous_epoch=previous_epoch,
        new_epoch=new_epoch,
        unsettled_count=unsettled_count,
        audit_events=events,
    )


async def _prepare_clear_scope_tx1(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    marker: RecalledMemoryReferenceMarker,
    max_attempts: int,
    attempt_retention_days: int,
    now: datetime | None,
) -> _PreparedClearScope:
    async with session_factory.begin() as session:
        current_time = await resolve_current_time(session, now)
        await users_service.ensure_user_active_in_session(session, user_id=user_id, for_share=True)
        return await prepare_clear_scope_in_session(
            session,
            user_id=user_id,
            provider_id=provider_id,
            marker=marker,
            max_attempts=max_attempts,
            current_time=current_time,
            attempt_retention_days=attempt_retention_days,
        )


async def clear_user_memories(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider,
    user_id: UUID,
    confirm: bool,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    audit_archiver: DeletionAuditArchiver | None = None,
    max_attempts: int = 5,
    reconcile_delay_seconds: float = 10.0,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> MemoryDeletionOutcome:
    """Atomically bump epoch, establish clearing barrier, cancel queued writes, and clear."""
    if not confirm:
        raise UnprocessableEntityError(
            code="clear_confirmation_required",
            message="confirm=true is required to clear all memories",
        )

    provider_id = memory_provider.provider_id
    marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
    prepared = await _prepare_clear_scope_tx1(
        session_factory,
        user_id=user_id,
        provider_id=provider_id,
        marker=marker,
        max_attempts=max_attempts,
        attempt_retention_days=attempt_retention_days,
        now=now,
    )
    if prepared.shortcut is not None:
        return prepared.shortcut

    assert prepared.operation is not None and prepared.tombstone_id is not None
    operation = prepared.operation
    await export_deletion_audit_events_safely(audit_archiver, prepared.audit_events)

    if prepared.unsettled_count > 0 or prepared.sync_job is None:
        return AcceptedMemoryDeletionOutcome(operation=operation)

    sync_job = prepared.sync_job
    lease_token = sync_job.lease_token
    assert lease_token is not None
    default_ref = build_clear_memories_external_ref(user_id)
    result = await invoke_provider_delete_call(
        lambda: memory_provider.clear_memories(user_id=user_id),
        default_external_ref=default_ref,
        provider_id=provider_id,
        operation_id=operation.id,
        job_id=sync_job.id,
        action_label="clear_memories",
    )
    ext_ref = result.external_ref or default_ref

    if result.status == "succeeded":
        async with session_factory.begin() as session:
            current_time = await resolve_current_time(session, now)
            completed_op = await finalize_clear_barrier_in_session(
                session,
                user_id=user_id,
                provider_id=provider_id,
                operation_id=operation.id,
                tombstone_id=prepared.tombstone_id,
                previous_epoch=prepared.previous_epoch,
                new_epoch=prepared.new_epoch,
                external_ref=ext_ref,
                recalled_memory_marker=marker,
                now=current_time,
            )
            await jobs_service.complete_job_in_session(
                session,
                job_id=sync_job.id,
                lease_token=lease_token,
                status="succeeded",
                external_ref=ext_ref,
                metadata_patch={"clear_phase": "completed"},
                attempt_retention_days=attempt_retention_days,
                now=current_time,
            )
        final_op = completed_op or operation
        await export_deletion_audit_events_safely(
            audit_archiver,
            (
                make_clear_audit_event(
                    "memory_barrier_released",
                    user_id=user_id,
                    operation_id=final_op.id,
                    provider_id=provider_id,
                    memory_epoch=prepared.new_epoch,
                    barrier_state="idle",
                    status="succeeded",
                ),
                make_clear_audit_event(
                    "operation_transitioned",
                    user_id=user_id,
                    operation_id=final_op.id,
                    provider_id=provider_id,
                    memory_epoch=prepared.new_epoch,
                    barrier_state="idle",
                    status="succeeded",
                ),
            ),
        )
        return CompletedMemoryDeletionOutcome(operation=final_op)

    is_rec = result.status in ("accepted", "reconciling")
    err_code = result.error_code or (
        "memory_provider_timeout" if result.status == "reconciling" else "memory_provider_error"
    )
    retryable_fail = not is_rec and is_retryable_memory_provider_error(err_code)
    mapped_error = (
        None
        if (is_rec or retryable_fail)
        else map_memory_provider_error(
            err_code, default_message="memory provider failed to clear memories"
        )
    )
    if is_rec:
        op_status: OperationStatus = "reconciling"
        job_status: JobStatus = "reconciling"
        clear_phase = "verify_clear"
        op_err_code = (
            (result.error_code or "memory_provider_timeout")
            if result.status == "reconciling"
            else None
        )
        op_err_msg: str | None = "remote clear outcome unknown; barrier held reconciling"
        job_err_summary = "remote clear outcome unknown; awaiting verification"
    elif retryable_fail:
        op_status = "reconciling"
        job_status = "retry_wait"
        clear_phase = "execute_final_clear"
        op_err_code = err_code
        op_err_msg = "remote clear failed transiently; retrying automatically"
        job_err_summary = "remote clear failed transiently; scheduled for Worker retry"
    else:
        assert mapped_error is not None
        op_status = "failed"
        job_status = "failed"
        clear_phase = "failed"
        op_err_code = mapped_error.code
        op_err_msg = mapped_error.message
        job_err_summary = "memory provider rejected clear operation; barrier held reconciling"

    async with session_factory.begin() as session:
        current_time = await resolve_current_time(session, now)
        await transition_scope_barrier_in_session(
            session,
            user_id=user_id,
            provider_id=provider_id,
            new_barrier_state="reconciling",
            expected_barrier_operation_id=operation.id,
            now=current_time,
        )
        await transition_tombstone_in_session(
            session,
            tombstone_id=prepared.tombstone_id,
            status="reconciling",
            external_ref=ext_ref,
            now=current_time,
        )
        updated_op = await operations_service.transition_operation_in_session(
            session,
            operation_id=operation.id,
            status=op_status,
            progress={
                "phase": clear_phase,
                "provider_id": provider_id,
                "previous_epoch": prepared.previous_epoch,
                "new_epoch": prepared.new_epoch,
                "barrier_state": "reconciling",
                "external_ref": ext_ref,
            },
            error_code=op_err_code,
            error_message=op_err_msg,
            now=current_time,
        )
        await jobs_service.complete_job_in_session(
            session,
            job_id=sync_job.id,
            lease_token=lease_token,
            status=job_status,
            external_ref=ext_ref,
            error_code=op_err_code,
            error_summary=job_err_summary,
            metadata_patch={"clear_phase": clear_phase},
            next_run_at=(
                current_time + timedelta(seconds=reconcile_delay_seconds)
                if job_status in ("reconciling", "retry_wait")
                else None
            ),
            attempt_retention_days=attempt_retention_days,
            now=current_time,
        )

    (logger.warning if mapped_error is None else logger.error)(
        "Holding clearing barrier in reconciling state after clear attempt",
        extra={
            "extra_fields": {
                "alert": "memory_clear_barrier_reconciling",
                "user_id": str(user_id),
                "provider": provider_id,
                "operation_id": str(operation.id),
                "memory_epoch": prepared.new_epoch,
                "external_ref": ext_ref,
                "error_code": op_err_code,
            }
        },
    )
    final_op = updated_op or operation
    await export_deletion_audit_events_safely(
        audit_archiver,
        (
            make_clear_audit_event(
                "operation_transitioned",
                user_id=user_id,
                operation_id=final_op.id,
                provider_id=provider_id,
                memory_epoch=prepared.new_epoch,
                barrier_state="reconciling",
                status=op_status,
                metadata={"error_code": op_err_code} if op_err_code else {},
            ),
        ),
    )
    if mapped_error is not None:
        raise mapped_error
    return AcceptedMemoryDeletionOutcome(operation=final_op)
