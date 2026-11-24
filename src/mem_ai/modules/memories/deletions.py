"""Single-memory deletion orchestration and Worker handler (`memory_delete`)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryProvider,
    build_delete_memory_external_ref,
)
from mem_ai.core.exceptions import ServiceUnavailableError, UnprocessableEntityError
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.db.base import is_unique_violation
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    JobBeforeCommitHook,
    JobExecutionContext,
    JobHandlerOutcome,
    JobRecord,
)
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_DELETE as JOB_TYPE_MEMORY_DELETE,
)
from mem_ai.modules.memories.barrier import (
    OPERATION_TYPE_MEMORY_DELETE as OPERATION_TYPE_MEMORY_DELETE,
)
from mem_ai.modules.memories.barrier import (
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
    invoke_provider_delete_call,
    make_delete_audit_event,
    map_memory_provider_error,
    resolve_current_time,
    transition_tombstone_in_session,
)
from mem_ai.modules.memories.models import MEMORY_TOMBSTONE_USER_MEMORY_UNIQUE_INDEX
from mem_ai.modules.memories.schemas import (
    AcceptedMemoryDeletionOutcome,
    CompletedMemoryDeletionOutcome,
    MemoryDeletionOutcome,
    MemoryTombstoneRecord,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    DeletionAuditEvent,
    export_deletion_audit_events_safely,
)
from mem_ai.modules.operations.schemas import OperationRecord, OperationStatus
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories.deletions")

MemoryDeleteState = Literal[
    "pending",
    "in_flight",
    "accepted",
    "reconciling",
    "retry_delete",
    "verified",
    "failed",
]


class MemoryDeleteJobMetadata(BaseModel):
    """Validated metadata persisted for a `memory_delete` durable job."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    operation_id: UUID
    tombstone_id: UUID
    provider_id: str
    memory_id: str
    memory_epoch: int = 0
    delete_state: MemoryDeleteState = "pending"


@dataclass(frozen=True, slots=True)
class _PreparedSingleDelete:
    shortcut: MemoryDeletionOutcome | None
    operation: OperationRecord | None = None
    tombstone: MemoryTombstoneRecord | None = None
    job: JobRecord | None = None
    created_new_tombstone: bool = False
    audit_events: tuple[DeletionAuditEvent, ...] = ()


async def _prepare_single_delete_tx1(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    memory_id: str,
    marker: RecalledMemoryReferenceMarker,
    max_attempts: int,
    attempt_retention_days: int,
    now: datetime | None,
) -> _PreparedSingleDelete:
    for _ in range(2):
        try:
            async with session_factory.begin() as session:
                current_time = await resolve_current_time(session, now)
                await users_service.ensure_user_active_in_session(
                    session, user_id=user_id, for_share=True
                )
                scope_row = await repository.get_scope(
                    session, user_id=user_id, provider_id=provider_id, for_share=True
                )
                current_epoch = scope_row.epoch if scope_row is not None else 0

                existing = await repository.get_memory_tombstone(
                    session,
                    user_id=user_id,
                    provider_id=provider_id,
                    memory_id=memory_id,
                    for_update=True,
                )
                if existing is not None:
                    existing_op = (
                        await operations_service.get_operation_in_session(
                            session, operation_id=existing.operation_id
                        )
                        if existing.operation_id is not None
                        else None
                    )
                    if existing.status == "verified":
                        return _PreparedSingleDelete(
                            shortcut=CompletedMemoryDeletionOutcome(operation=existing_op)
                        )
                    if existing_op is not None and existing_op.status in (
                        "pending",
                        "running",
                        "reconciling",
                    ):
                        return _PreparedSingleDelete(
                            shortcut=AcceptedMemoryDeletionOutcome(operation=existing_op)
                        )

                operation = await operations_service.create_operation_in_session(
                    session,
                    owner_id=user_id,
                    operation_type=OPERATION_TYPE_MEMORY_DELETE,
                    status="running",
                    target_type="memory",
                    target_id=memory_id,
                    progress={
                        "phase": "deleting",
                        "provider_id": provider_id,
                        "memory_id": memory_id,
                        "memory_epoch": current_epoch,
                    },
                )
                created_new = existing is None
                if existing is None:
                    tombstone_row = await repository.create_tombstone(
                        session,
                        user_id=user_id,
                        provider_id=provider_id,
                        target_kind="memory",
                        memory_id=memory_id,
                        memory_epoch=current_epoch,
                        operation_id=operation.id,
                        status="pending",
                    )
                else:
                    rebound = await repository.attach_operation_to_tombstone(
                        session,
                        tombstone_id=existing.id,
                        operation_id=operation.id,
                        external_ref=None,
                        updated_at=current_time,
                    )
                    tombstone_row = rebound if rebound is not None else existing

                job = await jobs_service.register_and_claim_sync_job_in_session(
                    session,
                    job_type=JOB_TYPE_MEMORY_DELETE,
                    business_id=str(operation.id),
                    user_id=user_id,
                    dedupe_key=f"{JOB_TYPE_MEMORY_DELETE}:{operation.id}",
                    max_attempts=max_attempts,
                    attempt_retention_days=attempt_retention_days,
                    now=current_time,
                    metadata_json={
                        "operation_id": str(operation.id),
                        "tombstone_id": str(tombstone_row.id),
                        "provider_id": provider_id,
                        "memory_id": memory_id,
                        "memory_epoch": current_epoch,
                        "delete_state": "in_flight",
                    },
                )
                attached = await operations_service.attach_job_in_session(
                    session, operation_id=operation.id, job_id=job.id, now=current_time
                )
                final_op = attached or operation
                await marker.mark_memory_deleted_in_session(
                    session,
                    owner_id=user_id,
                    provider_id=provider_id,
                    memory_id=memory_id,
                    deleted_at=current_time,
                )
                await repository.delete_list_snapshots_for_scope(
                    session, user_id=user_id, provider_id=provider_id
                )
                tombstone_dto = MemoryTombstoneRecord.model_validate(tombstone_row)
                events = (
                    make_delete_audit_event(
                        "operation_accepted",
                        user_id=user_id,
                        operation_id=final_op.id,
                        provider_id=provider_id,
                        memory_id=memory_id,
                        memory_epoch=current_epoch,
                        status=final_op.status,
                        occurred_at=current_time,
                    ),
                    make_delete_audit_event(
                        "memory_tombstone_created",
                        user_id=user_id,
                        operation_id=final_op.id,
                        provider_id=provider_id,
                        memory_id=memory_id,
                        memory_epoch=current_epoch,
                        status=tombstone_dto.status,
                        occurred_at=current_time,
                    ),
                )
                return _PreparedSingleDelete(
                    shortcut=None,
                    operation=final_op,
                    tombstone=tombstone_dto,
                    job=job,
                    created_new_tombstone=created_new,
                    audit_events=events,
                )
        except IntegrityError as exc:
            if is_unique_violation(exc, constraint_name=MEMORY_TOMBSTONE_USER_MEMORY_UNIQUE_INDEX):
                continue
            raise

    raise ServiceUnavailableError(
        code="memory_delete_conflict",
        message="concurrent deletion state could not be resolved",
    )


async def delete_single_memory(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider,
    user_id: UUID,
    memory_id: str,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    audit_archiver: DeletionAuditArchiver | None = None,
    max_attempts: int = 5,
    reconcile_delay_seconds: float = 10.0,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> MemoryDeletionOutcome:
    """Persist tombstone + operation, invoke provider delete outside Tx, and converge outcome."""
    normalized_id = memory_id.strip()
    if not normalized_id or len(normalized_id) > 255:
        raise UnprocessableEntityError(
            code="validation_error",
            message="memoryId must be a non-empty string up to 255 characters",
        )

    provider_id = memory_provider.provider_id
    marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
    prepared = await _prepare_single_delete_tx1(
        session_factory,
        user_id=user_id,
        provider_id=provider_id,
        memory_id=normalized_id,
        marker=marker,
        max_attempts=max_attempts,
        attempt_retention_days=attempt_retention_days,
        now=now,
    )
    if prepared.shortcut is not None:
        return prepared.shortcut

    assert prepared.operation is not None and prepared.tombstone is not None
    assert prepared.job is not None
    operation, tombstone, job = prepared.operation, prepared.tombstone, prepared.job
    lease_token = job.lease_token
    assert lease_token is not None
    await export_deletion_audit_events_safely(audit_archiver, prepared.audit_events)

    default_ref = build_delete_memory_external_ref(normalized_id)
    result = await invoke_provider_delete_call(
        lambda: memory_provider.delete_memory(user_id=user_id, memory_id=normalized_id),
        default_external_ref=default_ref,
        provider_id=provider_id,
        operation_id=operation.id,
        job_id=job.id,
        action_label="delete_memory",
    )
    ext_ref = result.external_ref or default_ref

    if result.status == "succeeded":
        async with session_factory.begin() as session:
            current_time = await resolve_current_time(session, now)
            await transition_tombstone_in_session(
                session,
                tombstone_id=tombstone.id,
                status="verified",
                external_ref=ext_ref,
                now=current_time,
            )
            completed_op = await operations_service.transition_operation_in_session(
                session,
                operation_id=operation.id,
                status="succeeded",
                progress={
                    "phase": "succeeded",
                    "provider_id": provider_id,
                    "memory_id": normalized_id,
                },
                result_summary={"memory_id": normalized_id, "provider_id": provider_id},
                now=current_time,
            )
            await jobs_service.complete_job_in_session(
                session,
                job_id=job.id,
                lease_token=lease_token,
                status="succeeded",
                external_ref=ext_ref,
                metadata_patch={"delete_state": "verified"},
                attempt_retention_days=attempt_retention_days,
                now=current_time,
            )
        final_op = completed_op or operation
        await export_deletion_audit_events_safely(
            audit_archiver,
            (
                make_delete_audit_event(
                    "memory_tombstone_verified",
                    user_id=user_id,
                    operation_id=final_op.id,
                    provider_id=provider_id,
                    memory_id=normalized_id,
                    status="verified",
                ),
                make_delete_audit_event(
                    "operation_transitioned",
                    user_id=user_id,
                    operation_id=final_op.id,
                    provider_id=provider_id,
                    memory_id=normalized_id,
                    status="succeeded",
                ),
            ),
        )
        return CompletedMemoryDeletionOutcome(operation=final_op)

    if result.status in ("accepted", "reconciling"):
        is_rec = result.status == "reconciling"
        op_status: OperationStatus = "reconciling" if is_rec else "running"
        err_code = result.error_code if is_rec else None
        err_msg = "memory deletion outcome unknown; verification pending" if is_rec else None
        async with session_factory.begin() as session:
            current_time = await resolve_current_time(session, now)
            await transition_tombstone_in_session(
                session,
                tombstone_id=tombstone.id,
                status="reconciling",
                external_ref=ext_ref,
                now=current_time,
            )
            updated_op = await operations_service.transition_operation_in_session(
                session,
                operation_id=operation.id,
                status=op_status,
                progress={
                    "phase": "reconciling" if is_rec else "accepted",
                    "provider_id": provider_id,
                    "memory_id": normalized_id,
                    "external_ref": ext_ref,
                },
                error_code=err_code,
                error_message=err_msg,
                now=current_time,
            )
            await jobs_service.complete_job_in_session(
                session,
                job_id=job.id,
                lease_token=lease_token,
                status="reconciling",
                external_ref=ext_ref,
                error_code=err_code,
                error_summary=(
                    "single memory deletion outcome unknown; pending verification"
                    if is_rec
                    else "single memory deletion accepted asynchronously"
                ),
                metadata_patch={"delete_state": "reconciling" if is_rec else "accepted"},
                next_run_at=current_time + timedelta(seconds=reconcile_delay_seconds),
                attempt_retention_days=attempt_retention_days,
                now=current_time,
            )
        final_op = updated_op or operation
        await export_deletion_audit_events_safely(
            audit_archiver,
            (
                make_delete_audit_event(
                    "operation_transitioned",
                    user_id=user_id,
                    operation_id=final_op.id,
                    provider_id=provider_id,
                    memory_id=normalized_id,
                    status=final_op.status,
                ),
            ),
        )
        return AcceptedMemoryDeletionOutcome(operation=final_op)

    err_code = result.error_code or "memory_provider_error"
    mapped_error = map_memory_provider_error(
        err_code, default_message="memory provider failed to delete memory"
    )
    async with session_factory.begin() as session:
        current_time = await resolve_current_time(session, now)
        if err_code == "memory_not_found" and prepared.created_new_tombstone:
            await repository.delete_tombstone_by_id(session, tombstone_id=tombstone.id)
        await operations_service.transition_operation_in_session(
            session,
            operation_id=operation.id,
            status="failed",
            progress={"phase": "failed", "provider_id": provider_id, "memory_id": normalized_id},
            error_code=mapped_error.code,
            error_message=mapped_error.message,
            now=current_time,
        )
        await jobs_service.complete_job_in_session(
            session,
            job_id=job.id,
            lease_token=lease_token,
            status="failed",
            external_ref=ext_ref,
            error_code=mapped_error.code,
            error_summary="memory provider rejected single memory deletion",
            metadata_patch={"delete_state": "failed"},
            attempt_retention_days=attempt_retention_days,
            now=current_time,
        )
    await export_deletion_audit_events_safely(
        audit_archiver,
        (
            make_delete_audit_event(
                "operation_transitioned",
                user_id=user_id,
                operation_id=operation.id,
                provider_id=provider_id,
                memory_id=normalized_id,
                status="failed",
                metadata={"error_code": mapped_error.code},
            ),
        ),
    )
    raise mapped_error


class MemoryDeleteJobHandler:
    """Worker handler verifying and retrying durable `memory_delete` jobs."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        memory_provider: MemoryProvider,
        audit_archiver: DeletionAuditArchiver | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._memory_provider = memory_provider
        self._audit_archiver = audit_archiver

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Verify an in-flight/reconciling single delete or retry provider deletion."""
        try:
            meta = MemoryDeleteJobMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(
                outcome="failed",
                error_code="invalid_job_metadata",
                error_summary="memory_delete job metadata is incomplete or invalid",
            )

        log_tokens = bind_log_context(
            user_id=str(ctx.job.user_id) if ctx.job.user_id is not None else None,
            provider=meta.provider_id or self._memory_provider.provider_id,
            job_id=str(ctx.job.id),
        )
        try:
            if ctx.job.user_id is None:
                return JobHandlerOutcome(
                    outcome="cancelled",
                    error_code="user_deleted",
                    error_summary="owning user no longer exists",
                )
            if meta.provider_id != self._memory_provider.provider_id:
                return JobHandlerOutcome(
                    outcome="failed",
                    error_code="memory_provider_mismatch",
                    error_summary="memory_delete job provider does not match configured provider",
                )

            user_id = ctx.job.user_id
            default_ref = build_delete_memory_external_ref(meta.memory_id)
            ext_ref = ctx.job.external_ref or default_ref

            if meta.delete_state in ("in_flight", "accepted", "reconciling") and (
                ctx.job.external_ref is not None
            ):
                return await self._verify_delete(user_id=user_id, meta=meta, external_ref=ext_ref)

            return await self._dispatch_delete(
                ctx=ctx, user_id=user_id, meta=meta, default_ref=default_ref
            )
        finally:
            reset_log_context(log_tokens)

    def _build_verified_outcome(
        self,
        *,
        user_id: UUID,
        meta: MemoryDeleteJobMetadata,
        external_ref: str,
    ) -> JobHandlerOutcome:
        async def _before_commit(session: AsyncSession, _job: JobRecord) -> None:
            await transition_tombstone_in_session(
                session,
                tombstone_id=meta.tombstone_id,
                status="verified",
                external_ref=external_ref,
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=meta.operation_id,
                status="succeeded",
                progress={
                    "phase": "succeeded",
                    "provider_id": meta.provider_id,
                    "memory_id": meta.memory_id,
                },
                result_summary={"memory_id": meta.memory_id, "provider_id": meta.provider_id},
            )

        async def _after_commit(_job: JobRecord) -> None:
            await export_deletion_audit_events_safely(
                self._audit_archiver,
                (
                    make_delete_audit_event(
                        "memory_tombstone_verified",
                        user_id=user_id,
                        operation_id=meta.operation_id,
                        provider_id=meta.provider_id,
                        memory_id=meta.memory_id,
                        status="verified",
                    ),
                    make_delete_audit_event(
                        "operation_transitioned",
                        user_id=user_id,
                        operation_id=meta.operation_id,
                        provider_id=meta.provider_id,
                        memory_id=meta.memory_id,
                        status="succeeded",
                    ),
                ),
            )

        return JobHandlerOutcome(
            outcome="succeeded",
            external_ref=external_ref,
            metadata_patch={"delete_state": "verified"},
            result_summary={"phase": "verified", "provider_id": meta.provider_id},
            before_commit=_before_commit,
            after_commit=_after_commit,
        )

    def _make_fail_on_exhausted_hook(
        self,
        *,
        meta: MemoryDeleteJobMetadata,
        error_code: str,
        error_message: str,
    ) -> JobBeforeCommitHook:
        async def _hook(session: AsyncSession, updated_job: JobRecord) -> None:
            if updated_job.status != "failed":
                return
            await operations_service.transition_operation_in_session(
                session,
                operation_id=meta.operation_id,
                status="failed",
                progress={
                    "phase": "failed",
                    "provider_id": meta.provider_id,
                    "memory_id": meta.memory_id,
                },
                error_code=error_code,
                error_message=error_message,
            )

        return _hook

    async def _verify_delete(
        self,
        *,
        user_id: UUID,
        meta: MemoryDeleteJobMetadata,
        external_ref: str,
    ) -> JobHandlerOutcome:
        try:
            status = await self._memory_provider.verify_operation(
                user_id=user_id,
                external_ref=external_ref,
            )
        except Exception:
            logger.warning(
                "Memory delete verification raised an error; keeping reconciling",
                extra={"extra_fields": {"external_ref": external_ref}},
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=external_ref,
                error_code="memory_verification_error",
                error_summary="single delete verification failed; will retry",
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
            )

        resolved_ref = status.external_ref or external_ref
        if status.status == "succeeded":
            return self._build_verified_outcome(
                user_id=user_id, meta=meta, external_ref=resolved_ref
            )
        if status.status == "failed":
            err_code = status.error_code or "memory_provider_error"
            mapped = map_memory_provider_error(
                err_code, default_message="memory provider failed to delete memory"
            )
            return JobHandlerOutcome(
                outcome="retryable",
                external_ref=resolved_ref,
                error_code=err_code,
                error_summary="provider indicated deletion not yet complete; retrying delete",
                metadata_patch={"delete_state": "retry_delete"},
                result_summary={"phase": "retry_delete", "provider_id": meta.provider_id},
                before_commit=self._make_fail_on_exhausted_hook(
                    meta=meta, error_code=mapped.code, error_message=mapped.message
                ),
            )
        return JobHandlerOutcome(
            outcome="reconciling",
            external_ref=resolved_ref,
            error_code=status.error_code,
            error_summary="single memory deletion still pending verification",
            result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
        )

    async def _dispatch_delete(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: MemoryDeleteJobMetadata,
        default_ref: str,
    ) -> JobHandlerOutcome:
        async with self._session_factory.begin() as session:
            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=default_ref,
                metadata_patch={"delete_state": "in_flight"},
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=meta.operation_id,
                status="running",
                progress={
                    "phase": "deleting",
                    "provider_id": meta.provider_id,
                    "memory_id": meta.memory_id,
                },
            )

        result = await invoke_provider_delete_call(
            lambda: self._memory_provider.delete_memory(user_id=user_id, memory_id=meta.memory_id),
            default_external_ref=default_ref,
            provider_id=meta.provider_id,
            operation_id=meta.operation_id,
            job_id=ctx.job.id,
            action_label="delete_memory",
        )
        ext_ref = result.external_ref or default_ref

        if result.status == "succeeded" or (
            result.status == "failed" and result.error_code == "memory_not_found"
        ):
            return self._build_verified_outcome(user_id=user_id, meta=meta, external_ref=ext_ref)

        if result.status in ("accepted", "reconciling"):

            async def _mark_rec(session: AsyncSession, _job: JobRecord) -> None:
                await transition_tombstone_in_session(
                    session,
                    tombstone_id=meta.tombstone_id,
                    status="reconciling",
                    external_ref=ext_ref,
                )
                await operations_service.transition_operation_in_session(
                    session,
                    operation_id=meta.operation_id,
                    status="reconciling",
                    progress={
                        "phase": "reconciling",
                        "provider_id": meta.provider_id,
                        "memory_id": meta.memory_id,
                        "external_ref": ext_ref,
                    },
                    error_code=result.error_code,
                    error_message="memory deletion outcome unknown; verification pending",
                )

            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=ext_ref,
                error_code=result.error_code,
                error_summary="single memory deletion awaiting verification",
                metadata_patch={"delete_state": "reconciling"},
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
                before_commit=_mark_rec,
            )

        err_code = result.error_code or "memory_provider_error"
        mapped = map_memory_provider_error(
            err_code, default_message="memory provider failed to delete memory"
        )
        exhausted = (
            err_code == "memory_provider_auth_error"
            or ctx.job.attempt_count >= ctx.job.max_attempts
        )
        fail_hook = self._make_fail_on_exhausted_hook(
            meta=meta, error_code=mapped.code, error_message=mapped.message
        )
        if exhausted:

            async def _audit_failed(_job: JobRecord) -> None:
                await export_deletion_audit_events_safely(
                    self._audit_archiver,
                    (
                        make_delete_audit_event(
                            "operation_transitioned",
                            user_id=user_id,
                            operation_id=meta.operation_id,
                            provider_id=meta.provider_id,
                            memory_id=meta.memory_id,
                            status="failed",
                            metadata={"error_code": mapped.code},
                        ),
                    ),
                )

            return JobHandlerOutcome(
                outcome="failed",
                external_ref=ext_ref,
                error_code=mapped.code,
                error_summary="single memory deletion failed permanently",
                metadata_patch={"delete_state": "failed"},
                result_summary={"phase": "failed", "provider_id": meta.provider_id},
                before_commit=fail_hook,
                after_commit=_audit_failed,
            )

        return JobHandlerOutcome(
            outcome="retryable",
            external_ref=ext_ref,
            error_code=mapped.code,
            error_summary="memory provider returned retryable error during single delete",
            metadata_patch={"delete_state": "retry_delete"},
            result_summary={"phase": "retry_delete", "provider_id": meta.provider_id},
            before_commit=fail_hook,
        )
