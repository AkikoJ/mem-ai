"""Worker job handler (`memory_clear`) converging in-flight writes and clearing barriers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryProvider,
    build_clear_memories_external_ref,
    provider_supports_write_status_verification,
)
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    JobExecutionContext,
    JobHandlerOutcome,
    JobRecord,
)
from mem_ai.modules.memories.barrier import (
    AUDIT_RECONCILING_DEFER_SECONDS,
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
    cancel_queued_and_list_unsettled_writes_in_session,
    fail_superseded_explicit_operation_in_session,
    finalize_clear_barrier_in_session,
    invoke_provider_delete_call,
    is_write_job_already_audited_for_clear,
    is_write_job_settled_for_clear,
    make_clear_audit_event,
    map_memory_provider_error,
)
from mem_ai.modules.memories.clear_state import (
    ClearExecutionPlan as _ClearExecutionPlan,
)
from mem_ai.modules.memories.clear_state import (
    MemoryClearJobMetadata,
    make_hold_barrier_hook,
)
from mem_ai.modules.memories.user_cleanup import prepare_user_memory_clear_batch_in_session
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    export_deletion_audit_events_safely,
)

logger = logging.getLogger("mem_ai.memories.clear_jobs")


def _log_alert(alert: str, message: str, *, level: int = logging.WARNING, **fields: Any) -> None:
    logger.log(level, message, extra={"extra_fields": {"alert": alert, **fields}})


class MemoryClearJobHandler:
    """Worker handler converging in-flight writes, final clear, and compensation clear."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        memory_provider: MemoryProvider,
        inflight_settle_timeout_seconds: float = 60.0,
        compensation_delay_seconds: float = 30.0,
        recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
        audit_archiver: DeletionAuditArchiver | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._memory_provider = memory_provider
        self._inflight_settle_timeout_seconds = inflight_settle_timeout_seconds
        self._compensation_delay_seconds = compensation_delay_seconds
        self._marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
        self._audit_archiver = audit_archiver
        self._clock = clock

    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Advance one `memory_clear` job phase through §11.3.1 barrier convergence."""
        try:
            meta = MemoryClearJobMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(
                outcome="failed",
                error_code="invalid_job_metadata",
                error_summary="memory_clear job metadata is incomplete or invalid",
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
                _log_alert(
                    "memory_clear_provider_unavailable",
                    "Retaining clear until its provider is available",
                    operation_id=str(meta.operation_id),
                    provider=meta.provider_id,
                )
                return JobHandlerOutcome(
                    outcome="reconciling",
                    error_code="memory_provider_mismatch",
                    error_summary="memory_clear is waiting for its configured provider",
                )

            user_id = ctx.job.user_id
            if meta.clear_phase in ("clear_dispatched", "verify_clear") and (
                ctx.job.external_ref is not None
            ):
                return await self._verify_clear(
                    user_id=user_id, meta=meta, external_ref=ctx.job.external_ref
                )
            if meta.clear_phase in ("compensation_clear", "execute_final_clear"):
                is_comp = meta.clear_phase == "compensation_clear"
                plan = _ClearExecutionPlan(
                    is_compensation=is_comp,
                    settle_timeout_exceeded=True if is_comp else meta.settle_timeout_exceeded,
                    unsettled_audit_job_ids=tuple(meta.unsettled_audit_job_ids),
                )
                return await self._execute_clear(ctx=ctx, user_id=user_id, meta=meta, plan=plan)
            return await self._await_inflight_and_clear(ctx=ctx, user_id=user_id, meta=meta)
        finally:
            reset_log_context(log_tokens)

    async def _load_unsettled_writes(
        self,
        *,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        current_time: datetime,
    ) -> list[JobRecord]:
        async with self._session_factory.begin() as session:
            await operations_service.transition_operation_in_session(
                session,
                operation_id=meta.operation_id,
                status="running",
                now=current_time,
            )
            _, unsettled = await cancel_queued_and_list_unsettled_writes_in_session(
                session,
                user_id=user_id,
                provider_id=meta.provider_id,
                max_epoch=meta.previous_epoch,
                cancel_summary="queued memory write cancelled during clear barrier convergence",
                now=current_time,
                include_live_leases=meta.user_cleanup_operation_id is not None,
            )
            return unsettled

    async def _reconcile_verifiable_writes(
        self,
        *,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        unsettled: list[JobRecord],
        current_time: datetime,
    ) -> list[JobRecord]:
        remaining: list[JobRecord] = []
        for write_job in unsettled:
            is_expired_running = (
                write_job.status == "running"
                and write_job.lease_expires_at is not None
                and write_job.lease_expires_at <= current_time
            )
            if write_job.external_ref is None or not (
                write_job.status == "reconciling" or is_expired_running
            ):
                remaining.append(write_job)
                continue

            try:
                status = await self._memory_provider.verify_operation(
                    user_id=user_id, external_ref=write_job.external_ref
                )
            except Exception:
                remaining.append(write_job)
                continue

            if status.status == "succeeded":
                async with self._session_factory.begin() as session:
                    patch: dict[str, Any] = {"dispatch_state": "confirmed_during_clear"}
                    if status.memory_id is not None:
                        patch["memory_id"] = status.memory_id
                    updated = await jobs_service.update_reconciling_job_in_session(
                        session,
                        job_id=write_job.id,
                        allowed_statuses=("reconciling", "running"),
                        error_code="memory_scope_clearing",
                        error_summary="in-flight write confirmed during clear; ready for wipe",
                        metadata_patch=patch,
                        now=current_time,
                    )
                    if updated is None:
                        remaining.append(write_job)
                continue

            if status.status == "failed":
                target_job_id = write_job.id

                def _matches_target(candidate: JobRecord, target: UUID = target_job_id) -> bool:
                    return candidate.id == target and not (
                        candidate.status == "running"
                        and candidate.lease_expires_at is not None
                        and candidate.lease_expires_at > current_time
                    )

                async with self._session_factory.begin() as session:
                    cancelled = await jobs_service.cancel_matching_jobs_in_session(
                        session,
                        user_id=user_id,
                        job_types=(write_job.job_type,),
                        allowed_statuses=("reconciling", "running"),
                        predicate=_matches_target,
                        job_ids=(target_job_id,),
                        error_code="memory_scope_cleared",
                        error_summary="upstream confirmed old-epoch write failed during clear",
                        now=current_time,
                    )
                    for item in cancelled:
                        await fail_superseded_explicit_operation_in_session(
                            session,
                            job=item,
                            provider_id=meta.provider_id,
                            clear_operation_id=meta.operation_id,
                            error_message="memory write failed during scope clearing",
                            now=current_time,
                        )
                    if not cancelled:
                        remaining.append(write_job)
                continue

            remaining.append(write_job)
        return remaining

    async def _await_inflight_and_clear(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
    ) -> JobHandlerOutcome:
        current_time = self._now()
        if meta.user_cleanup_operation_id is not None:
            return await self._await_user_cleanup_writes(
                ctx=ctx, user_id=user_id, meta=meta, current_time=current_time
            )
        unsettled = await self._load_unsettled_writes(
            user_id=user_id, meta=meta, current_time=current_time
        )
        if not unsettled:
            return await self._execute_clear(
                ctx=ctx, user_id=user_id, meta=meta, plan=_ClearExecutionPlan()
            )

        if provider_supports_write_status_verification(self._memory_provider):
            still_unsettled = await self._reconcile_verifiable_writes(
                user_id=user_id, meta=meta, unsettled=unsettled, current_time=current_time
            )
            if not still_unsettled:
                return await self._execute_clear(
                    ctx=ctx, user_id=user_id, meta=meta, plan=_ClearExecutionPlan()
                )

            _log_alert(
                "memory_clear_waiting_inflight",
                "Memory clear waiting for in-flight old-epoch writes to stabilize",
                operation_id=str(meta.operation_id),
                inflight_writes_remaining=len(still_unsettled),
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                error_code="memory_inflight_writes_pending",
                error_summary="waiting for old-epoch in-flight writes to stabilize before clear",
                metadata_patch={"clear_phase": "await_inflight"},
                result_summary={
                    "phase": "await_inflight",
                    "inflight_writes_remaining": len(still_unsettled),
                },
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="reconciling",
                    phase="await_inflight",
                    current_time=current_time,
                    extra_progress={"inflight_writes_remaining": len(still_unsettled)},
                ),
            )

        elapsed = (current_time - meta.barrier_started_at).total_seconds()
        if elapsed < self._inflight_settle_timeout_seconds:
            return JobHandlerOutcome(
                outcome="reconciling",
                error_summary="waiting for in-flight writes within settle timeout window",
                metadata_patch={"clear_phase": "await_inflight"},
                result_summary={
                    "phase": "await_inflight",
                    "inflight_writes_remaining": len(unsettled),
                },
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="running",
                    phase="await_inflight",
                    current_time=current_time,
                    barrier_state="clearing",
                    extra_progress={"inflight_writes_remaining": len(unsettled)},
                ),
            )

        audit_ids = tuple(str(job.id) for job in unsettled)
        _log_alert(
            "memory_clear_settle_timeout_exceeded",
            "In-flight write settle timeout reached; proceeding to clear + delayed compensation",
            operation_id=str(meta.operation_id),
            provider=meta.provider_id,
            unsettled_write_job_ids=list(audit_ids),
            settle_timeout_seconds=self._inflight_settle_timeout_seconds,
        )
        return await self._execute_clear(
            ctx=ctx,
            user_id=user_id,
            meta=meta,
            plan=_ClearExecutionPlan(
                is_compensation=False,
                settle_timeout_exceeded=True,
                unsettled_audit_job_ids=audit_ids,
            ),
        )

    async def _await_user_cleanup_writes(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        current_time: datetime,
    ) -> JobHandlerOutcome:
        async with self._session_factory.begin() as session:
            await operations_service.transition_operation_in_session(
                session, operation_id=meta.operation_id, status="running", now=current_time
            )
            batch = await prepare_user_memory_clear_batch_in_session(
                session,
                user_id=user_id,
                provider_id=meta.provider_id,
                after_job_id=meta.inflight_scan_after_id,
                now=current_time,
            )
        if batch.ready:
            return await self._execute_clear(
                ctx=ctx,
                user_id=user_id,
                meta=meta,
                plan=_ClearExecutionPlan(settle_timeout_exceeded=meta.settle_timeout_exceeded),
            )
        unsettled = [
            job
            for job in batch.writes
            if not is_write_job_settled_for_clear(job)
            and not is_write_job_already_audited_for_clear(job)
        ]
        live_writes = any(
            job.status == "running"
            and job.lease_expires_at is not None
            and job.lease_expires_at > current_time
            for job in batch.writes
        )
        verifiable = provider_supports_write_status_verification(self._memory_provider)
        if unsettled and verifiable and not live_writes:
            unsettled = await self._reconcile_verifiable_writes(
                user_id=user_id, meta=meta, unsettled=unsettled, current_time=current_time
            )
        elapsed = (current_time - meta.barrier_started_at).total_seconds()
        timed_out = (
            bool(unsettled)
            and not verifiable
            and (elapsed >= self._inflight_settle_timeout_seconds)
        )
        can_advance = bool(batch.writes) and not live_writes and (not unsettled or timed_out)
        metadata_patch: dict[str, Any] = {"clear_phase": "await_inflight"}
        if can_advance and timed_out:
            # Keep unknown writes fenced and retained for the parent's bounded
            # settlement after final clear and delayed compensation succeed.
            async with self._session_factory.begin() as session:
                for job in unsettled:
                    updated = await jobs_service.update_reconciling_job_in_session(
                        session,
                        job_id=job.id,
                        metadata_patch={"user_cleanup_clear_operation_id": str(meta.operation_id)},
                        error_code="memory_inflight_settle_timeout",
                        error_summary="unknown write retained until user cleanup confirms",
                        defer_next_run_seconds=AUDIT_RECONCILING_DEFER_SECONDS,
                        now=current_time,
                    )
                    if updated is None:
                        can_advance = False
        if can_advance:
            metadata_patch.update(
                inflight_scan_after_id=str(batch.writes[-1].id),
                settle_timeout_exceeded=meta.settle_timeout_exceeded or timed_out,
            )
        return JobHandlerOutcome(
            outcome="reconciling",
            retry_after_seconds=1 if can_advance else None,
            error_code="memory_inflight_writes_pending" if unsettled or live_writes else None,
            metadata_patch=metadata_patch,
            before_commit=make_hold_barrier_hook(
                user_id=user_id,
                meta=meta,
                op_status="reconciling",
                phase=batch.phase,
                current_time=current_time,
                extra_progress={"inflight_writes_remaining": len(unsettled)},
            ),
        )

    async def _execute_clear(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        plan: _ClearExecutionPlan,
    ) -> JobHandlerOutcome:
        default_ref = build_clear_memories_external_ref(user_id)
        async with self._session_factory.begin() as session:
            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=default_ref,
                metadata_patch={
                    "clear_phase": "clear_dispatched",
                    "verifying_compensation": plan.is_compensation,
                    "settle_timeout_exceeded": plan.settle_timeout_exceeded,
                    "unsettled_audit_job_ids": list(plan.unsettled_audit_job_ids),
                },
            )

        result = await invoke_provider_delete_call(
            lambda: self._memory_provider.clear_memories(user_id=user_id),
            default_external_ref=default_ref,
            provider_id=meta.provider_id,
            operation_id=meta.operation_id,
            job_id=ctx.job.id,
            action_label="clear_memories",
        )
        return self._build_clear_result_outcome(
            ctx=ctx,
            user_id=user_id,
            meta=meta,
            result=result,
            default_ref=default_ref,
            plan=plan,
        )

    async def _verify_clear(
        self,
        *,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        external_ref: str,
    ) -> JobHandlerOutcome:
        try:
            status = await self._memory_provider.verify_operation(
                user_id=user_id, external_ref=external_ref
            )
        except Exception:
            _log_alert(
                "memory_clear_barrier_reconciling",
                "Remote clear verification raised an error; holding barrier reconciling",
                operation_id=str(meta.operation_id),
                external_ref=external_ref,
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=external_ref,
                error_code="memory_verification_error",
                error_summary="clear verification failed; holding barrier reconciling",
                metadata_patch={"clear_phase": "verify_clear"},
                result_summary={"phase": "verify_clear", "provider_id": meta.provider_id},
            )

        resolved_ref = status.external_ref or external_ref
        if status.status == "succeeded":
            return self._build_clear_succeeded_outcome(
                user_id=user_id,
                meta=meta,
                ext_ref=resolved_ref,
                plan=_ClearExecutionPlan(
                    is_compensation=meta.verifying_compensation,
                    settle_timeout_exceeded=meta.settle_timeout_exceeded,
                    unsettled_audit_job_ids=tuple(meta.unsettled_audit_job_ids),
                ),
            )
        if status.status == "failed":
            current_time = self._now()
            retry_phase = (
                "compensation_clear" if meta.verifying_compensation else "execute_final_clear"
            )
            err_code = status.error_code or "memories_still_present"
            return JobHandlerOutcome(
                outcome="retryable",
                external_ref=resolved_ref,
                error_code=err_code,
                error_summary="provider verification found remaining memories; retrying clear",
                metadata_patch={"clear_phase": retry_phase},
                result_summary={"phase": retry_phase, "provider_id": meta.provider_id},
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="reconciling",
                    phase=retry_phase,
                    current_time=current_time,
                    error_code=err_code,
                    error_message="provider verification found remaining memories; retrying clear",
                    fail_op_when_job_exhausted=True,
                    exhausted_error_message="remote clear verification exhausted retries",
                ),
            )

        _log_alert(
            "memory_clear_barrier_reconciling",
            "Remote clear still pending verification; keeping barrier reconciling",
            operation_id=str(meta.operation_id),
            external_ref=resolved_ref,
        )
        return JobHandlerOutcome(
            outcome="reconciling",
            external_ref=resolved_ref,
            error_code=status.error_code,
            error_summary="remote clear still pending verification",
            metadata_patch={"clear_phase": "verify_clear"},
            result_summary={"phase": "verify_clear", "provider_id": meta.provider_id},
        )

    def _build_clear_succeeded_outcome(
        self,
        *,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        ext_ref: str,
        plan: _ClearExecutionPlan,
    ) -> JobHandlerOutcome:
        current_time = self._now()
        if plan.settle_timeout_exceeded and not plan.is_compensation:
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=ext_ref,
                error_code="memory_inflight_settle_timeout",
                error_summary="scheduled delayed secondary compensation clear after settle timeout",
                retry_after_seconds=self._compensation_delay_seconds,
                metadata_patch={
                    "clear_phase": "compensation_clear",
                    "settle_timeout_exceeded": True,
                    "compensation_scheduled": True,
                    "verifying_compensation": False,
                    "unsettled_audit_job_ids": list(plan.unsettled_audit_job_ids),
                },
                result_summary={
                    "phase": "awaiting_compensation_clear",
                    "provider_id": meta.provider_id,
                },
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="reconciling",
                    phase="awaiting_compensation_clear",
                    current_time=current_time,
                    error_code="memory_inflight_settle_timeout",
                    error_message="first clear completed; delayed secondary compensation scheduled",
                    extra_progress={
                        "compensation_scheduled": True,
                        "unsettled_write_job_ids": list(plan.unsettled_audit_job_ids),
                    },
                ),
            )

        parsed_audit_ids = tuple(UUID(raw) for raw in plan.unsettled_audit_job_ids)

        async def _release_barrier(session: AsyncSession, _job: JobRecord) -> None:
            await finalize_clear_barrier_in_session(
                session,
                user_id=user_id,
                provider_id=meta.provider_id,
                operation_id=meta.operation_id,
                tombstone_id=meta.tombstone_id,
                previous_epoch=meta.previous_epoch,
                new_epoch=meta.new_epoch,
                external_ref=ext_ref,
                recalled_memory_marker=self._marker,
                unsettled_audit_job_ids=parsed_audit_ids,
                compensation_executed=plan.is_compensation,
                defer_user_cleanup=meta.user_cleanup_operation_id is not None,
                now=current_time,
            )

        async def _audit_release(_job: JobRecord) -> None:
            await export_deletion_audit_events_safely(
                self._audit_archiver,
                (
                    make_clear_audit_event(
                        "memory_barrier_released",
                        user_id=user_id,
                        operation_id=meta.operation_id,
                        provider_id=meta.provider_id,
                        memory_epoch=meta.new_epoch,
                        barrier_state="idle",
                        status="succeeded",
                        metadata={"compensation_executed": plan.is_compensation},
                    ),
                    make_clear_audit_event(
                        "operation_transitioned",
                        user_id=user_id,
                        operation_id=meta.operation_id,
                        provider_id=meta.provider_id,
                        memory_epoch=meta.new_epoch,
                        barrier_state="idle",
                        status="succeeded",
                    ),
                ),
            )

        return JobHandlerOutcome(
            outcome="succeeded",
            external_ref=ext_ref,
            metadata_patch={"clear_phase": "completed"},
            result_summary={
                "phase": "completed",
                "provider_id": meta.provider_id,
                "compensation_executed": plan.is_compensation,
            },
            before_commit=_release_barrier,
            after_commit=_audit_release,
        )

    def _build_clear_result_outcome(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: MemoryClearJobMetadata,
        result: MemoryDeleteResult,
        default_ref: str,
        plan: _ClearExecutionPlan,
    ) -> JobHandlerOutcome:
        ext_ref = result.external_ref or default_ref
        if result.status == "succeeded":
            return self._build_clear_succeeded_outcome(
                user_id=user_id, meta=meta, ext_ref=ext_ref, plan=plan
            )

        current_time = self._now()
        if result.status in ("accepted", "reconciling"):
            _log_alert(
                "memory_clear_barrier_reconciling",
                "Final remote clear outcome unknown; holding barrier reconciling",
                operation_id=str(meta.operation_id),
                external_ref=ext_ref,
                error_code=result.error_code,
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=ext_ref,
                error_code=result.error_code,
                error_summary="remote clear outcome unknown; awaiting verification",
                metadata_patch={
                    "clear_phase": "verify_clear",
                    "verifying_compensation": plan.is_compensation,
                    "settle_timeout_exceeded": plan.settle_timeout_exceeded,
                    "unsettled_audit_job_ids": list(plan.unsettled_audit_job_ids),
                },
                result_summary={"phase": "verify_clear", "provider_id": meta.provider_id},
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="reconciling",
                    phase="verify_clear",
                    current_time=current_time,
                    error_code=result.error_code,
                    error_message="remote clear outcome unknown; barrier held reconciling",
                    tombstone_external_ref=ext_ref,
                    extra_progress={"external_ref": ext_ref},
                ),
            )

        err_code = result.error_code or "memory_provider_error"
        mapped = map_memory_provider_error(
            err_code, default_message="memory provider failed to clear memories"
        )
        exhausted = (
            err_code == "memory_provider_auth_error"
            or ctx.job.attempt_count >= ctx.job.max_attempts
        )
        _log_alert(
            "memory_clear_barrier_reconciling",
            "Remote clear failed; holding barrier reconciling",
            level=logging.ERROR,
            operation_id=str(meta.operation_id),
            error_code=mapped.code,
            exhausted=exhausted,
        )
        if exhausted:
            retry_phase = "compensation_clear" if plan.is_compensation else "execute_final_clear"
            is_user_cleanup = meta.user_cleanup_operation_id is not None
            failed_phase = "awaiting_retry" if is_user_cleanup else "failed"

            async def _audit_failed(_job: JobRecord) -> None:
                await export_deletion_audit_events_safely(
                    self._audit_archiver,
                    (
                        make_clear_audit_event(
                            "operation_transitioned",
                            user_id=user_id,
                            operation_id=meta.operation_id,
                            provider_id=meta.provider_id,
                            memory_epoch=meta.new_epoch,
                            barrier_state="reconciling",
                            status="reconciling" if is_user_cleanup else "failed",
                            metadata={
                                "error_code": mapped.code,
                                "retry_required": is_user_cleanup,
                            },
                        ),
                    ),
                )

            return JobHandlerOutcome(
                outcome="failed",
                external_ref=ext_ref,
                error_code=mapped.code,
                error_summary="remote clear stopped; barrier held reconciling",
                metadata_patch={
                    "clear_phase": retry_phase if is_user_cleanup else "failed",
                    "settle_timeout_exceeded": plan.settle_timeout_exceeded,
                    "unsettled_audit_job_ids": list(plan.unsettled_audit_job_ids),
                },
                result_summary={"phase": failed_phase, "provider_id": meta.provider_id},
                before_commit=make_hold_barrier_hook(
                    user_id=user_id,
                    meta=meta,
                    op_status="failed",
                    phase="failed",
                    current_time=current_time,
                    error_code=mapped.code,
                    error_message=mapped.message,
                ),
                after_commit=_audit_failed,
            )

        retry_phase = "compensation_clear" if plan.is_compensation else "execute_final_clear"
        return JobHandlerOutcome(
            outcome="retryable",
            external_ref=ext_ref,
            error_code=mapped.code,
            error_summary="remote clear failed transiently; retrying",
            metadata_patch={
                "clear_phase": retry_phase,
                "settle_timeout_exceeded": plan.settle_timeout_exceeded,
                "unsettled_audit_job_ids": list(plan.unsettled_audit_job_ids),
            },
            result_summary={"phase": retry_phase, "provider_id": meta.provider_id},
            before_commit=make_hold_barrier_hook(
                user_id=user_id,
                meta=meta,
                op_status="reconciling",
                phase=retry_phase,
                current_time=current_time,
                error_code=mapped.code,
                error_message="remote clear failed transiently; retrying",
                fail_op_when_job_exhausted=True,
                exhausted_error_message=mapped.message,
            ),
        )
