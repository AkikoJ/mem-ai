"""Typed background job handlers for asynchronous turn memory writes and reconciliation."""

from __future__ import annotations

import logging
from typing import Any, Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryProvider,
    MemoryScope,
    MemoryWriteInput,
    provider_supports_write_status_verification,
)
from mem_ai.core.exceptions import AppError, ConflictError, ForbiddenError, GatewayTimeoutError
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    JobBeforeCommitHook,
    JobExecutionContext,
    JobHandlerOutcome,
    JobRecord,
)
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    AUDIT_RECONCILING_DEFER_SECONDS,
)
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_CLEAR as JOB_TYPE_MEMORY_CLEAR,
)
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_DELETE as JOB_TYPE_MEMORY_DELETE,
)
from mem_ai.modules.memories.clear_jobs import (
    MemoryClearJobHandler as MemoryClearJobHandler,
)
from mem_ai.modules.memories.deletions import (
    MemoryDeleteJobHandler as MemoryDeleteJobHandler,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories.jobs")

JOB_TYPE_MEMORY_TURN_WRITE = "memory_turn_write"
JOB_TYPE_MEMORY_EXPLICIT_CREATE = "memory_explicit_create"

TurnMemoryDispatchState = Literal[
    "pending",
    "in_flight",
    "accepted",
    "uncertain",
    "retry_write",
    "confirmed",
    "confirmed_during_clear",
]


class TurnMemoryJobMetadata(BaseModel):
    """Validated structured metadata persisted for a `memory_turn_write` job."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    write_id: str | None = None
    provider_id: str
    scope: MemoryScope
    conversation_id: UUID
    generation_id: UUID
    memory_epoch: int = 0
    dispatch_state: TurnMemoryDispatchState = "pending"


class ExplicitMemoryJobMetadata(BaseModel):
    """Validated structured metadata persisted for a `memory_explicit_create` job."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    write_id: str | None = None
    provider_id: str
    memory_epoch: int = 0
    operation_id: UUID | None = None
    dispatch_state: TurnMemoryDispatchState = "pending"


class TurnContentRecord(BaseModel):
    """Read-only turn payload loaded by `TurnContentLoader` for derived memory extraction."""

    model_config = ConfigDict(frozen=True)

    generation_id: UUID
    conversation_id: UUID
    owner_id: UUID
    user_message_content: str
    assistant_message_content: str
    conversation_status: Literal["active", "deleting"]
    generation_status: Literal["running", "succeeded", "failed", "cancelled"]


@runtime_checkable
class TurnContentLoader(Protocol):
    """Seam decoupling `memories` background handlers from `conversations` persistence."""

    async def load_turn_for_memory_write(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
        generation_id: UUID,
    ) -> TurnContentRecord | None:
        """Load committed user/assistant turn contents for an owned succeeded generation."""
        ...

    async def is_conversation_active_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> bool:
        """Verify conversation ownership and `active` status inside caller's transaction."""
        ...


def format_turn_memory_content(user_message: str, assistant_message: str) -> str:
    """Format a completed conversation turn into a provider-agnostic memory payload."""
    return f"User: {user_message.strip()}\nAssistant: {assistant_message.strip()}"


async def _check_post_confirm_validity(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    expected_epoch: int,
    conversation_id: UUID | None = None,
    turn_loader: TurnContentLoader | None = None,
) -> tuple[bool, str | None]:
    """Re-verify user, optional conversation, and `(user, provider)` scope barrier/epoch."""
    async with session_factory() as session:
        try:
            await users_service.ensure_user_active_in_session(session, user_id=user_id)
        except ForbiddenError as exc:
            return False, exc.code

        if conversation_id is not None and turn_loader is not None:
            conv_active = await turn_loader.is_conversation_active_in_session(
                session,
                owner_id=user_id,
                conversation_id=conversation_id,
            )
            if not conv_active:
                return False, "conversation_deleting"

        scope_row = await repository.get_scope(
            session,
            user_id=user_id,
            provider_id=provider_id,
        )
        if scope_row is not None and (
            scope_row.barrier_state != "idle" or scope_row.epoch != expected_epoch
        ):
            return False, "memory_scope_clearing"
    return True, None


async def _finalize_confirmed_outcome(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    expected_epoch: int,
    external_ref: str,
    memory_id: str | None,
    phase: str,
    scope: MemoryScope | None = None,
    conversation_id: UUID | None = None,
    turn_loader: TurnContentLoader | None = None,
    before_commit: JobBeforeCommitHook | None = None,
) -> JobHandlerOutcome:
    """Shared post-confirm validity check converting barrier/delete races into `reconciling`."""
    still_valid, invalid_reason = await _check_post_confirm_validity(
        session_factory,
        user_id=user_id,
        provider_id=provider_id,
        expected_epoch=expected_epoch,
        conversation_id=conversation_id,
        turn_loader=turn_loader,
    )
    patch: dict[str, Any] = {
        "dispatch_state": "confirmed" if still_valid else "confirmed_during_clear",
    }
    if memory_id is not None:
        patch["memory_id"] = memory_id

    if not still_valid:
        return JobHandlerOutcome(
            outcome="reconciling",
            external_ref=external_ref,
            error_code=invalid_reason or "memory_scope_clearing",
            error_summary="memory scope or resource state changed while write was in flight",
            metadata_patch=patch,
            result_summary={"phase": "reconciling", "reason": invalid_reason},
        )

    summary: dict[str, Any] = {"phase": phase, "provider_id": provider_id}
    if scope is not None:
        summary["scope"] = scope
    return JobHandlerOutcome(
        outcome="succeeded",
        external_ref=external_ref,
        metadata_patch=patch,
        result_summary=summary,
        before_commit=before_commit,
    )


def _superseded_audit_outcome(job: JobRecord, *, external_ref: str) -> JobHandlerOutcome:
    return JobHandlerOutcome(
        outcome="reconciling",
        external_ref=external_ref,
        error_code=job.last_error_code or "memory_inflight_settle_timeout",
        error_summary="superseded write job retained in reconciling state for audit",
        retry_after_seconds=AUDIT_RECONCILING_DEFER_SECONDS,
        result_summary={"phase": "reconciling", "reason": "retained_for_clear_audit"},
    )


def _unverifiable_write_outcome(job: JobRecord, *, external_ref: str) -> JobHandlerOutcome:
    return JobHandlerOutcome(
        outcome="reconciling",
        external_ref=external_ref,
        error_code=job.last_error_code or "memory_verification_unsupported",
        error_summary="memory provider does not support per-write status verification",
        result_summary={"phase": "reconciling", "reason": "verification_unsupported"},
    )


async def _handle_confirmed_during_clear(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    expected_epoch: int,
    external_ref: str,
    last_error_code: str | None,
    operation_id: UUID | None = None,
) -> JobHandlerOutcome:
    """Self-cancel a `confirmed_during_clear` write once clear completes, else stay reconciling."""
    async with session_factory.begin() as session:
        scope_row = await repository.get_scope(
            session,
            user_id=user_id,
            provider_id=provider_id,
        )
    if (
        scope_row is not None
        and scope_row.barrier_state == "idle"
        and scope_row.epoch > expected_epoch
    ):

        async def _fail_op_if_pending(session: AsyncSession, _job: JobRecord) -> None:
            if operation_id is None:
                return
            await operations_service.transition_operation_in_session(
                session,
                operation_id=operation_id,
                status="failed",
                progress={"phase": "superseded_by_clear", "provider_id": provider_id},
                error_code="memory_scope_cleared",
                error_message="memory scope was cleared while creation was in flight",
            )

        return JobHandlerOutcome(
            outcome="cancelled",
            external_ref=external_ref,
            error_code="memory_scope_cleared",
            error_summary="memory write superseded by completed scope clear",
            result_summary={"phase": "cancelled", "reason": "memory_scope_cleared"},
            before_commit=_fail_op_if_pending if operation_id is not None else None,
        )

    return JobHandlerOutcome(
        outcome="reconciling",
        external_ref=external_ref,
        error_code=last_error_code or "memory_scope_clearing",
        error_summary="confirmed write awaiting clearing barrier convergence",
        result_summary={"phase": "reconciling", "reason": "awaiting_clearing_convergence"},
    )


class TurnMemoryWriteJobHandler:
    """Executes `memory_turn_write` jobs via `MemoryProvider` with epoch/barrier guards."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        memory_provider: MemoryProvider,
        turn_loader: TurnContentLoader,
    ) -> None:
        self._session_factory = session_factory
        self._memory_provider = memory_provider
        self._turn_loader = turn_loader

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Dispatch or verify one durable `memory_turn_write` attempt."""
        raw_meta = dict(ctx.job.metadata_json)
        if "generation_id" not in raw_meta and ctx.job.business_id is not None:
            raw_meta["generation_id"] = ctx.job.business_id

        try:
            meta = TurnMemoryJobMetadata.model_validate(raw_meta)
        except ValidationError:
            return JobHandlerOutcome(
                outcome="failed",
                error_code="invalid_job_metadata",
                error_summary="turn memory job metadata is incomplete or invalid",
                result_summary={"phase": "failed", "reason": "invalid_metadata"},
            )

        write_id = meta.write_id or f"mtw_{ctx.job.id.hex}"
        log_tokens = bind_log_context(
            user_id=str(ctx.job.user_id) if ctx.job.user_id is not None else None,
            conversation_id=str(meta.conversation_id),
            generation_id=str(meta.generation_id),
            provider=meta.provider_id or self._memory_provider.provider_id,
            job_id=str(ctx.job.id),
        )
        try:
            if ctx.job.user_id is None:
                return JobHandlerOutcome(
                    outcome="cancelled",
                    error_code="user_deleted",
                    error_summary="owning user no longer exists",
                    result_summary={"phase": "cancelled", "reason": "user_deleted"},
                )
            if meta.provider_id != self._memory_provider.provider_id:
                return JobHandlerOutcome(
                    outcome="failed",
                    error_code="memory_provider_mismatch",
                    error_summary="job provider does not match configured memory provider",
                    result_summary={"phase": "failed", "reason": "provider_mismatch"},
                )

            if ctx.job.metadata_json.get("superseded_by_clear_operation_id"):
                return _superseded_audit_outcome(
                    ctx.job, external_ref=ctx.job.external_ref or write_id
                )

            if meta.dispatch_state == "confirmed_during_clear":
                return await _handle_confirmed_during_clear(
                    self._session_factory,
                    user_id=ctx.job.user_id,
                    provider_id=meta.provider_id,
                    expected_epoch=meta.memory_epoch,
                    external_ref=ctx.job.external_ref or write_id,
                    last_error_code=ctx.job.last_error_code,
                )

            if (
                meta.dispatch_state in ("in_flight", "accepted", "uncertain")
                and ctx.job.external_ref is not None
            ):
                return await self._verify_dispatched_write(
                    ctx=ctx,
                    user_id=ctx.job.user_id,
                    meta=meta,
                    external_ref=ctx.job.external_ref,
                )

            return await self._dispatch_turn_write(
                ctx=ctx,
                user_id=ctx.job.user_id,
                meta=meta,
                write_id=write_id,
            )
        finally:
            reset_log_context(log_tokens)

    async def _verify_dispatched_write(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: TurnMemoryJobMetadata,
        external_ref: str,
    ) -> JobHandlerOutcome:
        if not provider_supports_write_status_verification(self._memory_provider):
            return _unverifiable_write_outcome(ctx.job, external_ref=external_ref)
        try:
            status = await self._memory_provider.verify_operation(
                user_id=user_id,
                external_ref=external_ref,
            )
        except Exception:
            logger.warning(
                "Memory operation verification raised an error; keeping job reconciling",
                extra={
                    "extra_fields": {
                        "external_ref": external_ref,
                        "attempt_number": ctx.attempt_number,
                    }
                },
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=external_ref,
                error_code="memory_verification_error",
                error_summary="external memory verification failed; will re-verify",
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
            )

        resolved_ext_ref = status.external_ref or external_ref
        if status.status == "succeeded":
            return await _finalize_confirmed_outcome(
                self._session_factory,
                user_id=user_id,
                provider_id=meta.provider_id,
                expected_epoch=meta.memory_epoch,
                external_ref=resolved_ext_ref,
                memory_id=status.memory_id,
                phase="verified",
                scope=meta.scope,
                conversation_id=meta.conversation_id,
                turn_loader=self._turn_loader,
            )

        if status.status == "failed":
            return JobHandlerOutcome(
                outcome="retryable",
                external_ref=resolved_ext_ref,
                error_code=status.error_code or "memory_provider_error",
                error_summary="provider confirmed previous memory write failed",
                metadata_patch={"dispatch_state": "retry_write"},
                result_summary={"phase": "verify_failed", "provider_id": meta.provider_id},
            )

        return JobHandlerOutcome(
            outcome="reconciling",
            external_ref=resolved_ext_ref,
            error_code=status.error_code,
            error_summary="external memory write outcome still pending verification",
            result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
        )

    async def _dispatch_turn_write(
        self,
        *,
        ctx: JobExecutionContext,
        user_id: UUID,
        meta: TurnMemoryJobMetadata,
        write_id: str,
    ) -> JobHandlerOutcome:
        turn = await self._turn_loader.load_turn_for_memory_write(
            owner_id=user_id,
            conversation_id=meta.conversation_id,
            generation_id=meta.generation_id,
        )
        if (
            turn is None
            or turn.conversation_status != "active"
            or turn.generation_status != "succeeded"
        ):
            return JobHandlerOutcome(
                outcome="cancelled",
                error_code="turn_not_writable",
                error_summary="conversation turn is missing, deleting, or not succeeded",
                result_summary={"phase": "cancelled", "reason": "turn_not_writable"},
            )

        # Short Tx: lock order users -> conversations (FOR SHARE) -> memory_scopes (FOR UPDATE)
        # -> jobs (FOR UPDATE) to serialize cleanly with clearing barriers and deletions (§11.2).
        async with self._session_factory.begin() as session:
            try:
                await users_service.ensure_user_active_in_session(
                    session,
                    user_id=user_id,
                    for_share=True,
                )
            except ForbiddenError as exc:
                return JobHandlerOutcome(
                    outcome="cancelled",
                    error_code=exc.code,
                    error_summary="user is no longer active",
                    result_summary={"phase": "cancelled", "reason": exc.code},
                )

            conv_active = await self._turn_loader.is_conversation_active_in_session(
                session,
                owner_id=user_id,
                conversation_id=meta.conversation_id,
            )
            if not conv_active:
                return JobHandlerOutcome(
                    outcome="cancelled",
                    error_code="conversation_deleting",
                    error_summary="conversation is missing or deleting",
                    result_summary={"phase": "cancelled", "reason": "conversation_deleting"},
                )

            scope_row = await repository.get_or_create_scope_for_update(
                session,
                user_id=user_id,
                provider_id=meta.provider_id,
            )
            if scope_row.barrier_state != "idle" or scope_row.epoch != meta.memory_epoch:
                return JobHandlerOutcome(
                    outcome="cancelled",
                    error_code="memory_scope_superseded",
                    error_summary="memory epoch advanced or clearing barrier active",
                    result_summary={"phase": "cancelled", "reason": "memory_scope_superseded"},
                )

            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=write_id,
                metadata_patch={"dispatch_state": "in_flight"},
            )

        write_input = MemoryWriteInput(
            write_id=write_id,
            user_id=user_id,
            scope=meta.scope,
            content=format_turn_memory_content(
                turn.user_message_content,
                turn.assistant_message_content,
            ),
            conversation_id=meta.conversation_id if meta.scope == "conversation" else None,
            memory_epoch=meta.memory_epoch,
        )

        try:
            result = await self._memory_provider.write_turn_memory(write_input)
        except GatewayTimeoutError as exc:
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=write_id,
                error_code=exc.code,
                error_summary="memory provider timed out after dispatch; verification pending",
                metadata_patch={"dispatch_state": "uncertain"},
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
            )
        except AppError as exc:
            return JobHandlerOutcome(
                outcome="retryable",
                external_ref=write_id,
                error_code=exc.code,
                error_summary="memory provider returned retryable error",
                metadata_patch={"dispatch_state": "retry_write"},
                result_summary={"phase": "retryable_error", "provider_id": meta.provider_id},
            )
        except Exception:
            logger.warning(
                "Unexpected exception after turn memory write dispatch; marking reconciling",
                extra={
                    "extra_fields": {
                        "external_ref": write_id,
                        "scope": meta.scope,
                        "attempt_number": ctx.attempt_number,
                    }
                },
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=write_id,
                error_code="memory_provider_error",
                error_summary="unexpected provider error after dispatch; verification pending",
                metadata_patch={"dispatch_state": "uncertain"},
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
            )

        resolved_ext_ref = result.external_ref or write_id
        if result.status == "succeeded":
            return await _finalize_confirmed_outcome(
                self._session_factory,
                user_id=user_id,
                provider_id=meta.provider_id,
                expected_epoch=meta.memory_epoch,
                external_ref=resolved_ext_ref,
                memory_id=result.memory_id,
                phase="succeeded",
                scope=meta.scope,
                conversation_id=meta.conversation_id,
                turn_loader=self._turn_loader,
            )

        if result.status == "accepted":
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=resolved_ext_ref,
                error_summary="provider accepted async memory write; verification pending",
                metadata_patch={"dispatch_state": "accepted"},
                result_summary={
                    "phase": "accepted",
                    "provider_id": meta.provider_id,
                    "scope": meta.scope,
                },
            )

        if result.status == "reconciling":
            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=resolved_ext_ref,
                error_code=result.error_code or "memory_provider_timeout",
                error_summary="memory provider response lost; verification pending",
                metadata_patch={"dispatch_state": "uncertain"},
                result_summary={
                    "phase": "reconciling",
                    "provider_id": meta.provider_id,
                    "scope": meta.scope,
                },
            )

        return JobHandlerOutcome(
            outcome="retryable",
            external_ref=resolved_ext_ref,
            error_code=result.error_code or "memory_provider_error",
            error_summary="memory provider rejected turn write attempt",
            metadata_patch={"dispatch_state": "retry_write"},
            result_summary={
                "phase": "failed_attempt",
                "provider_id": meta.provider_id,
                "scope": meta.scope,
            },
        )


class ExplicitMemoryReconcileJobHandler:
    """Verifies `reconciling` `memory_explicit_create` jobs and advances `operations` atomically."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        memory_provider: MemoryProvider,
    ) -> None:
        self._session_factory = session_factory
        self._memory_provider = memory_provider

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Verify an async or uncertain explicit memory write with atomic operation transition."""
        try:
            meta = ExplicitMemoryJobMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(
                outcome="failed",
                error_code="invalid_job_metadata",
                error_summary="explicit memory job metadata is invalid",
            )

        external_ref = ctx.job.external_ref or meta.write_id or ctx.job.business_id or ""
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
            if meta.provider_id != self._memory_provider.provider_id or not external_ref:
                return JobHandlerOutcome(
                    outcome="failed",
                    error_code="memory_provider_mismatch",
                    error_summary="explicit memory job provider or external_ref is invalid",
                )
            if ctx.job.metadata_json.get("superseded_by_clear_operation_id"):
                return _superseded_audit_outcome(ctx.job, external_ref=external_ref)
            if meta.dispatch_state == "confirmed_during_clear":
                return await _handle_confirmed_during_clear(
                    self._session_factory,
                    user_id=ctx.job.user_id,
                    provider_id=meta.provider_id,
                    expected_epoch=meta.memory_epoch,
                    external_ref=external_ref,
                    last_error_code=ctx.job.last_error_code,
                    operation_id=meta.operation_id,
                )

            if not provider_supports_write_status_verification(self._memory_provider):
                return _unverifiable_write_outcome(ctx.job, external_ref=external_ref)

            try:
                status = await self._memory_provider.verify_operation(
                    user_id=ctx.job.user_id,
                    external_ref=external_ref,
                )
            except ConflictError:
                raise
            except Exception:
                return JobHandlerOutcome(
                    outcome="reconciling",
                    external_ref=external_ref,
                    error_code="memory_verification_error",
                    error_summary="explicit memory verification failed; will retry",
                )

            resolved_ext_ref = status.external_ref or external_ref
            if status.status == "succeeded":
                op_id = meta.operation_id
                provider_id = meta.provider_id
                memory_id = status.memory_id

                async def _commit_op_succeeded(session: AsyncSession, _job: JobRecord) -> None:
                    if op_id is None:
                        return
                    await operations_service.transition_operation_in_session(
                        session,
                        operation_id=op_id,
                        status="succeeded",
                        progress={
                            "phase": "succeeded",
                            "provider_id": provider_id,
                            "external_ref": resolved_ext_ref,
                        },
                        result_summary={"memory_id": memory_id} if memory_id else {},
                    )

                return await _finalize_confirmed_outcome(
                    self._session_factory,
                    user_id=ctx.job.user_id,
                    provider_id=provider_id,
                    expected_epoch=meta.memory_epoch,
                    external_ref=resolved_ext_ref,
                    memory_id=memory_id,
                    phase="verified",
                    before_commit=_commit_op_succeeded if op_id is not None else None,
                )

            if status.status == "failed":
                op_id = meta.operation_id
                provider_id = meta.provider_id
                err_code = status.error_code or "memory_provider_error"

                async def _commit_op_failed(session: AsyncSession, _job: JobRecord) -> None:
                    if op_id is None:
                        return
                    await operations_service.transition_operation_in_session(
                        session,
                        operation_id=op_id,
                        status="failed",
                        progress={
                            "phase": "failed",
                            "provider_id": provider_id,
                            "external_ref": resolved_ext_ref,
                        },
                        error_code=err_code,
                        error_message="explicit memory creation failed upstream",
                    )

                return JobHandlerOutcome(
                    outcome="failed",
                    external_ref=resolved_ext_ref,
                    error_code=err_code,
                    error_summary="provider confirmed explicit memory write failed",
                    result_summary={"phase": "failed", "provider_id": provider_id},
                    before_commit=_commit_op_failed if op_id is not None else None,
                )

            return JobHandlerOutcome(
                outcome="reconciling",
                external_ref=resolved_ext_ref,
                error_code=status.error_code,
                error_summary="explicit memory operation still pending verification",
                result_summary={"phase": "reconciling", "provider_id": meta.provider_id},
            )
        finally:
            reset_log_context(log_tokens)
