"""Atomic account deletion acceptance and durable resource-owner cleanup coordination."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ForbiddenError
from mem_ai.db.base import get_database_now
from mem_ai.modules.agents.service import cleanup_user_agents_in_session
from mem_ai.modules.auth import service as auth_service
from mem_ai.modules.conversations.cleanup import (
    cleanup_user_conversations_in_session,
    settle_user_conversation_operations_in_session,
    settle_user_generations_in_session,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome, JobRecord
from mem_ai.modules.memories.user_cleanup import (
    begin_user_memory_cleanup_in_session,
    cleanup_user_memory_snapshots_in_session,
    finalize_user_memory_cleanup_in_session,
    inspect_user_memory_cleanup_in_session,
    peek_user_memory_cleanup_in_session,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    DeletionAuditEvent,
    RecoveryReplayStatus,
    export_deletion_audit_events_safely,
)
from mem_ai.modules.operations.schemas import DeletionReceiptResponse, OperationRecord
from mem_ai.modules.users import repository, service
from mem_ai.modules.users.schemas import UserRecord

logger = logging.getLogger("mem_ai.users.cleanup")

JOB_TYPE_USER_DELETE = "user_delete"
_STALLED_REMOTE_PHASES = frozenset({"awaiting_retry", "provider_unavailable"})
_STALLED_RECONCILE_DELAY_SECONDS = 60.0


class UserCleanupMetadata(BaseModel):
    """Minimal durable coordinates; credentials and external identities never enter the job."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    operation_id: UUID
    provider_id: str | None = None
    provider_operations: dict[str, str] | None = None
    local_complete: bool = False
    finalizing_identity: bool = False


async def _accept_user_deletion_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    settings: Settings,
    provider_id: str | None,
    recovery_replay: bool = False,
) -> tuple[OperationRecord, DeletionReceiptResponse, datetime] | RecoveryReplayStatus:
    user = await repository.get_user_by_id(session, user_id=owner_id, for_update=True)
    if user is None:
        if recovery_replay:
            return "skipped_missing"
        raise ForbiddenError(code="user_not_active", message="user is not active")
    if not recovery_replay:
        service.assert_user_active(UserRecord.model_validate(user))
    now = await get_database_now(session)
    await auth_service.revoke_user_sessions_in_session(session, user_id=owner_id, revoked_at=now)
    if recovery_replay and user.status == "deleting":
        return "already_applied"
    user.status = "deleting"
    user.deleted_at = now
    user.updated_at = now
    operation, receipt = await operations_service.create_user_deletion_operation_in_session(
        session, owner_id=owner_id, settings=settings, now=now
    )
    job, _ = await jobs_service.enqueue_job_in_session(
        session,
        job_type=JOB_TYPE_USER_DELETE,
        business_id=str(operation.id),
        user_id=owner_id,
        dedupe_key=f"{JOB_TYPE_USER_DELETE}:{operation.id}",
        metadata_json={
            "operation_id": str(operation.id),
            "provider_id": provider_id,
        },
        now=now,
    )
    await operations_service.attach_job_in_session(
        session, operation_id=operation.id, job_id=job.id, now=now
    )
    return operation, receipt, now


async def delete_user_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    settings: Settings,
    memory_provider: MemoryProvider | None = None,
    audit_archiver: DeletionAuditArchiver | None = None,
) -> DeletionReceiptResponse:
    """Fence all future writes and atomically retain identity, operation, receipt and task."""
    provider_id = memory_provider.provider_id if memory_provider is not None else None
    async with session_factory.begin() as session:
        accepted = await _accept_user_deletion_in_session(
            session,
            owner_id=owner_id,
            settings=settings,
            provider_id=provider_id,
            recovery_replay=False,
        )
        assert isinstance(accepted, tuple)
        operation, receipt, now = accepted
    await export_deletion_audit_events_safely(
        audit_archiver,
        [
            DeletionAuditEvent(
                event_type="operation_accepted",
                occurred_at=now,
                user_id=owner_id,
                operation_id=operation.id,
                operation_type=JOB_TYPE_USER_DELETE,
                target_type="user",
                status=operation.status,
            )
        ],
    )
    return receipt


async def replay_user_deletion_for_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    settings: Settings,
    memory_provider: MemoryProvider | None = None,
    provider_id: str | None = None,
) -> RecoveryReplayStatus:
    """Idempotently replay a post-backup user deletion without re-emitting audit events."""
    resolved_provider_id = (
        memory_provider.provider_id if memory_provider is not None else provider_id
    )
    async with session_factory.begin() as session:
        outcome = await _accept_user_deletion_in_session(
            session,
            owner_id=owner_id,
            settings=settings,
            provider_id=resolved_provider_id,
            recovery_replay=True,
        )
        return "enqueued" if isinstance(outcome, tuple) else outcome


async def _load_deleting_context_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    operation_id: UUID,
) -> tuple[OperationRecord, datetime] | None:
    """Lock the user deletion operation and verify its deleting user root."""
    current = await operations_service.get_operation_in_session(
        session, operation_id=operation_id, for_update=True
    )
    if current is None or current.status == "succeeded":
        return None
    user = await repository.get_user_by_id(session, user_id=owner_id)
    if user is None or user.status != "deleting":
        raise RuntimeError("user cleanup lost its deleting root")
    return current, await get_database_now(session)


class UserCleanupJobHandler:
    """Advance bounded database phases only while the generic job lease fences the commit."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: int = 50,
        audit_archiver: DeletionAuditArchiver | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = max(1, min(100, batch_size))
        self._audit_archiver = audit_archiver

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Recover from persisted metadata/progress and advance one bounded cleanup phase."""
        try:
            meta = UserCleanupMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(outcome="failed", error_code="invalid_job_metadata")
        owner = ctx.job.user_id
        can_finalize_identity = False
        retry_after: float | None = 1.0
        async with self._session_factory() as session:
            operation = await operations_service.get_operation_in_session(
                session, operation_id=meta.operation_id
            )
            if operation is None or operation.operation_type != JOB_TYPE_USER_DELETE:
                return JobHandlerOutcome(
                    outcome="failed", error_code="user_delete_operation_missing"
                )
            if operation.status == "succeeded":
                return JobHandlerOutcome(outcome="succeeded", result_summary={"phase": "succeeded"})
            if owner is None or operation.owner_id != owner:
                return JobHandlerOutcome(outcome="failed", error_code="user_delete_owner_missing")
            if meta.finalizing_identity:
                can_finalize_identity = (
                    await operations_service.can_finalize_user_operations_in_batch_in_session(
                        session,
                        owner_id=owner,
                        deletion_operation_id=meta.operation_id,
                        batch_size=self._batch_size,
                    )
                )
            elif meta.provider_operations is not None and meta.local_complete:
                peek = await peek_user_memory_cleanup_in_session(
                    session, user_id=owner, provider_operations=meta.provider_operations
                )
                if peek is not None:
                    if peek.retry_required or peek.phase in _STALLED_REMOTE_PHASES:
                        retry_after = _STALLED_RECONCILE_DELAY_SECONDS
                    else:
                        retry_after = None

        audit_events: list[DeletionAuditEvent] = []

        async def after_commit(_job: JobRecord) -> None:
            await export_deletion_audit_events_safely(self._audit_archiver, audit_events)

        if can_finalize_identity:

            async def commit_final_identity(session: AsyncSession, _job: JobRecord) -> None:
                loaded = await _load_deleting_context_in_session(
                    session, owner_id=owner, operation_id=meta.operation_id
                )
                if loaded is None:
                    return
                current, now = loaded
                if not await operations_service.finalize_user_operations_in_session(
                    session,
                    owner_id=owner,
                    deletion_operation_id=current.id,
                    batch_size=self._batch_size,
                ):
                    raise RuntimeError("user child operations were not settled before finalization")
                await repository.delete_deleting_user(session, user_id=owner)
                await operations_service.transition_operation_in_session(
                    session,
                    operation_id=current.id,
                    status="succeeded",
                    progress={
                        "phase": "succeeded",
                        "local_complete": True,
                        "remote_complete": True,
                    },
                    result_summary={"deleted": True},
                    error_code=None,
                    error_message=None,
                    now=now,
                )
                audit_events.append(
                    DeletionAuditEvent(
                        event_type="operation_transitioned",
                        occurred_at=now,
                        user_id=owner,
                        operation_id=current.id,
                        operation_type=JOB_TYPE_USER_DELETE,
                        target_type="user",
                        status="succeeded",
                    )
                )

            return JobHandlerOutcome(
                outcome="succeeded",
                result_summary={"phase": "succeeded"},
                before_commit=commit_final_identity,
                after_commit=after_commit,
            )

        async def commit_cleanup(session: AsyncSession, updated_job: JobRecord) -> None:
            loaded = await _load_deleting_context_in_session(
                session, owner_id=owner, operation_id=meta.operation_id
            )
            if loaded is None:
                return
            current, now = loaded
            if current.status == "pending":
                await operations_service.transition_operation_in_session(
                    session, operation_id=current.id, status="running", now=now
                )

            bindings = (
                dict(meta.provider_operations) if meta.provider_operations is not None else None
            )
            local_complete = meta.local_complete
            finalizing_identity = meta.finalizing_identity
            remote_complete = bool(current.progress.get("remote_complete", False))
            phase = str(current.progress.get("phase", "await_generation_leases"))
            pending_jobs = 0
            retry_required = False
            error_code: str | None = None
            error_message: str | None = None
            job_meta_patch: dict[str, Any] = {}

            if bindings is None:
                if not await settle_user_generations_in_session(
                    session, owner_id=owner, batch_size=self._batch_size, now=now
                ):
                    phase = "await_generation_leases"
                else:
                    # No user FOR UPDATE here: memory handlers use job -> user FOR SHARE.
                    # The committed, irreversible deleting state already rejects new writes.
                    plan = await begin_user_memory_cleanup_in_session(
                        session,
                        user_id=owner,
                        operation_id=current.id,
                        provider_id=meta.provider_id,
                        now=now,
                    )
                    audit_events.extend(plan.audit_events)
                    bindings = dict(plan.provider_operations)
                    job_meta_patch["provider_operations"] = bindings
                    remote_complete = not bindings
                    phase = "local_conversations"

            if bindings is not None and not local_complete and not finalizing_identity:
                if not await cleanup_user_conversations_in_session(
                    session, owner_id=owner, batch_size=self._batch_size, now=now
                ):
                    phase = "local_conversations"
                elif not await cleanup_user_agents_in_session(
                    session, owner_id=owner, batch_size=self._batch_size
                ):
                    phase = "local_agents"
                elif not await cleanup_user_memory_snapshots_in_session(
                    session, user_id=owner, batch_size=self._batch_size
                ):
                    phase = "local_memory_snapshots"
                else:
                    local_complete = True
                    job_meta_patch["local_complete"] = True

            if bindings is not None and local_complete and not finalizing_identity:
                memory = await inspect_user_memory_cleanup_in_session(
                    session,
                    user_id=owner,
                    provider_operations=bindings,
                    batch_size=self._batch_size,
                    now=now,
                )
                phase = memory.phase
                remote_complete = memory.remote_complete
                pending_jobs = memory.pending_jobs
                retry_required = memory.retry_required
                error_code = memory.error_code
                error_message = memory.error_message
                if not memory.remote_complete:
                    if retry_required or phase in _STALLED_REMOTE_PHASES:
                        logger.warning(
                            "User deletion remote memory cleanup requires operator attention",
                            extra={
                                "extra_fields": {
                                    "alert": "user_delete_remote_cleanup_blocked",
                                    "user_id": str(owner),
                                    "operation_id": str(current.id),
                                    "phase": phase,
                                    "error_code": error_code,
                                    "pending_jobs": pending_jobs,
                                }
                            },
                        )
                elif not await settle_user_conversation_operations_in_session(
                    session, owner_id=owner, batch_size=self._batch_size, now=now
                ):
                    phase = "settling_conversation_operations"
                elif not await jobs_service.cleanup_user_jobs_in_session(
                    session,
                    user_id=owner,
                    exclude_job_id=ctx.job.id,
                    batch_size=self._batch_size,
                    now=now,
                ):
                    phase = "local_jobs"
                elif not await finalize_user_memory_cleanup_in_session(
                    session,
                    user_id=owner,
                    provider_operations=bindings,
                    batch_size=self._batch_size,
                    now=now,
                ):
                    phase = "local_memory_state"
                else:
                    finalizing_identity = True
                    job_meta_patch["finalizing_identity"] = True
                    phase = "finalizing_identity"

            if finalizing_identity:
                remote_complete = True
                local_complete = True
                phase = "finalizing_identity"
                await operations_service.finalize_user_operations_in_session(
                    session,
                    owner_id=owner,
                    deletion_operation_id=current.id,
                    batch_size=self._batch_size,
                )

            if job_meta_patch:
                await jobs_service.patch_job_metadata_in_session(
                    session, job_id=updated_job.id, metadata_patch=job_meta_patch, now=now
                )

            progress_payload: dict[str, Any] = {
                "phase": phase,
                "local_complete": local_complete,
                "remote_complete": remote_complete,
            }
            if pending_jobs:
                progress_payload["pending_jobs"] = pending_jobs
            if retry_required:
                progress_payload["retry_required"] = True

            await operations_service.transition_operation_in_session(
                session,
                operation_id=current.id,
                status="reconciling",
                progress=progress_payload,
                error_code=error_code,
                error_message=error_message,
                now=now,
            )

        return JobHandlerOutcome(
            outcome="reconciling",
            retry_after_seconds=retry_after,
            result_summary={"phase": "user_cleanup"},
            before_commit=commit_cleanup,
            after_commit=after_commit,
        )
