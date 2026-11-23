"""Atomic account deletion acceptance and durable resource-owner cleanup coordination."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ForbiddenError
from mem_ai.db.base import get_database_now
from mem_ai.modules.agents.service import cleanup_user_agents_in_session
from mem_ai.modules.conversations.cleanup import (
    cleanup_user_conversations_in_session,
    settle_user_generations_in_session,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome, JobRecord
from mem_ai.modules.memories.user_cleanup import (
    begin_user_memory_cleanup_in_session,
    finalize_user_memory_cleanup_in_session,
    inspect_user_memory_cleanup_in_session,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    DeletionAuditEvent,
    export_deletion_audit_events_safely,
)
from mem_ai.modules.operations.schemas import DeletionReceiptResponse
from mem_ai.modules.users import repository, service
from mem_ai.modules.users.schemas import UserRecord

JOB_TYPE_USER_DELETE = "user_delete"


class UserCleanupMetadata(BaseModel):
    """Minimal durable coordinates; credentials and external identities never enter the job."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    operation_id: UUID
    provider_id: str | None = None


async def delete_user_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    settings: Settings,
    memory_provider: MemoryProvider | None = None,
    audit_archiver: DeletionAuditArchiver | None = None,
) -> DeletionReceiptResponse:
    """Fence all future writes and atomically retain identity, operation, receipt and task."""
    async with session_factory.begin() as session:
        user = await repository.get_user_by_id(session, user_id=owner_id, for_update=True)
        if user is None:
            raise ForbiddenError(code="user_not_active", message="user is not active")
        service.assert_user_active(UserRecord.model_validate(user))
        now = await get_database_now(session)
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
                "provider_id": memory_provider.provider_id if memory_provider is not None else None,
            },
            now=now,
        )
        await operations_service.attach_job_in_session(
            session, operation_id=operation.id, job_id=job.id, now=now
        )
    await export_deletion_audit_events_safely(
        audit_archiver,
        [
            DeletionAuditEvent(
                event_type="operation_accepted",
                occurred_at=now,
                user_id=owner_id,
                operation_id=operation.id,
                operation_type=JOB_TYPE_USER_DELETE,
                status=operation.status,
            )
        ],
    )
    return receipt


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
        """Recover from persisted operation progress, including the final user-FK SET NULL."""
        try:
            meta = UserCleanupMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(outcome="failed", error_code="invalid_job_metadata")
        async with self._session_factory() as session:
            operation = await operations_service.get_operation_in_session(
                session, operation_id=meta.operation_id
            )
        if operation is None or operation.operation_type != JOB_TYPE_USER_DELETE:
            return JobHandlerOutcome(outcome="failed", error_code="user_delete_operation_missing")
        if operation.status == "succeeded":
            return JobHandlerOutcome(outcome="succeeded", result_summary={"phase": "succeeded"})
        owner = ctx.job.user_id
        if owner is None or operation.owner_id != owner:
            return JobHandlerOutcome(outcome="failed", error_code="user_delete_owner_missing")
        audit_events: list[DeletionAuditEvent] = []

        async def commit_cleanup(session: AsyncSession, _job: JobRecord) -> None:
            current = await operations_service.get_operation_in_session(
                session, operation_id=meta.operation_id, for_update=True
            )
            if current is None or current.status == "succeeded":
                return
            user = await repository.get_user_by_id(session, user_id=owner)
            if user is None or user.status != "deleting":
                raise RuntimeError("user cleanup lost its deleting root")
            now = await get_database_now(session)
            if current.status == "pending":
                await operations_service.transition_operation_in_session(
                    session, operation_id=current.id, status="running", now=now
                )
            progress = dict(current.progress)
            generations_settled = "provider_operations" in progress or (
                await settle_user_generations_in_session(
                    session, owner_id=owner, batch_size=self._batch_size, now=now
                )
            )
            if not generations_settled:
                progress["phase"] = "await_generation_leases"
            elif "provider_operations" not in progress:
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
                progress.update(
                    phase="remote_memories_pending",
                    provider_operations=plan.provider_operations,
                    local_complete=False,
                    remote_complete=not plan.provider_operations,
                )
            elif progress.get("phase") != "finalizing_identity":
                bindings = dict(progress["provider_operations"])
                memory = await inspect_user_memory_cleanup_in_session(
                    session,
                    user_id=owner,
                    provider_operations=bindings,
                    batch_size=self._batch_size,
                    now=now,
                )
                progress.update(
                    phase=memory.phase,
                    remote_complete=memory.remote_complete,
                    pending_jobs=memory.pending_jobs,
                )
                if memory.remote_complete:
                    if not await cleanup_user_conversations_in_session(
                        session, owner_id=owner, batch_size=self._batch_size, now=now
                    ):
                        progress["phase"] = "local_conversations"
                    elif not await cleanup_user_agents_in_session(
                        session, owner_id=owner, batch_size=self._batch_size
                    ):
                        progress["phase"] = "local_agents"
                    elif not await jobs_service.cleanup_user_jobs_in_session(
                        session,
                        user_id=owner,
                        exclude_job_id=ctx.job.id,
                        batch_size=self._batch_size,
                        now=now,
                    ):
                        progress["phase"] = "local_jobs"
                    elif await finalize_user_memory_cleanup_in_session(
                        session,
                        user_id=owner,
                        provider_operations=bindings,
                        batch_size=self._batch_size,
                        now=now,
                    ):
                        progress["local_complete"] = True
                        progress["phase"] = "finalizing_identity"
            if progress.get("phase") == "finalizing_identity" and (
                await operations_service.finalize_user_operations_in_session(
                    session,
                    owner_id=owner,
                    deletion_operation_id=current.id,
                    batch_size=self._batch_size,
                )
            ):
                await repository.delete_user_and_identities(session, user_id=owner)
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
                    now=now,
                )
                return
            await operations_service.transition_operation_in_session(
                session, operation_id=current.id, status="reconciling", progress=progress, now=now
            )

        async def after_commit(_job: JobRecord) -> None:
            await export_deletion_audit_events_safely(self._audit_archiver, audit_events)

        return JobHandlerOutcome(
            outcome="reconciling",
            retry_after_seconds=1,
            result_summary={"phase": "user_cleanup"},
            before_commit=commit_cleanup,
            after_commit=after_commit,
        )
