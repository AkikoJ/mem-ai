"""Owned conversation deletion, generation settlement, and resumable bounded cleanup."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.exceptions import NotFoundError
from mem_ai.db.base import get_database_now
from mem_ai.modules.conversations import cleanup_repository, generation, repository
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome, JobRecord
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import DeletionAuditArchiver
from mem_ai.modules.operations.schemas import OperationRecord
from mem_ai.modules.users import service as users_service

JOB_TYPE_CONVERSATION_DELETE = "conversation_delete"


class ConversationCleanupMetadata(BaseModel):
    """Durable range inventory and bounded remote continuation for conversation deletion."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    operation_id: UUID
    conversation_id: UUID
    provider_ids: tuple[str, ...] = ()
    provider_index: int = 0
    pending_memory_ids: tuple[str, ...] = ()


async def delete_conversation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    memory_provider: MemoryProvider | None,
) -> OperationRecord:
    """Atomically hide an owned conversation and enqueue exactly one effective operation."""
    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id, for_share=True)
        conversation = await repository.get_conversation_for_update(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
        existing = await operations_service.find_operation_for_target_in_session(
            session,
            owner_id=owner_id,
            operation_type=JOB_TYPE_CONVERSATION_DELETE,
            target_id=str(conversation_id),
        )
        if existing is not None:
            return existing
        if conversation is None:
            raise NotFoundError(code="conversation_not_found", message="conversation not found")
        now = await get_database_now(session)
        conversation.status = "deleting"
        conversation.deleted_at = now
        conversation.updated_at = now
        operation = await operations_service.create_operation_in_session(
            session,
            owner_id=owner_id,
            operation_type=JOB_TYPE_CONVERSATION_DELETE,
            status="running",
            target_type="conversation",
            target_id=str(conversation_id),
            progress={"phase": "settling_generation"},
        )
        providers = await memories_service.begin_conversation_cleanup_in_session(
            session,
            user_id=owner_id,
            conversation_id=conversation_id,
            operation_id=operation.id,
            provider_id=memory_provider.provider_id if memory_provider is not None else None,
        )
        job, _ = await jobs_service.enqueue_job_in_session(
            session,
            job_type=JOB_TYPE_CONVERSATION_DELETE,
            business_id=str(conversation_id),
            user_id=owner_id,
            dedupe_key=f"{JOB_TYPE_CONVERSATION_DELETE}:{conversation_id}",
            metadata_json={
                "conversation_id": str(conversation_id),
                "operation_id": str(operation.id),
                "provider_ids": providers,
            },
            now=now,
        )
        return (
            await operations_service.attach_job_in_session(
                session,
                operation_id=operation.id,
                job_id=job.id,
                now=now,
            )
            or operation
        )


class ConversationCleanupJobHandler:
    """Wait for leases/writes, clean remote ranges, then delete the local graph in batches."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        memory_provider: MemoryProvider | None = None,
        batch_size: int = 50,
        audit_archiver: DeletionAuditArchiver | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._memory_provider = memory_provider
        self._batch_size = max(1, min(100, batch_size))
        self._audit_archiver = audit_archiver

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Do at most one bounded phase; final mutations share the generic job token commit."""
        try:
            meta = ConversationCleanupMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(outcome="failed", error_code="invalid_job_metadata")
        owner = ctx.job.user_id
        if owner is None:
            return JobHandlerOutcome(outcome="cancelled", error_code="user_deleted")
        phase = "settling_generation"
        patch: dict[str, object] = {}
        ready = False
        async with self._session_factory.begin() as session:
            # Match the job outcome hook's lock order, including after lease reclamation.
            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=f"conversation:{meta.conversation_id}",
            )
            conversation = await repository.get_conversation_for_update(
                session,
                owner_id=owner,
                conversation_id=meta.conversation_id,
            )
            now = await get_database_now(session)
            if conversation is None or conversation.status != "deleting":
                return JobHandlerOutcome(outcome="failed", error_code="conversation_not_deleting")
            running = await repository.get_running_generation_for_conversation(
                session,
                conversation_id=meta.conversation_id,
            )
            if (
                running is not None
                and running.lease_expires_at is not None
                and running.lease_expires_at <= now
            ):
                await generation.converge_generation_failure_in_session(
                    session,
                    owner_id=owner,
                    conversation_id=meta.conversation_id,
                    generation_id=running.id,
                    lease_token=None,
                    error_code="generation_lease_lost",
                    error_message="generation lease expired",
                    current_time=now,
                    expired_only=True,
                )
                running = None
            if running is None:
                phase = "settling_memory_writes"
                ready = await memories_service.settle_conversation_writes_in_session(
                    session,
                    user_id=owner,
                    conversation_id=meta.conversation_id,
                    operation_id=meta.operation_id,
                    batch_size=self._batch_size,
                    now=now,
                )
        completed = False
        local_batch = False
        if ready and meta.provider_index < len(meta.provider_ids):
            batch = await memories_service.clean_conversation_memory_batch(
                self._session_factory,
                ctx=ctx,
                memory_provider=self._memory_provider,
                user_id=owner,
                conversation_id=meta.conversation_id,
                provider_id=meta.provider_ids[meta.provider_index],
                pending_memory_ids=meta.pending_memory_ids,
                batch_size=self._batch_size,
                audit_archiver=self._audit_archiver,
            )
            phase = batch.phase
            patch = {"pending_memory_ids": list(batch.pending_memory_ids)}
            if batch.provider_complete:
                patch["provider_index"] = meta.provider_index + 1
        elif ready:
            phase = "local_data"
            local_batch = True
            async with self._session_factory() as session:
                completed = not await cleanup_repository.has_children(
                    session,
                    conversation_id=meta.conversation_id,
                )

        async def commit_cleanup(session: AsyncSession, _job: JobRecord) -> None:
            row = await repository.get_conversation_for_update(
                session,
                owner_id=owner,
                conversation_id=meta.conversation_id,
            )
            if row is None or row.status != "deleting":
                raise RuntimeError("conversation cleanup lost its deleting root")
            if completed:
                await memories_service.finish_conversation_cleanup_in_session(
                    session,
                    user_id=owner,
                    conversation_id=meta.conversation_id,
                    now=datetime.now(UTC),
                )
                await cleanup_repository.delete_empty_conversation(
                    session,
                    conversation_id=meta.conversation_id,
                )
            elif local_batch:
                await cleanup_repository.delete_batch(
                    session,
                    conversation_id=meta.conversation_id,
                    batch_size=self._batch_size,
                )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=meta.operation_id,
                status="succeeded" if completed else "reconciling",
                progress={"phase": "succeeded" if completed else phase},
                result_summary={"conversation_id": str(meta.conversation_id)} if completed else {},
            )

        return JobHandlerOutcome(
            outcome="succeeded" if completed else "reconciling",
            metadata_patch=patch,
            result_summary={"phase": phase},
            retry_after_seconds=1,
            before_commit=commit_cleanup,
        )
