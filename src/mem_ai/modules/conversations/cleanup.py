"""Owned conversation deletion, generation settlement, and resumable bounded cleanup."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.exceptions import ForbiddenError, NotFoundError
from mem_ai.db.base import get_database_now
from mem_ai.modules.conversations import cleanup_repository, generation, repository
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome, JobRecord
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    DeletionAuditEvent,
    RecoveryReplayStatus,
    export_deletion_audit_events_safely,
)
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


async def _accept_conversation_deletion_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    provider_id: str | None,
    allow_disabled_user: bool = False,
) -> tuple[OperationRecord, bool, datetime | None]:
    if allow_disabled_user:
        await users_service.ensure_user_not_deleting_in_session(
            session, user_id=owner_id, for_share=True
        )
    else:
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
    if existing is not None and (not allow_disabled_user or existing.status != "failed"):
        return existing, False, None
    if conversation is None:
        if existing is not None:
            return existing, False, None
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
        provider_id=provider_id,
    )
    dedupe_key = (
        f"{JOB_TYPE_CONVERSATION_DELETE}:{conversation_id}:{operation.id}"
        if existing is not None
        else f"{JOB_TYPE_CONVERSATION_DELETE}:{conversation_id}"
    )
    job, _ = await jobs_service.enqueue_job_in_session(
        session,
        job_type=JOB_TYPE_CONVERSATION_DELETE,
        business_id=str(conversation_id),
        user_id=owner_id,
        dedupe_key=dedupe_key,
        metadata_json={
            "conversation_id": str(conversation_id),
            "operation_id": str(operation.id),
            "provider_ids": providers,
        },
        now=now,
    )
    result = (
        await operations_service.attach_job_in_session(
            session,
            operation_id=operation.id,
            job_id=job.id,
            now=now,
        )
        or operation
    )
    return result, True, now


async def delete_conversation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    memory_provider: MemoryProvider | None,
    audit_archiver: DeletionAuditArchiver | None = None,
) -> OperationRecord:
    """Atomically hide an owned conversation and enqueue exactly one effective operation."""
    provider_id = memory_provider.provider_id if memory_provider is not None else None
    async with session_factory.begin() as session:
        result, created_new, accepted_at = await _accept_conversation_deletion_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
            provider_id=provider_id,
            allow_disabled_user=False,
        )
    if created_new and accepted_at is not None:
        await export_deletion_audit_events_safely(
            audit_archiver,
            [
                DeletionAuditEvent(
                    event_type="operation_accepted",
                    occurred_at=accepted_at,
                    user_id=owner_id,
                    operation_id=result.id,
                    operation_type=JOB_TYPE_CONVERSATION_DELETE,
                    target_type="conversation",
                    target_id=str(conversation_id),
                    status=result.status,
                )
            ],
        )
    return result


async def replay_conversation_deletion_for_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    memory_provider: MemoryProvider | None = None,
    provider_id: str | None = None,
) -> RecoveryReplayStatus:
    """Idempotently replay a post-backup conversation deletion without re-emitting audit events."""
    resolved_provider_id = (
        memory_provider.provider_id if memory_provider is not None else provider_id
    )
    try:
        async with session_factory.begin() as session:
            _, created_new, _ = await _accept_conversation_deletion_in_session(
                session,
                owner_id=owner_id,
                conversation_id=conversation_id,
                provider_id=resolved_provider_id,
                allow_disabled_user=True,
            )
    except (NotFoundError, ForbiddenError):
        return "skipped_missing"
    return "enqueued" if created_new else "already_applied"


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
        user_deleting = False
        async with self._session_factory.begin() as session:
            # Match the job outcome hook's lock order, including after lease reclamation.
            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=f"conversation:{meta.conversation_id}",
            )
            try:
                await users_service.ensure_user_active_in_session(session, user_id=owner)
            except ForbiddenError as exc:
                if exc.code in ("user_deleting", "user_not_active"):
                    user_deleting = True
            if not user_deleting:
                conversation = await repository.get_conversation_for_update(
                    session,
                    owner_id=owner,
                    conversation_id=meta.conversation_id,
                )
                now = await get_database_now(session)
                if conversation is None or conversation.status != "deleting":
                    return JobHandlerOutcome(
                        outcome="failed", error_code="conversation_not_deleting"
                    )
                if await _settle_running_generation_in_session(
                    session,
                    owner_id=owner,
                    conversation_id=meta.conversation_id,
                    now=now,
                    expired_message="generation lease expired",
                ):
                    phase = "settling_memory_writes"
                    ready = await memories_service.settle_conversation_writes_in_session(
                        session,
                        user_id=owner,
                        conversation_id=meta.conversation_id,
                        operation_id=meta.operation_id,
                        batch_size=self._batch_size,
                        now=now,
                    )
        if user_deleting:

            async def mark_superseded(session: AsyncSession, _job: JobRecord) -> None:
                op = await operations_service.get_operation_in_session(
                    session, operation_id=meta.operation_id, for_update=True
                )
                if op is None or op.status in ("succeeded", "failed"):
                    return
                now = await get_database_now(session)
                if op.status == "pending":
                    await operations_service.transition_operation_in_session(
                        session, operation_id=op.id, status="running", now=now
                    )
                if not op.progress.get("local_complete"):
                    await operations_service.transition_operation_in_session(
                        session,
                        operation_id=op.id,
                        status="reconciling",
                        progress={
                            "phase": "awaiting_user_remote_memories",
                            "local_complete": False,
                            "remote_complete": False,
                        },
                        now=now,
                    )

            return JobHandlerOutcome(
                outcome="cancelled",
                error_code="user_deleting",
                error_summary="conversation cleanup joined user deletion",
                metadata_patch={"superseded_by_user_delete": True},
                before_commit=mark_superseded,
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

        async def after_commit(_job: JobRecord) -> None:
            if not completed:
                return
            await export_deletion_audit_events_safely(
                self._audit_archiver,
                [
                    DeletionAuditEvent(
                        event_type="operation_transitioned",
                        occurred_at=datetime.now(UTC),
                        user_id=owner,
                        operation_id=meta.operation_id,
                        operation_type=JOB_TYPE_CONVERSATION_DELETE,
                        target_type="conversation",
                        target_id=str(meta.conversation_id),
                        status="succeeded",
                    )
                ],
            )

        return JobHandlerOutcome(
            outcome="succeeded" if completed else "reconciling",
            metadata_patch=patch,
            result_summary={"phase": phase},
            retry_after_seconds=1,
            before_commit=commit_cleanup,
            after_commit=after_commit,
        )


async def _settle_running_generation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    now: datetime,
    expired_message: str,
) -> bool:
    """Converge an expired running generation in place; return False if a live lease remains."""
    running = await repository.get_running_generation_for_conversation(
        session, conversation_id=conversation_id
    )
    if running is None:
        return True
    if running.lease_expires_at is None or running.lease_expires_at > now:
        return False
    settled = await generation.converge_generation_failure_in_session(
        session,
        owner_id=owner_id,
        conversation_id=conversation_id,
        generation_id=running.id,
        lease_token=None,
        error_code="generation_lease_lost",
        error_message=expired_message,
        current_time=now,
        expired_only=True,
    )
    return settled is not None


async def settle_user_generations_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    batch_size: int = 50,
    now: datetime,
) -> bool:
    """Await active leases and converge expired generations before the final memory clear.

    The owning user is irreversibly deleting, so no new generation can start or renew.
    Each expired batch uses the shared lifecycle to release leases and settle idempotency.
    """
    conversation_ids = await cleanup_repository.list_user_running_generation_conversations(
        session, owner_id=owner_id, batch_size=batch_size
    )
    for conversation_id in conversation_ids:
        await repository.get_conversation_for_update(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        if not await _settle_running_generation_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
            now=now,
            expired_message="generation lease expired during user deletion",
        ):
            return False
    return not await cleanup_repository.has_user_running_generations(session, owner_id=owner_id)


async def cleanup_user_conversations_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    batch_size: int = 50,
    now: datetime,
) -> bool:
    """Remove one bounded local batch after generation leases settled and barriers installed.

    The caller has persisted the irreversible deleting state. Conversation jobs are superseded only
    after their valid leases expire; superseded operations stay reconciling until remote user
    memory deletion is confirmed.
    """
    conversation_id = await cleanup_repository.first_user_conversation_id(
        session, owner_id=owner_id
    )
    if conversation_id is None:
        return True
    matching = {"conversation_id": str(conversation_id)}

    # Preserve the shared handler's job -> conversation lock order. A live job must finish
    # its attempt before this coordinator owns the graph; SKIP LOCKED also avoids lease races.
    await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=owner_id,
        job_types=(JOB_TYPE_CONVERSATION_DELETE,),
        allowed_statuses=("pending", "retry_wait", "running", "reconciling"),
        predicate=lambda job: not job.has_active_lease(now),
        error_code="user_deleting",
        error_summary="conversation cleanup joined user deletion",
        metadata_patch={"superseded_by_user_delete": True},
        metadata_match=matching,
        limit=1,
        skip_locked=True,
        now=now,
    )
    live_jobs = await jobs_service.list_user_jobs_in_session(
        session,
        user_id=owner_id,
        job_types=(JOB_TYPE_CONVERSATION_DELETE,),
        statuses=("pending", "retry_wait", "running", "reconciling"),
        metadata_match=matching,
        limit=1,
    )
    if live_jobs:
        return False
    conversation = await repository.get_conversation_for_update(
        session, owner_id=owner_id, conversation_id=conversation_id
    )
    if conversation is None:
        return not await cleanup_repository.has_user_conversations(session, owner_id=owner_id)
    conversation.status = "deleting"
    conversation.deleted_at = conversation.deleted_at or now
    conversation.updated_at = now
    if not await _settle_running_generation_in_session(
        session,
        owner_id=owner_id,
        conversation_id=conversation_id,
        now=now,
        expired_message="generation lease expired during user deletion",
    ):
        return False
    if await cleanup_repository.has_children(session, conversation_id=conversation_id):
        await cleanup_repository.delete_batch(
            session, conversation_id=conversation_id, batch_size=max(1, min(100, batch_size))
        )
        return False
    await cleanup_repository.delete_empty_conversation(session, conversation_id=conversation_id)
    operation = await operations_service.find_operation_for_target_in_session(
        session,
        owner_id=owner_id,
        operation_type=JOB_TYPE_CONVERSATION_DELETE,
        target_id=str(conversation_id),
    )
    if operation is not None and operation.status not in ("succeeded", "failed"):
        if operation.status == "pending":
            await operations_service.transition_operation_in_session(
                session,
                operation_id=operation.id,
                status="running",
                now=now,
            )
        await operations_service.transition_operation_in_session(
            session,
            operation_id=operation.id,
            status="reconciling",
            progress={
                "phase": "awaiting_user_remote_memories",
                "local_complete": True,
                "remote_complete": False,
            },
            now=now,
        )
    return not await cleanup_repository.has_user_conversations(session, owner_id=owner_id)


async def settle_user_conversation_operations_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    batch_size: int = 50,
    now: datetime,
) -> bool:
    """Confirm superseded conversation deletions only after whole-user remote cleanup succeeds."""
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=owner_id,
        job_types=(JOB_TYPE_CONVERSATION_DELETE,),
        statuses=("running",),
        active_lease_at=now,
    ):
        return False
    bounded = max(1, min(100, batch_size))
    await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=owner_id,
        job_types=(JOB_TYPE_CONVERSATION_DELETE,),
        allowed_statuses=("pending", "retry_wait", "running", "reconciling"),
        predicate=lambda job: not job.has_active_lease(now),
        error_code="user_deleting",
        error_summary="conversation cleanup confirmed by verified user deletion",
        metadata_patch={"superseded_by_user_delete": True},
        limit=bounded,
        skip_locked=True,
        now=now,
    )
    if await jobs_service.has_user_jobs_in_session(
        session,
        user_id=owner_id,
        job_types=(JOB_TYPE_CONVERSATION_DELETE,),
        statuses=("pending", "retry_wait", "running", "reconciling"),
    ):
        return False
    operations = await operations_service.list_non_terminal_operations_in_session(
        session,
        owner_id=owner_id,
        operation_type=JOB_TYPE_CONVERSATION_DELETE,
        batch_size=bounded,
    )
    for operation in operations:
        if operation.status == "pending":
            await operations_service.transition_operation_in_session(
                session,
                operation_id=operation.id,
                status="running",
                now=now,
            )
        summary: dict[str, object] = {"deleted_with_user": True}
        if operation.target_id is not None:
            summary["conversation_id"] = operation.target_id
        await operations_service.transition_operation_in_session(
            session,
            operation_id=operation.id,
            status="succeeded",
            progress={
                "phase": "succeeded",
                "local_complete": True,
                "remote_complete": True,
            },
            result_summary=summary,
            now=now,
        )
    remaining = await operations_service.list_non_terminal_operations_in_session(
        session,
        owner_id=owner_id,
        operation_type=JOB_TYPE_CONVERSATION_DELETE,
        batch_size=1,
    )
    return not remaining
