"""Conversation-range memory barriers and bounded cleanup using durable single deletions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryListQuery, MemoryProvider
from mem_ai.core.exceptions import AppError
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobRecord
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    WRITE_JOB_TYPES,
    is_write_job_settled_for_clear,
    transition_tombstone_in_session,
)
from mem_ai.modules.memories.deletions import delete_single_memory
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import DeletionAuditArchiver


async def begin_conversation_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    conversation_id: UUID,
    operation_id: UUID,
    provider_id: str | None,
) -> list[str]:
    """Install retained range tombstones atomically with the conversation deleting transition."""
    providers = set(await repository.list_provider_ids(session, user_id=user_id))
    if provider_id is not None:
        providers.add(provider_id)
    existing = {
        row.provider_id
        for row in await repository.list_conversation_tombstones(
            session, user_id=user_id, conversation_id=conversation_id
        )
    }
    for target in sorted(providers - existing):
        await repository.create_tombstone(
            session,
            user_id=user_id,
            provider_id=target,
            target_kind="conversation",
            conversation_id=conversation_id,
            operation_id=operation_id,
        )
        await repository.delete_list_snapshots_for_scope(
            session, user_id=user_id, provider_id=target
        )
    return sorted(providers | existing)


def _can_cancel(job: JobRecord) -> bool:
    if is_write_job_settled_for_clear(job):
        return True
    state = job.metadata_json.get("dispatch_state", "pending")
    if state == "retry_write":
        return True
    return (
        state == "pending"
        and job.external_ref is None
        and (job.job_type == "memory_turn_write" or job.status == "pending")
    )


async def settle_conversation_writes_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    conversation_id: UUID,
    operation_id: UUID,
    batch_size: int,
    now: datetime,
) -> bool:
    """Cancel a bounded set of undispatched/settled jobs; wait for dispatched writes to verify.

    The caller holds the conversation lock, so dispatch cannot pass its active-state recheck
    while cancellation is evaluated. User-scoped memories already written remain intact.
    """
    matching = {"conversation_id": str(conversation_id)}
    cancelled = await jobs_service.cancel_matching_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        allowed_statuses=("pending", "retry_wait", "running", "reconciling"),
        predicate=_can_cancel,
        error_code="conversation_deleting",
        error_summary="source conversation is deleting",
        metadata_patch={"superseded_by_conversation_operation_id": str(operation_id)},
        metadata_match=matching,
        limit=batch_size,
        skip_locked=True,
        now=now,
    )
    for job in cancelled:
        raw_op = job.metadata_json.get("operation_id")
        if raw_op is not None:
            await operations_service.transition_operation_in_session(
                session,
                operation_id=UUID(str(raw_op)),
                status="failed",
                error_code="conversation_deleting",
                error_message="source conversation is deleting",
                progress={"phase": "superseded_by_conversation_delete"},
                now=now,
            )
    remaining = await jobs_service.list_user_jobs_in_session(
        session,
        user_id=user_id,
        job_types=WRITE_JOB_TYPES,
        statuses=("pending", "retry_wait", "running", "reconciling"),
        metadata_match=matching,
        limit=1,
    )
    return not remaining


@dataclass(frozen=True, slots=True)
class ConversationMemoryBatch:
    """A bounded remote batch's persisted continuation and verification state."""

    provider_complete: bool = False
    pending_memory_ids: tuple[str, ...] = ()
    phase: str = "remote_memories"


async def clean_conversation_memory_batch(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    ctx: JobExecutionContext,
    memory_provider: MemoryProvider | None,
    user_id: UUID,
    conversation_id: UUID,
    provider_id: str,
    pending_memory_ids: tuple[str, ...],
    batch_size: int,
    audit_archiver: DeletionAuditArchiver | None = None,
) -> ConversationMemoryBatch:
    """Delete one owner-validated range page, retaining IDs until child deletions are verified.

    Always re-list the first page after a batch settles: offset-backed upstream cursors may
    shift after deletion. Enumeration intent is durably saved *before* any delete is dispatched.
    """
    if memory_provider is None or memory_provider.provider_id != provider_id:
        return ConversationMemoryBatch(
            pending_memory_ids=pending_memory_ids, phase="memory_provider_unavailable"
        )
    targets = pending_memory_ids
    if not targets:
        try:
            page = await memory_provider.list_memories(
                MemoryListQuery(
                    user_id=user_id,
                    scope="conversation",
                    conversation_id=conversation_id,
                    limit=batch_size,
                )
            )
        except Exception:
            return ConversationMemoryBatch(phase="memory_list_unverified")
        if not page.items:
            return ConversationMemoryBatch(
                provider_complete=not page.has_more, phase="remote_memories_verified"
            )
        if (
            any(
                item.user_id != user_id
                or item.provider_id != provider_id
                or item.scope != "conversation"
                or item.conversation_id != conversation_id
                for item in page.items
            )
            or len(page.items) > batch_size
        ):
            return ConversationMemoryBatch(phase="memory_range_unverified")
        targets = tuple(item.memory_id for item in page.items)
        async with session_factory.begin() as session:
            await jobs_service.mark_job_dispatched_in_session(
                session,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                external_ref=f"conversation:{conversation_id}",
                metadata_patch={"pending_memory_ids": list(targets)},
            )
    try:
        for memory_id in targets:
            await delete_single_memory(
                session_factory,
                memory_provider=memory_provider,
                user_id=user_id,
                memory_id=memory_id,
                audit_archiver=audit_archiver,
            )
    except AppError:
        return ConversationMemoryBatch(pending_memory_ids=targets, phase="memory_delete_unverified")
    async with session_factory() as session:
        for memory_id in targets:
            tombstone = await repository.get_memory_tombstone(
                session,
                user_id=user_id,
                provider_id=provider_id,
                memory_id=memory_id,
            )
            if tombstone is None or tombstone.status != "verified":
                return ConversationMemoryBatch(
                    pending_memory_ids=targets, phase="memory_delete_unverified"
                )
    # A separate pass proves the range empty, including any delayed pre-barrier writes.
    return ConversationMemoryBatch()


async def finish_conversation_cleanup_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    conversation_id: UUID,
    now: datetime,
) -> None:
    """Retain verified range markers after remote confirmation and local graph removal."""
    for row in await repository.list_conversation_tombstones(
        session, user_id=user_id, conversation_id=conversation_id
    ):
        await transition_tombstone_in_session(
            session, tombstone_id=row.id, status="verified", now=now
        )
