"""Public memory policy, scope validation, listing, and explicit creation use cases."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryListPage,
    MemoryListQuery,
    MemoryProvider,
    MemoryRecord,
    MemoryScope,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.contracts.persona_provider import MemoryPolicy
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    ConflictError,
    ForbiddenError,
    GatewayTimeoutError,
    NotFoundError,
    UnprocessableEntityError,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobRecord
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    ALLOWED_BARRIER_TRANSITIONS as ALLOWED_BARRIER_TRANSITIONS,
)
from mem_ai.modules.memories.barrier import (
    ALLOWED_TOMBSTONE_TRANSITIONS as ALLOWED_TOMBSTONE_TRANSITIONS,
)
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_CLEAR as JOB_TYPE_MEMORY_CLEAR,
)
from mem_ai.modules.memories.barrier import (
    JOB_TYPE_MEMORY_DELETE as JOB_TYPE_MEMORY_DELETE,
)
from mem_ai.modules.memories.barrier import (
    OPERATION_TYPE_MEMORY_CLEAR as OPERATION_TYPE_MEMORY_CLEAR,
)
from mem_ai.modules.memories.barrier import (
    OPERATION_TYPE_MEMORY_DELETE as OPERATION_TYPE_MEMORY_DELETE,
)
from mem_ai.modules.memories.barrier import (
    MemoryBarrierObservabilitySnapshot as MemoryBarrierObservabilitySnapshot,
)
from mem_ai.modules.memories.barrier import (
    NoopRecalledMemoryReferenceMarker as NoopRecalledMemoryReferenceMarker,
)
from mem_ai.modules.memories.barrier import (
    RecalledMemoryReferenceMarker as RecalledMemoryReferenceMarker,
)
from mem_ai.modules.memories.barrier import (
    get_memory_barrier_observability as get_memory_barrier_observability,
)
from mem_ai.modules.memories.barrier import (
    is_valid_barrier_transition as is_valid_barrier_transition,
)
from mem_ai.modules.memories.barrier import (
    is_valid_tombstone_transition as is_valid_tombstone_transition,
)
from mem_ai.modules.memories.barrier import (
    map_memory_provider_error as map_memory_provider_error,
)
from mem_ai.modules.memories.clearing import clear_user_memories as clear_user_memories
from mem_ai.modules.memories.conversation_cleanup import (
    begin_conversation_cleanup_in_session as begin_conversation_cleanup_in_session,
)
from mem_ai.modules.memories.conversation_cleanup import (
    clean_conversation_memory_batch as clean_conversation_memory_batch,
)
from mem_ai.modules.memories.conversation_cleanup import (
    finish_conversation_cleanup_in_session as finish_conversation_cleanup_in_session,
)
from mem_ai.modules.memories.conversation_cleanup import (
    settle_conversation_writes_in_session as settle_conversation_writes_in_session,
)
from mem_ai.modules.memories.deletions import delete_single_memory as delete_single_memory
from mem_ai.modules.memories.jobs import (
    JOB_TYPE_MEMORY_EXPLICIT_CREATE as JOB_TYPE_MEMORY_EXPLICIT_CREATE,
)
from mem_ai.modules.memories.jobs import (
    JOB_TYPE_MEMORY_TURN_WRITE as JOB_TYPE_MEMORY_TURN_WRITE,
)
from mem_ai.modules.memories.jobs import (
    TurnContentLoader as TurnContentLoader,
)
from mem_ai.modules.memories.jobs import (
    TurnContentRecord as TurnContentRecord,
)
from mem_ai.modules.memories.recall import (
    MemoryBarrierState as MemoryBarrierState,
)
from mem_ai.modules.memories.recall import (
    MemoryRecallGuard as MemoryRecallGuard,
)
from mem_ai.modules.memories.recall import (
    MemoryRecallOutcome as MemoryRecallOutcome,
)
from mem_ai.modules.memories.recall import (
    MemoryRecallPolicy as MemoryRecallPolicy,
)
from mem_ai.modules.memories.recall import (
    TombstoneLookup as TombstoneLookup,
)
from mem_ai.modules.memories.recall import (
    filter_injectable_memories_in_session as filter_injectable_memories_in_session,
)
from mem_ai.modules.memories.recall import (
    get_memory_epoch_in_session as get_memory_epoch_in_session,
)
from mem_ai.modules.memories.recall import (
    load_recall_guard_in_session as load_recall_guard_in_session,
)
from mem_ai.modules.memories.recall import (
    recall_relevant_memories as recall_relevant_memories,
)
from mem_ai.modules.memories.recall import (
    select_memories_for_prompt as select_memories_for_prompt,
)
from mem_ai.modules.memories.recovery import (
    replay_clear_scope_for_recovery as replay_clear_scope_for_recovery,
)
from mem_ai.modules.memories.recovery import (
    replay_single_memory_delete_for_recovery as replay_single_memory_delete_for_recovery,
)
from mem_ai.modules.memories.schemas import (
    AcceptedMemoryOutcome,
    CreatedMemoryOutcome,
    MemoryCreateOutcome,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.schemas import OperationStatus
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories")

OPERATION_TYPE_MEMORY_EXPLICIT_CREATE = "memory_explicit_create"


@runtime_checkable
class ConversationAccessChecker(Protocol):
    """Seam for validating conversation ownership before accessing conversation-scoped memory."""

    async def ensure_conversation_accessible(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> None:
        """Raise 404 if conversation does not exist/not owned, or 409 if deleting."""
        ...

    async def ensure_conversation_accessible_in_session(
        self, session: AsyncSession, *, owner_id: UUID, conversation_id: UUID
    ) -> None:
        """Validate ownership while holding a shared conversation lock."""
        ...


class DefaultConversationAccessChecker:
    """Default conversation ownership checker used before the `conversations` slice is wired."""

    async def ensure_conversation_accessible(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> None:
        _ = (owner_id, conversation_id)
        raise NotFoundError(
            code="conversation_not_found",
            message="conversation not found",
        )

    async def ensure_conversation_accessible_in_session(
        self, session: AsyncSession, *, owner_id: UUID, conversation_id: UUID
    ) -> None:
        await self.ensure_conversation_accessible(
            owner_id=owner_id, conversation_id=conversation_id
        )


async def resolve_and_validate_scope(
    *,
    user_id: UUID,
    scope: MemoryScope,
    conversation_id: UUID | None,
    conversation_access_checker: ConversationAccessChecker | None = None,
) -> tuple[MemoryScope, UUID | None]:
    """Validate `(scope, conversation_id)` pairing and conversation ownership."""
    if scope == "conversation":
        if conversation_id is None:
            raise UnprocessableEntityError(
                code="validation_error",
                message="conversationId is required when scope is 'conversation'",
            )
        checker = (
            conversation_access_checker
            if conversation_access_checker is not None
            else DefaultConversationAccessChecker()
        )
        await checker.ensure_conversation_accessible(
            owner_id=user_id,
            conversation_id=conversation_id,
        )
        return ("conversation", conversation_id)

    if conversation_id is not None:
        raise UnprocessableEntityError(
            code="validation_error",
            message="conversationId must not be set when scope is 'user'",
        )
    return ("user", None)


async def list_memories(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider,
    user_id: UUID,
    scope: MemoryScope | None = None,
    conversation_id: UUID | None = None,
    limit: int = 20,
    cursor: str | None = None,
    conversation_access_checker: ConversationAccessChecker | None = None,
) -> MemoryListPage:
    """List memories owned by `user_id` using stable cursor pagination."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=user_id)
        scope_state = await repository.get_scope(
            session,
            user_id=user_id,
            provider_id=memory_provider.provider_id,
        )
        tombstoned_ids, conversation_tombstoned = await repository.load_active_tombstones(
            session,
            user_id=user_id,
            provider_id=memory_provider.provider_id,
            conversation_id=conversation_id,
        )
        tombstoned_conversations = await repository.load_tombstoned_conversation_ids(
            session, user_id=user_id, provider_id=memory_provider.provider_id
        )

    if conversation_id is not None:
        if scope == "user":
            raise UnprocessableEntityError(
                code="validation_error",
                message="conversationId cannot be combined with scope='user'",
            )
        effective_scope: MemoryScope | None = "conversation"
        await resolve_and_validate_scope(
            user_id=user_id,
            scope="conversation",
            conversation_id=conversation_id,
            conversation_access_checker=conversation_access_checker,
        )
    else:
        effective_scope = scope

    if scope_state is not None and scope_state.barrier_state != "idle":
        return MemoryListPage(items=[], next_cursor=None, has_more=False)
    if conversation_id is not None and conversation_tombstoned:
        return MemoryListPage(items=[], next_cursor=None, has_more=False)

    current_epoch = scope_state.epoch if scope_state is not None else 0
    query = MemoryListQuery(
        user_id=user_id,
        scope=effective_scope,
        conversation_id=conversation_id,
        limit=limit,
        cursor=cursor,
    )
    page = await memory_provider.list_memories(query)

    # Defense-in-depth owner, epoch, and tombstone isolation filter.
    owned_items = [item for item in page.items if item.user_id == user_id]
    if len(owned_items) != len(page.items):
        logger.warning(
            "Filtered foreign user memories from provider list response",
            extra={
                "extra_fields": {
                    "provider": memory_provider.provider_id,
                    "filtered_count": len(page.items) - len(owned_items),
                }
            },
        )
    visible_items = [
        item
        for item in owned_items
        if item.memory_id not in tombstoned_ids
        and item.memory_epoch >= current_epoch
        and item.conversation_id not in tombstoned_conversations
    ]
    return MemoryListPage(
        items=visible_items,
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


async def create_explicit_memory(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider,
    user_id: UUID,
    content: str,
    scope: MemoryScope = "user",
    conversation_id: UUID | None = None,
    conversation_access_checker: ConversationAccessChecker | None = None,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
) -> MemoryCreateOutcome:
    """Persist an explicit memory write via unified jobs and MemoryProvider outside DB tx."""
    await users_service.get_active_user(session_factory, user_id=user_id)
    resolved_scope, resolved_conversation_id = await resolve_and_validate_scope(
        user_id=user_id,
        scope=scope,
        conversation_id=conversation_id,
        conversation_access_checker=conversation_access_checker,
    )

    write_id = f"mw_{uuid4().hex}"
    provider_id = memory_provider.provider_id

    # Short Tx 1: serialize with clearing barrier on `memory_scopes` row lock and claim sync job.
    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=user_id, for_share=True)
        if resolved_conversation_id is not None:
            checker = conversation_access_checker or DefaultConversationAccessChecker()
            await checker.ensure_conversation_accessible_in_session(
                session, owner_id=user_id, conversation_id=resolved_conversation_id
            )
        scope_row = await repository.get_or_create_scope_for_update(
            session,
            user_id=user_id,
            provider_id=provider_id,
        )
        if scope_row.barrier_state != "idle":
            raise ConflictError(
                code="memory_scope_clearing",
                message="memory scope is currently clearing",
            )
        captured_epoch = scope_row.epoch
        job = await jobs_service.register_and_claim_sync_job_in_session(
            session,
            job_type=JOB_TYPE_MEMORY_EXPLICIT_CREATE,
            business_id=write_id,
            user_id=user_id,
            dedupe_key=f"{JOB_TYPE_MEMORY_EXPLICIT_CREATE}:{write_id}",
            attempt_retention_days=attempt_retention_days,
            metadata_json={
                "write_id": write_id,
                "provider_id": provider_id,
                "scope": resolved_scope,
                "conversation_id": (
                    str(resolved_conversation_id) if resolved_conversation_id is not None else None
                ),
                "memory_epoch": captured_epoch,
            },
        )

    assert job.lease_token is not None

    # External provider call strictly outside any database transaction.
    write_input = MemoryWriteInput(
        write_id=write_id,
        user_id=user_id,
        scope=resolved_scope,
        content=content,
        conversation_id=resolved_conversation_id,
        memory_epoch=captured_epoch,
    )
    try:
        result = await memory_provider.create_memory(write_input)
    except AppError as exc:
        if isinstance(exc, GatewayTimeoutError):
            result = MemoryWriteResult(
                status="reconciling",
                provider_id=provider_id,
                external_ref=write_id,
                error_code=exc.code,
            )
        else:
            await _record_failed_write(
                session_factory,
                job_id=job.id,
                lease_token=job.lease_token,
                external_ref=write_id,
                error_code=exc.code,
                error_summary="memory provider raised application error",
            )
            raise
    except Exception:
        logger.exception(
            "Unexpected exception while executing explicit memory write",
            extra={
                "extra_fields": {
                    "provider": provider_id,
                    "job_id": str(job.id),
                    "scope": resolved_scope,
                }
            },
        )
        await _record_failed_write(
            session_factory,
            job_id=job.id,
            lease_token=job.lease_token,
            external_ref=write_id,
            error_code="memory_provider_error",
            error_summary="unexpected provider exception",
        )
        raise BadGatewayError(
            code="memory_provider_error",
            message="memory provider failed to persist memory",
        ) from None

    # Short Tx 2: re-verify user & memory_scopes barrier/epoch before committing outcome.
    if result.status == "succeeded" and result.memory_id is not None:
        return await _commit_confirmed_write(
            session_factory,
            job_id=job.id,
            lease_token=job.lease_token,
            user_id=user_id,
            provider_id=provider_id,
            captured_epoch=captured_epoch,
            write_id=write_id,
            resolved_scope=resolved_scope,
            resolved_conversation_id=resolved_conversation_id,
            conversation_access_checker=conversation_access_checker,
            content=content,
            result=result,
            attempt_retention_days=attempt_retention_days,
        )

    if result.status in ("accepted", "reconciling") or (
        result.status == "succeeded" and result.memory_id is None
    ):
        is_reconciling = result.status == "reconciling"
        op_status: OperationStatus = "reconciling" if is_reconciling else "running"
        ext_ref = result.external_ref or write_id
        err_code = result.error_code if is_reconciling else None
        err_msg = (
            "memory provider response timed out; verification pending" if is_reconciling else None
        )
        async with session_factory.begin() as session:
            operation = await operations_service.create_operation_in_session(
                session,
                owner_id=user_id,
                operation_type=OPERATION_TYPE_MEMORY_EXPLICIT_CREATE,
                status=op_status,
                target_type="memory",
                target_id=ext_ref,
                job_id=job.id,
                progress={
                    "phase": "reconciling" if is_reconciling else "accepted",
                    "provider_id": provider_id,
                    "scope": resolved_scope,
                    "memory_epoch": captured_epoch,
                    "external_ref": ext_ref,
                },
                error_code=err_code,
                error_message=err_msg,
            )
            await jobs_service.complete_job_in_session(
                session,
                job_id=job.id,
                lease_token=job.lease_token,
                status="reconciling",
                external_ref=ext_ref,
                error_code=err_code,
                error_summary=(
                    "upstream outcome unknown; pending verification"
                    if is_reconciling
                    else "accepted for async extraction; pending verification"
                ),
                metadata_patch={"operation_id": str(operation.id)},
                attempt_retention_days=attempt_retention_days,
            )
        return AcceptedMemoryOutcome(operation=operation)

    # Deterministic failure (`result.status == "failed"`).
    await _record_failed_write(
        session_factory,
        job_id=job.id,
        lease_token=job.lease_token,
        external_ref=result.external_ref or write_id,
        error_code=result.error_code or "memory_provider_error",
        error_summary="memory provider rejected explicit write",
        attempt_retention_days=attempt_retention_days,
    )
    raise map_memory_provider_error(
        result.error_code,
        default_message="memory provider failed to persist memory",
    )


async def _commit_confirmed_write(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    lease_token: str,
    user_id: UUID,
    provider_id: str,
    captured_epoch: int,
    write_id: str,
    resolved_scope: MemoryScope,
    resolved_conversation_id: UUID | None,
    conversation_access_checker: ConversationAccessChecker | None,
    content: str,
    result: MemoryWriteResult,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
) -> MemoryCreateOutcome:
    """Re-verify user status and scope barrier/epoch in Tx 2 before returning 201."""
    assert result.memory_id is not None
    now = datetime.now(UTC)
    ext_ref = result.external_ref or write_id
    user_inactive_error: ForbiddenError | None = None
    conversation_error: AppError | None = None
    scope_error: ConflictError | None = None

    async with session_factory.begin() as session:
        try:
            await users_service.ensure_user_active_in_session(
                session, user_id=user_id, for_share=True
            )
        except ForbiddenError as exc:
            # User became disabled/deleting mid-flight: preserve reconciling record for cleanup.
            await jobs_service.complete_job_in_session(
                session,
                job_id=job_id,
                lease_token=lease_token,
                status="reconciling",
                external_ref=ext_ref,
                error_code=exc.code,
                error_summary="user became inactive mid-flight; pending reconciliation",
                metadata_patch={
                    "memory_id": result.memory_id,
                    "dispatch_state": "confirmed_during_clear",
                },
                now=now,
            )
            user_inactive_error = exc
        else:
            if resolved_conversation_id is not None:
                checker = conversation_access_checker or DefaultConversationAccessChecker()
                try:
                    await checker.ensure_conversation_accessible_in_session(
                        session, owner_id=user_id, conversation_id=resolved_conversation_id
                    )
                except (ConflictError, NotFoundError) as exc:
                    conversation_error = exc
            if conversation_error is not None:
                await jobs_service.complete_job_in_session(
                    session,
                    job_id=job_id,
                    lease_token=lease_token,
                    status="reconciling",
                    external_ref=ext_ref,
                    error_code=conversation_error.code,
                    error_summary="conversation became inaccessible while write was in flight",
                    metadata_patch={
                        "memory_id": result.memory_id,
                        "dispatch_state": "confirmed_during_clear",
                    },
                    now=now,
                )
            else:
                scope_row = await repository.get_or_create_scope_for_update(
                    session,
                    user_id=user_id,
                    provider_id=provider_id,
                )
                if scope_row.barrier_state != "idle" or scope_row.epoch != captured_epoch:
                    if scope_row.barrier_state == "idle":
                        await jobs_service.complete_job_in_session(
                            session,
                            job_id=job_id,
                            lease_token=lease_token,
                            status="reconciling",
                            external_ref=ext_ref,
                            error_code="memory_scope_cleared",
                            error_summary="scope clear finished while explicit write was in flight",
                            metadata_patch={
                                "memory_id": result.memory_id,
                                "dispatch_state": "confirmed_during_clear",
                            },
                            now=now,
                        )
                        scope_error = ConflictError(
                            code="memory_scope_clearing",
                            message="memory scope was cleared while write was in flight",
                        )

                    else:
                        operation = await operations_service.create_operation_in_session(
                            session,
                            owner_id=user_id,
                            operation_type=OPERATION_TYPE_MEMORY_EXPLICIT_CREATE,
                            status="reconciling",
                            target_type="memory",
                            target_id=result.memory_id,
                            job_id=job_id,
                            progress={
                                "phase": "reconciling",
                                "provider_id": provider_id,
                                "scope": resolved_scope,
                                "memory_epoch": captured_epoch,
                                "external_ref": ext_ref,
                            },
                            result_summary={"memory_id": result.memory_id},
                            error_code="memory_scope_clearing",
                            error_message="memory scope clearing started while write was in flight",
                        )
                        updated_job = await jobs_service.complete_job_in_session(
                            session,
                            job_id=job_id,
                            lease_token=lease_token,
                            status="reconciling",
                            external_ref=ext_ref,
                            error_code="memory_scope_clearing",
                            error_summary=(
                                "clearing barrier started mid-flight; pending reconciliation"
                            ),
                            metadata_patch={
                                "memory_id": result.memory_id,
                                "operation_id": str(operation.id),
                                "dispatch_state": "confirmed_during_clear",
                            },
                            now=now,
                        )
                        if updated_job is None:
                            failed_op = await operations_service.transition_operation_in_session(
                                session,
                                operation_id=operation.id,
                                status="failed",
                                progress={
                                    "phase": "superseded_by_clear",
                                    "provider_id": provider_id,
                                },
                                error_code="memory_scope_cleared",
                                error_message="memory scope was cleared while write was in flight",
                                now=now,
                            )
                            return AcceptedMemoryOutcome(operation=failed_op or operation)
                        return AcceptedMemoryOutcome(operation=operation)
                if scope_error is None:
                    await jobs_service.complete_job_in_session(
                        session,
                        job_id=job_id,
                        lease_token=lease_token,
                        status="succeeded",
                        external_ref=ext_ref,
                        metadata_patch={"memory_id": result.memory_id},
                        attempt_retention_days=attempt_retention_days,
                        now=now,
                    )
    if user_inactive_error is not None:
        raise user_inactive_error
    if conversation_error is not None:
        raise conversation_error
    if scope_error is not None:
        raise scope_error

    record = MemoryRecord(
        memory_id=result.memory_id,
        provider_id=result.provider_id,
        user_id=user_id,
        scope=resolved_scope,
        content=content,
        conversation_id=resolved_conversation_id,
        memory_epoch=captured_epoch,
        created_at=now,
    )
    return CreatedMemoryOutcome(record=record)


async def _record_failed_write(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    lease_token: str,
    external_ref: str | None,
    error_code: str,
    error_summary: str,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
) -> None:
    async with session_factory.begin() as session:
        await jobs_service.complete_job_in_session(
            session,
            job_id=job_id,
            lease_token=lease_token,
            status="failed",
            external_ref=external_ref,
            error_code=error_code,
            error_summary=error_summary,
            attempt_retention_days=attempt_retention_days,
        )


async def enqueue_turn_memory_write_in_session(
    session: AsyncSession,
    *,
    provider_id: str,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
    user_message_id: UUID,
    assistant_message_id: UUID,
    memory_epoch: int,
    memory_policy: MemoryPolicy,
    max_attempts: int = 5,
    now: datetime | None = None,
) -> JobRecord | None:
    """Enqueue a `memory_turn_write` job inside the generation success commit transaction.

    Honors pinned `memory_policy.enabled`, `memory_policy.write_enabled`, and
    `memory_policy.write_scope` (`user` default or `conversation`), suppresses stale-epoch or
    barrier-blocked writes without failing the committed reply, and never persists raw message
    content in `jobs.metadata_json`.
    """
    if not memory_policy.enabled or not memory_policy.write_enabled:
        return None

    scope_row = await repository.get_scope(
        session,
        user_id=owner_id,
        provider_id=provider_id,
        for_share=True,
    )
    if scope_row is not None and (
        scope_row.barrier_state != "idle" or scope_row.epoch != memory_epoch
    ):
        logger.info(
            "Suppressed turn memory write job due to active clearing barrier or epoch change",
            extra={
                "extra_fields": {
                    "provider": provider_id,
                    "captured_epoch": memory_epoch,
                    "current_epoch": scope_row.epoch,
                    "barrier_state": scope_row.barrier_state,
                }
            },
        )
        return None

    write_scope: MemoryScope = memory_policy.write_scope
    write_id = f"mtw_{generation_id.hex}"
    dedupe_key = f"{JOB_TYPE_MEMORY_TURN_WRITE}:{generation_id}"
    job, _ = await jobs_service.enqueue_job_in_session(
        session,
        job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        business_id=str(generation_id),
        user_id=owner_id,
        dedupe_key=dedupe_key,
        max_attempts=max_attempts,
        now=now,
        metadata_json={
            "write_id": write_id,
            "provider_id": provider_id,
            "scope": write_scope,
            "conversation_id": str(conversation_id),
            "generation_id": str(generation_id),
            "user_message_id": str(user_message_id),
            "assistant_message_id": str(assistant_message_id),
            "memory_epoch": memory_epoch,
            "dispatch_state": "pending",
        },
    )
    return job
