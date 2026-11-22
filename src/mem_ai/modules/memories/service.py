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
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    ConflictError,
    ForbiddenError,
    GatewayTimeoutError,
    NotFoundError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import repository
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
from mem_ai.modules.memories.schemas import (
    AcceptedMemoryOutcome,
    CreatedMemoryOutcome,
    MemoryCreateOutcome,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.schemas import OperationStatus
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories")

JOB_TYPE_MEMORY_EXPLICIT_CREATE = "memory_explicit_create"
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


def map_memory_provider_error(error_code: str | None, *, default_message: str) -> AppError:
    """Translate a stable memory provider error code into an HTTP-mapped `AppError`."""
    normalized = error_code or "memory_provider_error"
    if normalized in ("memory_provider_unavailable", "memory_provider_rate_limited"):
        return ServiceUnavailableError(
            code=normalized,
            message="memory provider is temporarily unavailable",
        )
    if normalized == "memory_provider_timeout":
        return GatewayTimeoutError(
            code="memory_provider_timeout",
            message="memory provider request timed out",
        )
    return BadGatewayError(
        code="memory_provider_error",
        message=default_message,
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

    query = MemoryListQuery(
        user_id=user_id,
        scope=effective_scope,
        conversation_id=conversation_id,
        limit=limit,
        cursor=cursor,
    )
    page = await memory_provider.list_memories(query)

    # Defense-in-depth owner isolation check in case an adapter returns foreign items.
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
    return MemoryListPage(
        items=owned_items,
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
        await users_service.ensure_user_active_in_session(session, user_id=user_id)
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
            content=content,
            result=result,
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
    content: str,
    result: MemoryWriteResult,
) -> MemoryCreateOutcome:
    """Re-verify user status and scope barrier/epoch in Tx 2 before returning 201."""
    assert result.memory_id is not None
    now = datetime.now(UTC)
    ext_ref = result.external_ref or write_id
    user_inactive_error: ForbiddenError | None = None

    async with session_factory.begin() as session:
        try:
            await users_service.ensure_user_active_in_session(session, user_id=user_id)
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
                metadata_patch={"memory_id": result.memory_id},
                now=now,
            )
            user_inactive_error = exc
        else:
            scope_row = await repository.get_or_create_scope_for_update(
                session,
                user_id=user_id,
                provider_id=provider_id,
            )
            if scope_row.barrier_state != "idle" or scope_row.epoch != captured_epoch:
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
                await jobs_service.complete_job_in_session(
                    session,
                    job_id=job_id,
                    lease_token=lease_token,
                    status="reconciling",
                    external_ref=ext_ref,
                    error_code="memory_scope_clearing",
                    error_summary="clearing barrier started mid-flight; pending reconciliation",
                    metadata_patch={
                        "memory_id": result.memory_id,
                        "operation_id": str(operation.id),
                    },
                    now=now,
                )
                return AcceptedMemoryOutcome(operation=operation)

            await jobs_service.complete_job_in_session(
                session,
                job_id=job_id,
                lease_token=lease_token,
                status="succeeded",
                external_ref=ext_ref,
                metadata_patch={"memory_id": result.memory_id},
                now=now,
            )

    if user_inactive_error is not None:
        raise user_inactive_error

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
        )
