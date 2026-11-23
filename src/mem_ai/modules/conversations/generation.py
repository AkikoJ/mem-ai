"""Shared generation persistence phases, ownership checks, and terminal state transitions.

Implements the persistence half of Engineering Design §5.4 (Shared Generation Lifecycle):
- Ticket 07 extends `prepare_generation_phase`, lease renewal, and expiry takeover here.
- Ticket 08 adds recalled memory reference persistence after context preparation.
- Ticket 09 participates in `commit_generation_success_phase` (`before_commit_hook`) to enqueue
  ordinary turn memory jobs atomically in the same short transaction.
- Ticket 10 participates in `prepare_generation_phase` and `commit_generation_success_phase` /
  `converge_generation_failure_phase` for idempotency record creation and finalization.
- Ticket 12 reuses these exact preparation, commit, and cancellation convergence phases for SSE.
- Ticket 13 extends `prepare_generation_phase` / `commit_generation_success_phase` with memory
  epoch capture and clearing-barrier checks.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from types import TracebackType
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelUsage
from mem_ai.contracts.persona_provider import PersonaSnapshot
from mem_ai.core.exceptions import AppError, ConflictError, ForbiddenError, NotFoundError
from mem_ai.db.base import is_unique_violation
from mem_ai.modules.conversations import idempotency, queries, repository
from mem_ai.modules.conversations.models import (
    GENERATION_ACTIVE_CONVERSATION_UNIQUE_INDEX,
    IDEMPOTENCY_SCOPE_UNIQUE_CONSTRAINT,
    MESSAGE_GENERATION_ROLE_UNIQUE_CONSTRAINT,
    MESSAGE_RECALLED_MEMORY_UNIQUE_CONSTRAINT,
)
from mem_ai.modules.conversations.prompt import HistoryRole, PromptHistoryMessage
from mem_ai.modules.conversations.queries import (
    ensure_conversation_accessible as ensure_conversation_accessible,
)
from mem_ai.modules.conversations.queries import (
    get_generation_for_owner as get_generation_for_owner,
)
from mem_ai.modules.conversations.queries import (
    list_conversation_messages_for_owner as list_conversation_messages_for_owner,
)
from mem_ai.modules.conversations.queries import (
    list_recalled_memories_for_message_for_owner as list_recalled_memories_for_message_for_owner,
)
from mem_ai.modules.conversations.schemas import (
    CommittedChatResult,
    GenerationRecord,
    IdempotencyKeySpec,
    IdempotencyRecordDTO,
    MessageRecord,
    PreparedGeneration,
    RecalledMemoryReferenceRecord,
)
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.users import service as users_service

BeforeCommitHook = Callable[[AsyncSession, GenerationRecord, MessageRecord], Awaitable[None]]
RecalledMemoryFilterHook = Callable[
    [AsyncSession, Sequence[MemoryRecallItem]],
    Awaitable[list[MemoryRecallItem]],
]

logger = logging.getLogger("mem_ai.conversations.generation")


class GenerationLeaseHeartbeat:
    """Periodic lease renewal context manager shared by sync and streaming generation execution."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        prepared: PreparedGeneration,
        lease_ttl_seconds: float,
        renew_interval_seconds: float | None,
        on_lease_lost: Callable[[AppError], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._prepared = prepared
        self._lease_ttl_seconds = lease_ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._on_lease_lost = on_lease_lost
        self._task: asyncio.Task[None] | None = None
        self.loss_error: AppError | None = None

    async def __aenter__(self) -> GenerationLeaseHeartbeat:
        if self._renew_interval_seconds is not None and self._renew_interval_seconds > 0:
            self._task = asyncio.create_task(self._run_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)
        await self.stop()

    async def stop(self) -> None:
        """Stop the background renewal loop cleanly before Tx 2 or failure convergence."""
        task = self._task
        self._task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run_loop(self) -> None:
        assert self._renew_interval_seconds is not None
        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                await renew_generation_lease_phase(
                    self._session_factory,
                    owner_id=self._prepared.owner_id,
                    conversation_id=self._prepared.conversation_id,
                    generation_id=self._prepared.generation_id,
                    lease_token=self._prepared.lease_token,
                    lease_ttl_seconds=self._lease_ttl_seconds,
                )
            except AppError as exc:
                self.loss_error = exc
                if self._on_lease_lost is not None:
                    self._on_lease_lost(exc)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Transient error renewing generation lease; retrying before expiry",
                    exc_info=True,
                )


class IdempotencyRecordHitError(Exception):
    """Internal Tx 1 signal indicating a live idempotency record was already claimed."""

    def __init__(self, record: IdempotencyRecordDTO) -> None:
        super().__init__("idempotency record already exists")
        self.record = record


class IdempotencyScopeRaceError(Exception):
    """Internal Tx 1 signal indicating a concurrent insert won `uq_idempotency_records_scope`."""


def _generation_conflict_error(
    *,
    conversation_id: UUID | None,
    active_generation_id: UUID | None = None,
) -> ConflictError:
    details: dict[str, object] = {}
    if conversation_id is not None:
        details["conversation_id"] = str(conversation_id)
    if active_generation_id is not None:
        details["active_generation_id"] = str(active_generation_id)
    return ConflictError(
        code="conversation_generation_conflict",
        message="conversation already has an active generation",
        details=details,
    )


async def _resolve_current_time(session: AsyncSession, now: datetime | None) -> datetime:
    if now is None:
        return await repository.get_database_now(session)
    return repository.normalize_utc(now)


async def _inspect_resource_inactivation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> tuple[str, str, int] | None:
    """Return `(code, message, status_code)` when owner or conversation is no longer active."""
    try:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id, for_share=True)
    except ForbiddenError as exc:
        return exc.code, exc.message, exc.status_code

    try:
        await queries.load_accessible_conversation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id, lock_mode="share"
        )
    except (ConflictError, NotFoundError) as exc:
        return exc.code, exc.message, exc.status_code

    return None


async def converge_generation_failure_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
    lease_token: str | None,
    error_code: str,
    error_message: str,
    target_status: Literal["failed", "cancelled"] = "failed",
    current_time: datetime,
    expired_only: bool = False,
    error_status_code: int | None = None,
    idempotency_retention_seconds: float = idempotency.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
) -> GenerationRecord | None:
    """Shared in-session failure/cancellation convergence, lease release, and idempotency sync."""
    resolved_status: Literal["failed", "cancelled"] = target_status
    resolved_error_code = error_code
    resolved_error_message = error_message
    resolved_status_code = 409 if expired_only else error_status_code

    if not expired_only:
        inactivation = await _inspect_resource_inactivation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        if inactivation is not None:
            resolved_status = "failed"
            resolved_error_code, resolved_error_message, resolved_status_code = inactivation

    updated = await repository.conditional_mark_generation_terminal_failure(
        session,
        generation_id=generation_id,
        conversation_id=conversation_id,
        owner_id=owner_id,
        target_status=resolved_status,
        lease_token=lease_token,
        current_time=current_time,
        error_code=resolved_error_code,
        error_message=resolved_error_message,
        expired_only=expired_only,
    )
    if updated is None:
        return None
    await idempotency.finalize_terminal_failure_in_session(
        session,
        generation_id=generation_id,
        target_status=resolved_status,
        error_code=resolved_error_code,
        error_message=resolved_error_message,
        response_status_code=resolved_status_code,
        terminal_at=current_time,
        retention_seconds=idempotency_retention_seconds,
    )
    return GenerationRecord.model_validate(updated)


async def _ensure_idempotency_slot_available_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    idempotency_spec: IdempotencyKeySpec | None,
    current_time: datetime,
) -> None:
    if idempotency_spec is None:
        return
    existing = await idempotency.purge_expired_or_find_live_in_session(
        session,
        user_id=owner_id,
        spec=idempotency_spec,
        current_time=current_time,
    )
    if existing is not None:
        raise IdempotencyRecordHitError(existing)


async def prepare_generation_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    persona: PersonaSnapshot,
    model_id: str,
    user_message_content: str,
    upstream_model: str | None = None,
    max_output_tokens: int | None = None,
    memory_provider_id: str | None = None,
    memory_epoch: int | None = None,
    history_limit: int = 20,
    lease_ttl_seconds: float = 60.0,
    idempotency_spec: IdempotencyKeySpec | None = None,
    idempotency_retention_seconds: float | None = None,
    now: datetime | None = None,
) -> PreparedGeneration:
    """Short Tx 1: claim/reclaim lease, link idempotency placeholder, and persist user turn."""
    if lease_ttl_seconds <= 0:
        raise ValueError("lease_ttl_seconds must be positive")
    lease_token = f"genlease_{uuid4().hex}"
    retention_seconds = (
        idempotency_retention_seconds
        if idempotency_retention_seconds is not None
        else (
            idempotency_spec.retention_seconds
            if idempotency_spec is not None
            else idempotency.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS
        )
    )

    try:
        async with session_factory.begin() as session:
            current_time = await _resolve_current_time(session, now)
            lease_expires_at = current_time + timedelta(seconds=lease_ttl_seconds)

            await users_service.ensure_user_active_in_session(
                session, user_id=owner_id, for_share=True
            )

            created_conversation = conversation_id is None
            if conversation_id is None:
                await _ensure_idempotency_slot_available_in_session(
                    session,
                    owner_id=owner_id,
                    idempotency_spec=idempotency_spec,
                    current_time=current_time,
                )
                conversation = await repository.create_conversation(
                    session,
                    owner_id=owner_id,
                    agent_id=agent_id,
                    persona_version_id=persona.version_id,
                    persona_version_number=persona.version,
                )
                prior_messages: list[PromptHistoryMessage] = []
            else:
                conversation = await queries.load_accessible_conversation_in_session(
                    session,
                    owner_id=owner_id,
                    conversation_id=conversation_id,
                    lock_mode="update",
                )
                # Always resolve an already-claimed idempotency key before checking lease conflict.
                await _ensure_idempotency_slot_available_in_session(
                    session,
                    owner_id=owner_id,
                    idempotency_spec=idempotency_spec,
                    current_time=current_time,
                )
                if conversation.agent_id != agent_id:
                    raise ConflictError(
                        code="conversation_agent_mismatch",
                        message="conversation does not belong to the specified agent",
                    )
                if conversation.persona_version_id != persona.version_id:
                    raise ConflictError(
                        code="persona_version_mismatch",
                        message=(
                            "resolved persona version does not match conversation pinned version"
                        ),
                    )

                active_generation = await repository.get_running_generation_for_conversation(
                    session, conversation_id=conversation.id, for_update=True
                )
                if active_generation is not None:
                    if (
                        active_generation.lease_expires_at is not None
                        and active_generation.lease_expires_at > current_time
                    ):
                        raise _generation_conflict_error(
                            conversation_id=conversation.id,
                            active_generation_id=active_generation.id,
                        )

                    reclaimed = await converge_generation_failure_in_session(
                        session,
                        owner_id=owner_id,
                        conversation_id=conversation.id,
                        generation_id=active_generation.id,
                        lease_token=active_generation.lease_token,
                        target_status="failed",
                        error_code="generation_lease_lost",
                        error_message="generation lease expired and was reclaimed",
                        current_time=current_time,
                        expired_only=True,
                        idempotency_retention_seconds=retention_seconds,
                    )
                    if reclaimed is None:
                        raise _generation_conflict_error(
                            conversation_id=conversation.id,
                            active_generation_id=active_generation.id,
                        )

                history_rows = await repository.list_succeeded_history_messages(
                    session, conversation_id=conversation.id, limit=history_limit
                )
                prior_messages = [
                    PromptHistoryMessage(role=cast(HistoryRole, row.role), content=row.content)
                    for row in history_rows
                ]

            if memory_epoch is not None:
                resolved_memory_epoch = memory_epoch
            elif memory_provider_id is not None:
                resolved_memory_epoch = await memories_service.get_memory_epoch_in_session(
                    session, user_id=owner_id, provider_id=memory_provider_id
                )
            else:
                resolved_memory_epoch = 0

            generation = await repository.create_generation(
                session,
                conversation_id=conversation.id,
                owner_id=owner_id,
                agent_id=agent_id,
                persona_version_id=persona.version_id,
                model_id=model_id,
                memory_epoch=resolved_memory_epoch,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
                started_at=current_time,
            )
            user_sequence = await repository.allocate_message_sequence(
                session, conversation=conversation
            )
            user_message = await repository.create_message(
                session,
                conversation_id=conversation.id,
                generation_id=generation.id,
                role="user",
                content=user_message_content,
                sequence=user_sequence,
            )
            await repository.link_generation_user_message(
                session, generation=generation, user_message_id=user_message.id
            )
            if idempotency_spec is not None:
                await idempotency.create_placeholder_in_session(
                    session,
                    user_id=owner_id,
                    spec=idempotency_spec,
                    conversation_id=conversation.id,
                    generation_id=generation.id,
                )

            prepared = PreparedGeneration(
                conversation_id=conversation.id,
                generation_id=generation.id,
                user_message_id=user_message.id,
                user_message_content=user_message.content,
                owner_id=owner_id,
                agent_id=agent_id,
                persona=persona,
                model_id=model_id,
                upstream_model=upstream_model,
                max_output_tokens=max_output_tokens,
                memory_epoch=resolved_memory_epoch,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
                created_conversation=created_conversation,
                history_messages=tuple(prior_messages),
            )
    except IntegrityError as exc:
        if idempotency_spec is not None and is_unique_violation(
            exc, constraint_name=IDEMPOTENCY_SCOPE_UNIQUE_CONSTRAINT
        ):
            raise IdempotencyScopeRaceError() from exc
        if is_unique_violation(exc, constraint_name=GENERATION_ACTIVE_CONVERSATION_UNIQUE_INDEX):
            raise _generation_conflict_error(conversation_id=conversation_id) from exc
        raise

    return prepared


async def renew_generation_lease_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
    lease_token: str,
    lease_ttl_seconds: float = 60.0,
    now: datetime | None = None,
) -> GenerationRecord:
    """Short Tx: renew an active generation lease after checking user, conversation, and token."""
    if lease_ttl_seconds <= 0:
        raise ValueError("lease_ttl_seconds must be positive")

    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        await users_service.ensure_user_active_in_session(session, user_id=owner_id, for_share=True)
        await queries.load_accessible_conversation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id, lock_mode="share"
        )
        new_expires_at = current_time + timedelta(seconds=lease_ttl_seconds)
        updated = await repository.conditional_renew_generation_lease(
            session,
            generation_id=generation_id,
            conversation_id=conversation_id,
            owner_id=owner_id,
            lease_token=lease_token,
            current_time=current_time,
            new_lease_expires_at=new_expires_at,
        )
        if updated is None:
            raise ConflictError(
                code="generation_lease_lost",
                message="generation lease is no longer active or has expired",
            )
        return GenerationRecord.model_validate(updated)


async def commit_generation_success_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    assistant_content: str,
    usage: ModelUsage,
    finish_reason: str | None = None,
    before_commit_hook: BeforeCommitHook | None = None,
    idempotency_retention_seconds: float = idempotency.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    now: datetime | None = None,
) -> CommittedChatResult:
    """Short Tx 2: atomically commit reply, succeeded status, idempotency result, and hooks."""
    try:
        async with session_factory.begin() as session:
            completed_at = await _resolve_current_time(session, now)
            await users_service.ensure_user_active_in_session(
                session, user_id=prepared.owner_id, for_share=True
            )
            conversation = await queries.load_accessible_conversation_in_session(
                session,
                owner_id=prepared.owner_id,
                conversation_id=prepared.conversation_id,
                lock_mode="update",
            )

            assistant_sequence = await repository.allocate_message_sequence(
                session, conversation=conversation
            )
            assistant_message = await repository.create_message(
                session,
                conversation_id=conversation.id,
                generation_id=prepared.generation_id,
                role="assistant",
                content=assistant_content,
                sequence=assistant_sequence,
            )
            updated_generation = await repository.conditional_mark_generation_succeeded(
                session,
                generation_id=prepared.generation_id,
                conversation_id=conversation.id,
                owner_id=prepared.owner_id,
                lease_token=prepared.lease_token,
                assistant_message_id=assistant_message.id,
                usage=usage,
                finish_reason=finish_reason,
                completed_at=completed_at,
            )
            if updated_generation is None:
                raise ConflictError(
                    code="generation_lease_lost",
                    message="generation is no longer active or lease was lost",
                )

            await idempotency.finalize_succeeded_in_session(
                session,
                generation_id=prepared.generation_id,
                terminal_at=completed_at,
                retention_seconds=idempotency_retention_seconds,
            )
            generation_record = GenerationRecord.model_validate(updated_generation)
            assistant_record = MessageRecord.model_validate(assistant_message)

            if before_commit_hook is not None:
                await before_commit_hook(session, generation_record, assistant_record)
    except IntegrityError as exc:
        if is_unique_violation(exc, constraint_name=MESSAGE_GENERATION_ROLE_UNIQUE_CONSTRAINT):
            raise ConflictError(
                code="generation_lease_lost",
                message="generation reply was already committed or lease was lost",
            ) from exc
        raise

    return CommittedChatResult(
        conversation_id=prepared.conversation_id,
        generation_id=prepared.generation_id,
        assistant_message=assistant_record,
        model_id=prepared.model_id,
        usage=usage,
        finish_reason=finish_reason,
    )


async def converge_generation_failure_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
    lease_token: str,
    error_code: str,
    error_message: str,
    target_status: Literal["failed", "cancelled"] = "failed",
    error_status_code: int | None = None,
    idempotency_retention_seconds: float = idempotency.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    now: datetime | None = None,
) -> GenerationRecord | None:
    """Short Tx: conditionally converge a `running` generation to `failed` or `cancelled`."""
    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        return await converge_generation_failure_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
            generation_id=generation_id,
            lease_token=lease_token,
            target_status=target_status,
            error_code=error_code,
            error_message=error_message,
            current_time=current_time,
            expired_only=False,
            error_status_code=error_status_code,
            idempotency_retention_seconds=idempotency_retention_seconds,
        )


async def recover_expired_generations_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    batch_size: int = 50,
    idempotency_retention_seconds: float = idempotency.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    now: datetime | None = None,
) -> list[GenerationRecord]:
    """Short Tx: fallback recovery converging expired `running` generations via shared entry."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    recovered: list[GenerationRecord] = []
    async with session_factory.begin() as session:
        current_time = await _resolve_current_time(session, now)
        expired_rows = await repository.list_expired_running_generations_for_update(
            session, current_time=current_time, limit=batch_size
        )
        for row in expired_rows:
            converged = await converge_generation_failure_in_session(
                session,
                owner_id=row.owner_id,
                conversation_id=row.conversation_id,
                generation_id=row.id,
                lease_token=row.lease_token,
                target_status="failed",
                error_code="generation_lease_lost",
                error_message="generation lease expired before completion",
                current_time=current_time,
                expired_only=True,
                idempotency_retention_seconds=idempotency_retention_seconds,
            )
            if converged is not None:
                recovered.append(converged)
    return recovered


async def persist_recalled_memories_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    candidates: Sequence[MemoryRecallItem],
    filter_in_session: RecalledMemoryFilterHook | None = None,
    now: datetime | None = None,
) -> tuple[tuple[MemoryRecallItem, ...], list[RecalledMemoryReferenceRecord]]:
    """Short Tx 1.5: re-verify status/lease, apply guard hook, and persist metadata-only refs."""
    try:
        async with session_factory.begin() as session:
            current_time = await _resolve_current_time(session, now)
            await users_service.ensure_user_active_in_session(
                session, user_id=prepared.owner_id, for_share=True
            )
            await queries.load_accessible_conversation_in_session(
                session,
                owner_id=prepared.owner_id,
                conversation_id=prepared.conversation_id,
                lock_mode="share",
            )
            active_generation = await repository.get_active_generation_by_lease(
                session,
                generation_id=prepared.generation_id,
                conversation_id=prepared.conversation_id,
                owner_id=prepared.owner_id,
                lease_token=prepared.lease_token,
                current_time=current_time,
            )
            if active_generation is None:
                raise ConflictError(
                    code="generation_lease_lost",
                    message="generation lease is no longer active or has expired",
                )
            user_message = await repository.get_message_for_conversation(
                session,
                conversation_id=prepared.conversation_id,
                message_id=prepared.user_message_id,
                generation_id=prepared.generation_id,
                role="user",
            )
            if user_message is None:
                raise NotFoundError(
                    code="message_not_found", message="generation user message not found"
                )
            filtered = (
                await filter_in_session(session, candidates)
                if (candidates and filter_in_session is not None)
                else list(candidates)
            )
            if not filtered:
                return (), []
            rows = await repository.create_recalled_memory_references(
                session,
                user_message_id=user_message.id,
                items=filtered,
                recalled_at=current_time,
            )
            return tuple(filtered), [RecalledMemoryReferenceRecord.model_validate(r) for r in rows]
    except IntegrityError as exc:
        if is_unique_violation(exc, constraint_name=MESSAGE_RECALLED_MEMORY_UNIQUE_CONSTRAINT):
            raise ConflictError(
                code="recalled_memory_conflict",
                message="recalled memory reference already recorded for this message",
            ) from exc
        raise
