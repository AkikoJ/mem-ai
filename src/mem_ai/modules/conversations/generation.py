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

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.model_provider import ModelUsage
from mem_ai.contracts.persona_provider import PersonaSnapshot
from mem_ai.core.exceptions import ConflictError, NotFoundError
from mem_ai.db.base import is_unique_violation
from mem_ai.modules.conversations import repository
from mem_ai.modules.conversations.models import (
    MESSAGE_GENERATION_ROLE_UNIQUE_CONSTRAINT,
    Conversation,
)
from mem_ai.modules.conversations.prompt import HistoryRole, PromptHistoryMessage
from mem_ai.modules.conversations.schemas import (
    CommittedChatResult,
    ConversationRecord,
    GenerationRecord,
    MessageRecord,
    PreparedGeneration,
)
from mem_ai.modules.users import service as users_service

BeforeCommitHook = Callable[[AsyncSession, GenerationRecord, MessageRecord], Awaitable[None]]


async def _load_accessible_conversation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    for_update: bool = False,
) -> Conversation:
    """Single resource policy for write/continuation access: 404 missing/foreign, 409 deleting."""
    conversation = (
        await repository.get_conversation_for_update(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
        if for_update
        else await repository.get_conversation_for_owner(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
    )
    if conversation is None:
        raise NotFoundError(
            code="conversation_not_found",
            message="conversation not found",
        )
    if conversation.status == "deleting":
        raise ConflictError(
            code="conversation_deleting",
            message="conversation is being deleted",
        )
    return conversation


async def _load_readable_conversation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> Conversation:
    """Single resource policy for read queries: 404 for missing, foreign, or deleting."""
    conversation = await repository.get_conversation_for_owner(
        session,
        owner_id=owner_id,
        conversation_id=conversation_id,
    )
    if conversation is None or conversation.status == "deleting":
        raise NotFoundError(
            code="conversation_not_found",
            message="conversation not found",
        )
    return conversation


async def ensure_conversation_accessible_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    for_update: bool = False,
) -> ConversationRecord:
    """Validate conversation ownership and active status inside an existing session."""
    conversation = await _load_accessible_conversation_in_session(
        session,
        owner_id=owner_id,
        conversation_id=conversation_id,
        for_update=for_update,
    )
    return ConversationRecord.model_validate(conversation)


async def ensure_conversation_accessible(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> ConversationRecord:
    """Verify conversation ownership and active status in a short read session."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        return await ensure_conversation_accessible_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
            for_update=False,
        )


async def load_conversation_for_continuation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    expected_agent_id: UUID,
) -> ConversationRecord:
    """Load an existing owned conversation outside Tx 1 to resolve its pinned persona."""
    record = await ensure_conversation_accessible(
        session_factory,
        owner_id=owner_id,
        conversation_id=conversation_id,
    )
    if record.agent_id != expected_agent_id:
        raise ConflictError(
            code="conversation_agent_mismatch",
            message="conversation does not belong to the specified agent",
        )
    return record


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
    memory_epoch: int = 0,
    history_limit: int = 20,
    lease_ttl_seconds: int = 60,
    now: datetime | None = None,
) -> PreparedGeneration:
    """Short Tx 1: create/lock conversation, load succeeded history, persist running turn."""
    current_time = now if now is not None else datetime.now(UTC)
    lease_token = f"genlease_{uuid4().hex}"
    lease_expires_at = current_time + timedelta(seconds=lease_ttl_seconds)

    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)

        created_conversation = conversation_id is None
        if conversation_id is None:
            conversation = await repository.create_conversation(
                session,
                owner_id=owner_id,
                agent_id=agent_id,
                persona_version_id=persona.version_id,
                persona_version_number=persona.version,
            )
            prior_messages: list[PromptHistoryMessage] = []
        else:
            conversation = await _load_accessible_conversation_in_session(
                session,
                owner_id=owner_id,
                conversation_id=conversation_id,
                for_update=True,
            )
            if conversation.agent_id != agent_id:
                raise ConflictError(
                    code="conversation_agent_mismatch",
                    message="conversation does not belong to the specified agent",
                )
            if conversation.persona_version_id != persona.version_id:
                raise ConflictError(
                    code="persona_version_mismatch",
                    message="resolved persona version does not match conversation pinned version",
                )
            history_rows = await repository.list_succeeded_history_messages(
                session,
                conversation_id=conversation.id,
                limit=history_limit,
            )
            prior_messages = [
                PromptHistoryMessage(
                    role=cast(HistoryRole, row.role),
                    content=row.content,
                )
                for row in history_rows
            ]

        generation = await repository.create_generation(
            session,
            conversation_id=conversation.id,
            owner_id=owner_id,
            agent_id=agent_id,
            persona_version_id=persona.version_id,
            model_id=model_id,
            memory_epoch=memory_epoch,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            started_at=current_time,
        )
        user_sequence = await repository.allocate_message_sequence(
            session,
            conversation=conversation,
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
            session,
            generation=generation,
            user_message_id=user_message.id,
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
            memory_epoch=memory_epoch,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            created_conversation=created_conversation,
            history_messages=tuple(prior_messages),
        )

    return prepared


async def commit_generation_success_phase(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    assistant_content: str,
    usage: ModelUsage,
    finish_reason: str | None = None,
    before_commit_hook: BeforeCommitHook | None = None,
    now: datetime | None = None,
) -> CommittedChatResult:
    """Short Tx 2: atomically commit complete assistant message, succeeded status, and hooks."""
    completed_at = now if now is not None else datetime.now(UTC)

    try:
        async with session_factory.begin() as session:
            await users_service.ensure_user_active_in_session(session, user_id=prepared.owner_id)

            conversation = await _load_accessible_conversation_in_session(
                session,
                owner_id=prepared.owner_id,
                conversation_id=prepared.conversation_id,
                for_update=True,
            )

            assistant_sequence = await repository.allocate_message_sequence(
                session,
                conversation=conversation,
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
    generation_id: UUID,
    lease_token: str | None,
    error_code: str,
    error_message: str,
    target_status: Literal["failed", "cancelled"] = "failed",
    now: datetime | None = None,
) -> GenerationRecord | None:
    """Short Tx: conditionally converge a `running` generation to `failed` or `cancelled`."""
    completed_at = now if now is not None else datetime.now(UTC)
    async with session_factory.begin() as session:
        updated = await repository.conditional_mark_generation_terminal_failure(
            session,
            generation_id=generation_id,
            target_status=target_status,
            lease_token=lease_token,
            error_code=error_code,
            error_message=error_message,
            completed_at=completed_at,
        )
        if updated is None:
            return None
        return GenerationRecord.model_validate(updated)


async def get_generation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> GenerationRecord:
    """Read a single owned generation record (used by recovery/query flows and tests)."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        await _load_readable_conversation_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
        generation = await repository.get_generation_for_owner(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
            generation_id=generation_id,
        )
        if generation is None:
            raise NotFoundError(
                code="generation_not_found",
                message="generation not found",
            )
        return GenerationRecord.model_validate(generation)


async def list_conversation_messages_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> list[MessageRecord]:
    """Read all committed messages for an owned active conversation ordered ascending."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        await _load_readable_conversation_in_session(
            session,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
        rows = await repository.list_all_messages_for_conversation(
            session,
            conversation_id=conversation_id,
        )
        return [MessageRecord.model_validate(row) for row in rows]
