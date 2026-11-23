"""Shared conversation ownership/status policies and read-only generation/message queries.

Centralizes the single resource access policy (`active` vs `deleting` vs missing/foreign)
used across generation write phases, idempotent replay, generation status queries, and
background turn-memory loading.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.api.pagination import (
    CursorPayload,
    PaginatedResponse,
    decode_cursor,
    encode_cursor,
    hash_filters,
)
from mem_ai.core.exceptions import ConflictError, NotFoundError, UnprocessableEntityError
from mem_ai.modules.conversations import history_repository, repository
from mem_ai.modules.conversations.models import Conversation
from mem_ai.modules.conversations.schemas import (
    ConversationRecord,
    ConversationResponse,
    GenerationDetail,
    GenerationRecord,
    GenerationResponse,
    HistoryMessageResponse,
    MessageRecord,
    RecalledMemoryReferenceRecord,
)
from mem_ai.modules.memories.service import TurnContentRecord
from mem_ai.modules.users import service as users_service

ConversationLockMode = Literal["none", "share", "update"]


def _page_anchor(
    cursor: str | None,
    *,
    direction: Literal["asc", "desc"],
    filters_hash: str,
) -> tuple[datetime, UUID] | None:
    if cursor is None:
        return None
    payload = decode_cursor(
        cursor, expected_direction=direction, expected_filters_hash=filters_hash
    )
    try:
        identifier = UUID(str(payload.id))
        if payload.created_at.tzinfo is None:
            raise ValueError("timezone required")
    except ValueError as exc:
        raise UnprocessableEntityError(code="invalid_cursor", message="invalid cursor key") from exc
    return payload.created_at, identifier


async def _conversation_response(session: AsyncSession, row: Conversation) -> ConversationResponse:
    latest = await history_repository.latest_generation(session, conversation_id=row.id)
    detail = (
        await load_generation_detail_in_session(session, row.owner_id, row.id, latest.id)
        if latest is not None
        else None
    )
    return ConversationResponse(
        conversation_id=row.id,
        agent_id=row.agent_id,
        persona_version_id=row.persona_version_id,
        persona_version_number=row.persona_version_number,
        status="active",
        generation=GenerationResponse.from_detail(detail) if detail is not None else None,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


async def list_conversations_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    limit: int,
    cursor: str | None = None,
) -> PaginatedResponse[ConversationResponse]:
    """Read active owned conversations using a cursor bound to the owner and collection."""
    filters = hash_filters({"owner": owner_id, "collection": "conversations"})
    anchor = _page_anchor(cursor, direction="desc", filters_hash=filters)
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        rows = await history_repository.list_conversations(
            session, owner_id=owner_id, limit=limit + 1, after=anchor
        )
        selected = rows[:limit]
        items = [await _conversation_response(session, row) for row in selected]
    has_more = len(rows) > limit
    next_cursor = (
        encode_cursor(
            CursorPayload(
                created_at=selected[-1].created_at,
                id=selected[-1].id,
                direction="desc",
                filters_hash=filters,
            )
        )
        if has_more
        else None
    )
    return PaginatedResponse(items=items, next_cursor=next_cursor, has_more=has_more)


async def get_conversation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> ConversationResponse:
    """Read one active owned conversation with its latest generation."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        row = await _load_readable_conversation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        return await _conversation_response(session, row)


async def page_messages_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    limit: int,
    cursor: str | None = None,
) -> PaginatedResponse[HistoryMessageResponse]:
    """Page persisted messages in sequence order, including incomplete/failed turns."""
    filters = hash_filters({"owner": owner_id, "collection": "messages", "cid": conversation_id})
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        await _load_readable_conversation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        rows = await history_repository.list_messages(
            session,
            conversation_id=conversation_id,
            limit=limit + 1,
            after=_page_anchor(cursor, direction="asc", filters_hash=filters),
        )
        items = []
        for message, generation in rows[:limit]:
            public_generation = GenerationResponse.from_detail(
                GenerationDetail(
                    generation=GenerationRecord.model_validate(generation),
                    assistant_message=None,
                )
            )
            items.append(
                HistoryMessageResponse(
                    message_id=message.id,
                    generation_id=message.generation_id,
                    role=message.role,
                    content=message.content,
                    sequence=message.sequence,
                    generation_status=public_generation.status,
                    error=public_generation.error,
                    created_at=message.created_at,
                )
            )
    has_more = len(rows) > limit
    next_cursor = (
        encode_cursor(
            CursorPayload(
                created_at=items[-1].created_at,
                id=items[-1].message_id,
                direction="asc",
                filters_hash=filters,
            )
        )
        if has_more
        else None
    )
    return PaginatedResponse(items=items, next_cursor=next_cursor, has_more=has_more)


async def load_accessible_conversation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    lock_mode: ConversationLockMode = "none",
) -> Conversation:
    """Single policy for write/continuation access: 404 missing/foreign, 409 deleting."""
    lookup = (
        repository.get_conversation_for_update
        if lock_mode == "update"
        else (
            repository.get_conversation_for_share
            if lock_mode == "share"
            else repository.get_conversation_for_owner
        )
    )
    conversation = await lookup(session, owner_id=owner_id, conversation_id=conversation_id)
    if conversation is None:
        raise NotFoundError(code="conversation_not_found", message="conversation not found")
    if conversation.status == "deleting":
        raise ConflictError(code="conversation_deleting", message="conversation is being deleted")
    return conversation


async def _load_readable_conversation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> Conversation:
    """Single policy for read/recovery queries: 404 for missing, foreign, or deleting."""
    conversation = await repository.get_conversation_for_owner(
        session, owner_id=owner_id, conversation_id=conversation_id
    )
    if conversation is None or conversation.status == "deleting":
        raise NotFoundError(code="conversation_not_found", message="conversation not found")
    return conversation


async def ensure_conversation_accessible_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    for_update: bool = False,
    for_share: bool = False,
) -> ConversationRecord:
    """Validate conversation ownership and active status inside an existing session."""
    mode: ConversationLockMode = "update" if for_update else ("share" if for_share else "none")
    conversation = await load_accessible_conversation_in_session(
        session, owner_id=owner_id, conversation_id=conversation_id, lock_mode=mode
    )
    return ConversationRecord.model_validate(conversation)


async def is_conversation_active_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    for_share: bool = True,
) -> bool:
    """Return True iff `conversation_id` exists for `owner_id` and is `active`."""
    try:
        await ensure_conversation_accessible_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id, for_share=for_share
        )
    except (ConflictError, NotFoundError):
        return False
    return True


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
            session, owner_id=owner_id, conversation_id=conversation_id, for_update=False
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
        session_factory, owner_id=owner_id, conversation_id=conversation_id
    )
    if record.agent_id != expected_agent_id:
        raise ConflictError(
            code="conversation_agent_mismatch",
            message="conversation does not belong to the specified agent",
        )
    return record


async def load_turn_for_memory_write(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> TurnContentRecord | None:
    """Load committed user/assistant turn messages for background derived memory writes."""
    async with session_factory() as session:
        conv = await repository.get_conversation_for_owner(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        if conv is None:
            return None
        gen = await repository.get_generation_for_owner(
            session, owner_id=owner_id, conversation_id=conversation_id, generation_id=generation_id
        )
        if gen is None or gen.user_message_id is None or gen.assistant_message_id is None:
            return None
        user_msg = await repository.get_message_for_conversation(
            session,
            conversation_id=conversation_id,
            message_id=gen.user_message_id,
            generation_id=generation_id,
            role="user",
        )
        asst_msg = await repository.get_message_for_conversation(
            session,
            conversation_id=conversation_id,
            message_id=gen.assistant_message_id,
            generation_id=generation_id,
            role="assistant",
        )
        if user_msg is None or asst_msg is None:
            return None
        return TurnContentRecord(
            generation_id=gen.id,
            conversation_id=conv.id,
            owner_id=owner_id,
            user_message_content=user_msg.content,
            assistant_message_content=asst_msg.content,
            conversation_status=cast(Literal["active", "deleting"], conv.status),
            generation_status=cast(
                Literal["running", "succeeded", "failed", "cancelled"], gen.status
            ),
        )


async def load_generation_detail_in_session(
    session: AsyncSession,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> GenerationDetail:
    """Single in-session read entry for generation status query and idempotent replay."""
    await _load_readable_conversation_in_session(
        session, owner_id=owner_id, conversation_id=conversation_id
    )
    generation = await repository.get_generation_for_owner(
        session, owner_id=owner_id, conversation_id=conversation_id, generation_id=generation_id
    )
    if generation is None:
        raise NotFoundError(code="generation_not_found", message="generation not found")

    assistant_record: MessageRecord | None = None
    if generation.status == "succeeded" and generation.assistant_message_id is not None:
        msg = await repository.get_message_for_conversation(
            session,
            conversation_id=conversation_id,
            message_id=generation.assistant_message_id,
            generation_id=generation.id,
            role="assistant",
        )
        if msg is not None:
            assistant_record = MessageRecord.model_validate(msg)

    return GenerationDetail(
        generation=GenerationRecord.model_validate(generation),
        assistant_message=assistant_record,
    )


async def get_generation_detail_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> GenerationDetail:
    """Read an owned generation and its committed reply, enforcing user and conversation policy."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        return await load_generation_detail_in_session(
            session, owner_id, conversation_id, generation_id
        )


async def get_generation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> GenerationRecord:
    """Read a single owned generation record (used by recovery/query flows and tests)."""
    detail = await get_generation_detail_for_owner(
        session_factory,
        owner_id=owner_id,
        conversation_id=conversation_id,
        generation_id=generation_id,
    )
    return detail.generation


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
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        rows = await repository.list_all_messages_for_conversation(
            session, conversation_id=conversation_id
        )
        return [MessageRecord.model_validate(row) for row in rows]


async def list_recalled_memories_for_message_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    conversation_id: UUID,
    user_message_id: UUID,
) -> list[RecalledMemoryReferenceRecord]:
    """Read ordered metadata-only recalled memory references for an owned user message."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        await _load_readable_conversation_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id
        )
        user_message = await repository.get_message_for_conversation(
            session, conversation_id=conversation_id, message_id=user_message_id, role="user"
        )
        if user_message is None:
            raise NotFoundError(code="message_not_found", message="message not found")
        rows = await repository.list_recalled_memories_for_user_message(
            session, user_message_id=user_message.id
        )
        return [RecalledMemoryReferenceRecord.model_validate(row) for row in rows]


class DatabaseConversationAccessChecker:
    """Real `ConversationAccessChecker` backed by `conversations.queries`."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def ensure_conversation_accessible(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> None:
        await ensure_conversation_accessible(
            self._session_factory, owner_id=owner_id, conversation_id=conversation_id
        )

    async def ensure_conversation_accessible_in_session(
        self, session: AsyncSession, *, owner_id: UUID, conversation_id: UUID
    ) -> None:
        await ensure_conversation_accessible_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id, for_share=True
        )


class DatabaseTurnContentLoader:
    """Read-only `TurnContentLoader` delegating to `conversations.queries`."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def load_turn_for_memory_write(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
        generation_id: UUID,
    ) -> TurnContentRecord | None:
        """Load committed user/assistant turn messages via `conversations.queries`."""
        return await load_turn_for_memory_write(
            self._session_factory,
            owner_id=owner_id,
            conversation_id=conversation_id,
            generation_id=generation_id,
        )

    async def is_conversation_active_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> bool:
        """Re-check conversation ownership and `active` status (`FOR SHARE`) inside caller's Tx."""
        return await is_conversation_active_in_session(
            session, owner_id=owner_id, conversation_id=conversation_id, for_share=True
        )


class DatabaseRecalledMemoryReferenceMarker:
    """In-session marker setting `message_recalled_memories.deleted_at` without reverse imports."""

    async def mark_memory_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        memory_id: str,
        deleted_at: datetime,
    ) -> int:
        """Mark historical recalled references for one owned memory as deleted."""
        return await repository.mark_recalled_memories_deleted_for_memory(
            session,
            owner_id=owner_id,
            provider_id=provider_id,
            memory_id=memory_id,
            deleted_at=deleted_at,
        )

    async def mark_user_memories_deleted_in_session(
        self,
        session: AsyncSession,
        *,
        owner_id: UUID,
        provider_id: str,
        max_memory_epoch: int,
        deleted_at: datetime,
    ) -> int:
        """Mark historical recalled references up to `max_memory_epoch` as deleted."""
        return await repository.mark_recalled_memories_deleted_for_user_epoch(
            session,
            owner_id=owner_id,
            provider_id=provider_id,
            max_memory_epoch=max_memory_epoch,
            deleted_at=deleted_at,
        )
