"""PostgreSQL repository for `conversations`, `messages`, and `generations`."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelUsage
from mem_ai.modules.conversations.models import (
    Conversation,
    Generation,
    Message,
    MessageRecalledMemory,
)
from mem_ai.modules.conversations.schemas import MessageRole


def normalize_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def get_database_now(session: AsyncSession) -> datetime:
    """Return PostgreSQL's authoritative UTC transaction timestamp (`SELECT now()`)."""
    db_now = await session.scalar(select(func.now()))
    if not isinstance(db_now, datetime):
        return datetime.now(UTC)
    return normalize_utc(db_now)


def _active_lease_conditions(
    *,
    generation_id: UUID,
    conversation_id: UUID,
    owner_id: UUID,
    lease_token: str,
    current_time: datetime,
) -> tuple[ColumnElement[bool], ...]:
    """Canonical SQL predicates guarding an active, unexpired generation lease."""
    return (
        Generation.id == generation_id,
        Generation.conversation_id == conversation_id,
        Generation.owner_id == owner_id,
        Generation.status == "running",
        Generation.lease_token == lease_token,
        Generation.lease_expires_at.is_not(None),
        Generation.lease_expires_at > current_time,
    )


async def get_conversation_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> Conversation | None:
    """Fetch an owned conversation without acquiring a row lock."""
    stmt = select(Conversation).where(
        Conversation.id == conversation_id,
        Conversation.owner_id == owner_id,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_conversation_for_share(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> Conversation | None:
    """Fetch and shared-lock an owned conversation row (`FOR SHARE`)."""
    stmt = (
        select(Conversation)
        .where(
            Conversation.id == conversation_id,
            Conversation.owner_id == owner_id,
        )
        .with_for_update(read=True)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_conversation_for_update(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
) -> Conversation | None:
    """Fetch and lock an owned conversation row (`FOR UPDATE`)."""
    stmt = (
        select(Conversation)
        .where(
            Conversation.id == conversation_id,
            Conversation.owner_id == owner_id,
        )
        .with_for_update()
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def create_conversation(
    session: AsyncSession,
    *,
    owner_id: UUID,
    agent_id: UUID,
    persona_version_id: UUID,
    persona_version_number: int,
) -> Conversation:
    """Create a new active conversation pinning an immutable persona version snapshot."""
    conversation = Conversation(
        owner_id=owner_id,
        agent_id=agent_id,
        persona_version_id=persona_version_id,
        persona_version_number=persona_version_number,
        status="active",
        next_message_sequence=1,
    )
    session.add(conversation)
    await session.flush()
    return conversation


async def allocate_message_sequence(
    session: AsyncSession,
    *,
    conversation: Conversation,
) -> int:
    """Allocate the next monotonic message sequence inside a locked conversation tx."""
    allocated = conversation.next_message_sequence
    conversation.next_message_sequence = allocated + 1
    await session.flush()
    return allocated


async def create_generation(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    owner_id: UUID,
    agent_id: UUID,
    persona_version_id: UUID,
    model_id: str,
    memory_epoch: int,
    lease_token: str,
    lease_expires_at: datetime,
    started_at: datetime,
) -> Generation:
    """Insert a `running` generation record before any external model invocation."""
    generation = Generation(
        conversation_id=conversation_id,
        owner_id=owner_id,
        agent_id=agent_id,
        persona_version_id=persona_version_id,
        model_id=model_id,
        memory_epoch=memory_epoch,
        status="running",
        lease_token=lease_token,
        lease_expires_at=lease_expires_at,
        started_at=started_at,
    )
    session.add(generation)
    await session.flush()
    return generation


async def create_message(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    generation_id: UUID,
    role: MessageRole,
    content: str,
    sequence: int,
) -> Message:
    """Insert an ordered user or assistant message bound to a generation."""
    message = Message(
        conversation_id=conversation_id,
        generation_id=generation_id,
        role=role,
        content=content,
        sequence=sequence,
    )
    session.add(message)
    await session.flush()
    return message


async def link_generation_user_message(
    session: AsyncSession,
    *,
    generation: Generation,
    user_message_id: UUID,
) -> None:
    """Attach the persisted original user message ID to its running generation."""
    generation.user_message_id = user_message_id
    await session.flush()


async def list_succeeded_history_messages(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    limit: int,
) -> list[Message]:
    """Return up to `limit` most recent messages belonging strictly to `succeeded` turns."""
    if limit <= 0:
        return []
    stmt = (
        select(Message)
        .join(Generation, Message.generation_id == Generation.id)
        .where(
            Message.conversation_id == conversation_id,
            Generation.status == "succeeded",
        )
        .order_by(Message.sequence.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    rows.reverse()
    return rows


async def list_all_messages_for_conversation(
    session: AsyncSession,
    *,
    conversation_id: UUID,
) -> list[Message]:
    """Return all committed messages (including failed/cancelled turns) ordered ascending."""
    stmt = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.sequence.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_generation_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    conversation_id: UUID,
    generation_id: UUID,
) -> Generation | None:
    """Fetch a single generation owned by `owner_id` within `conversation_id`."""
    stmt = select(Generation).where(
        Generation.id == generation_id,
        Generation.conversation_id == conversation_id,
        Generation.owner_id == owner_id,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_running_generation_for_conversation(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    for_update: bool = False,
) -> Generation | None:
    """Fetch the active `running` generation for `conversation_id`, if any."""
    stmt = select(Generation).where(
        Generation.conversation_id == conversation_id,
        Generation.status == "running",
    )
    if for_update:
        stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_expired_running_generations_for_update(
    session: AsyncSession,
    *,
    current_time: datetime,
    limit: int,
) -> list[Generation]:
    """Claim a bounded batch of expired `running` generations with `FOR UPDATE SKIP LOCKED`."""
    stmt = (
        select(Generation)
        .where(
            Generation.status == "running",
            Generation.lease_expires_at.is_not(None),
            Generation.lease_expires_at <= current_time,
        )
        .order_by(Generation.lease_expires_at.asc(), Generation.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def conditional_renew_generation_lease(
    session: AsyncSession,
    *,
    generation_id: UUID,
    conversation_id: UUID,
    owner_id: UUID,
    lease_token: str,
    current_time: datetime,
    new_lease_expires_at: datetime,
) -> Generation | None:
    """Extend an active, unexpired generation lease matching `lease_token`."""
    stmt = (
        update(Generation)
        .where(
            *_active_lease_conditions(
                generation_id=generation_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                lease_token=lease_token,
                current_time=current_time,
            )
        )
        .values(
            lease_expires_at=new_lease_expires_at,
            updated_at=func.now(),
        )
        .returning(Generation)
        .execution_options(populate_existing=True)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.flush()
    return row


async def conditional_mark_generation_succeeded(
    session: AsyncSession,
    *,
    generation_id: UUID,
    conversation_id: UUID,
    owner_id: UUID,
    lease_token: str,
    assistant_message_id: UUID,
    usage: ModelUsage,
    finish_reason: str | None,
    completed_at: datetime,
) -> Generation | None:
    """Atomically mark an unexpired `running` generation matching `lease_token` as `succeeded`."""
    stmt = (
        update(Generation)
        .where(
            *_active_lease_conditions(
                generation_id=generation_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                lease_token=lease_token,
                current_time=completed_at,
            )
        )
        .values(
            status="succeeded",
            assistant_message_id=assistant_message_id,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
            finish_reason=finish_reason,
            error_code=None,
            error_message=None,
            lease_token=None,
            lease_expires_at=None,
            completed_at=completed_at,
            updated_at=func.now(),
        )
        .returning(Generation)
        .execution_options(populate_existing=True)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.flush()
    return row


async def conditional_mark_generation_terminal_failure(
    session: AsyncSession,
    *,
    generation_id: UUID,
    conversation_id: UUID,
    owner_id: UUID,
    target_status: Literal["failed", "cancelled"],
    lease_token: str | None,
    current_time: datetime,
    error_code: str,
    error_message: str,
    expired_only: bool = False,
) -> Generation | None:
    """Conditionally transition a `running` generation to `failed` or `cancelled` and release lease.

    - `expired_only=False` (active executor): requires matching `lease_token` and unexpired lease.
    - `expired_only=True` (takeover / Worker recovery): requires `lease_expires_at <= current_time`.
    """
    if expired_only:
        conditions: list[ColumnElement[bool]] = [
            Generation.id == generation_id,
            Generation.conversation_id == conversation_id,
            Generation.owner_id == owner_id,
            Generation.status == "running",
            Generation.lease_expires_at.is_not(None),
            Generation.lease_expires_at <= current_time,
        ]
        if lease_token is not None:
            conditions.append(Generation.lease_token == lease_token)
    else:
        if lease_token is None:
            raise ValueError("lease_token is required for active-executor failure convergence")
        conditions = list(
            _active_lease_conditions(
                generation_id=generation_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                lease_token=lease_token,
                current_time=current_time,
            )
        )

    stmt = (
        update(Generation)
        .where(*conditions)
        .values(
            status=target_status,
            error_code=error_code,
            error_message=error_message[:500],
            lease_token=None,
            lease_expires_at=None,
            completed_at=current_time,
            updated_at=func.now(),
        )
        .returning(Generation)
        .execution_options(populate_existing=True)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.flush()
    return row


async def get_active_generation_by_lease(
    session: AsyncSession,
    *,
    generation_id: UUID,
    conversation_id: UUID,
    owner_id: UUID,
    lease_token: str,
    current_time: datetime,
) -> Generation | None:
    """Verify and lock an active, unexpired `running` generation matching `lease_token`."""
    stmt = (
        select(Generation)
        .where(
            *_active_lease_conditions(
                generation_id=generation_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                lease_token=lease_token,
                current_time=current_time,
            )
        )
        .with_for_update()
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_message_for_conversation(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    message_id: UUID,
    generation_id: UUID | None = None,
    role: MessageRole | None = None,
) -> Message | None:
    """Fetch a message verified against its conversation (and optional generation/role)."""
    conditions: list[ColumnElement[bool]] = [
        Message.id == message_id,
        Message.conversation_id == conversation_id,
    ]
    if generation_id is not None:
        conditions.append(Message.generation_id == generation_id)
    if role is not None:
        conditions.append(Message.role == role)
    stmt = select(Message).where(*conditions)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def create_recalled_memory_references(
    session: AsyncSession,
    *,
    user_message_id: UUID,
    items: Sequence[MemoryRecallItem],
    recalled_at: datetime,
) -> list[MessageRecalledMemory]:
    """Insert ordered metadata-only rows in `message_recalled_memories` (1-based position)."""
    rows: list[MessageRecalledMemory] = []
    for position, item in enumerate(items, start=1):
        row = MessageRecalledMemory(
            user_message_id=user_message_id,
            provider_id=item.provider_id,
            memory_id=item.memory_id,
            scope=item.scope,
            relevance=item.relevance,
            prompt_position=position,
            memory_epoch=item.memory_epoch,
            recalled_at=recalled_at,
        )
        session.add(row)
        rows.append(row)
    await session.flush()
    return rows


async def list_recalled_memories_for_user_message(
    session: AsyncSession,
    *,
    user_message_id: UUID,
) -> list[MessageRecalledMemory]:
    """Return metadata-only recalled memory references ordered by `prompt_position` ascending."""
    stmt = (
        select(MessageRecalledMemory)
        .where(MessageRecalledMemory.user_message_id == user_message_id)
        .order_by(MessageRecalledMemory.prompt_position.asc(), MessageRecalledMemory.id.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())
