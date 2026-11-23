"""Bounded local deletion of one conversation's graph in foreign-key order."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, exists, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.conversations.models import (
    Conversation,
    Generation,
    IdempotencyRecord,
    Message,
    MessageRecalledMemory,
)


async def has_children(session: AsyncSession, *, conversation_id: UUID) -> bool:
    """Check for any remaining generation graph before deleting its conversation."""
    return bool(
        await session.scalar(select(exists().where(Generation.conversation_id == conversation_id)))
    )


async def delete_batch(session: AsyncSession, *, conversation_id: UUID, batch_size: int) -> str:
    """Delete at most one bounded batch; the caller holds the deleting conversation lock."""
    references = list(
        await session.scalars(
            select(MessageRecalledMemory.id)
            .join(Message, Message.id == MessageRecalledMemory.user_message_id)
            .where(Message.conversation_id == conversation_id)
            .order_by(MessageRecalledMemory.id)
            .limit(batch_size)
        )
    )
    if references:
        await session.execute(
            delete(MessageRecalledMemory).where(MessageRecalledMemory.id.in_(references))
        )
        return "recalled_references"
    idempotency_ids = list(
        await session.scalars(
            select(IdempotencyRecord.id)
            .where(IdempotencyRecord.conversation_id == conversation_id)
            .order_by(IdempotencyRecord.id)
            .limit(batch_size)
        )
    )
    if idempotency_ids:
        await session.execute(
            delete(IdempotencyRecord).where(IdempotencyRecord.id.in_(idempotency_ids))
        )
        return "idempotency"
    generation_ids = list(
        await session.scalars(
            select(Generation.id)
            .where(Generation.conversation_id == conversation_id, Generation.status != "running")
            .order_by(Generation.id)
            .limit(batch_size)
        )
    )
    if generation_ids:
        # Generations point back to their messages: break these links before removing messages.
        await session.execute(
            update(Generation)
            .where(Generation.id.in_(generation_ids))
            .values(user_message_id=None, assistant_message_id=None)
        )
        await session.execute(delete(Message).where(Message.generation_id.in_(generation_ids)))
        await session.execute(delete(Generation).where(Generation.id.in_(generation_ids)))
    return "generations"


async def delete_empty_conversation(session: AsyncSession, *, conversation_id: UUID) -> None:
    """Remove the root only after all leases, remote writes, and child batches settled."""
    await session.execute(
        delete(Conversation).where(
            Conversation.id == conversation_id, Conversation.status == "deleting"
        )
    )


async def first_user_conversation_id(session: AsyncSession, *, owner_id: UUID) -> UUID | None:
    """Choose the next root without locking ahead of its durable cleanup job."""
    return await session.scalar(
        select(Conversation.id)
        .where(Conversation.owner_id == owner_id)
        .order_by(Conversation.id)
        .limit(1)
    )


async def has_user_conversations(session: AsyncSession, *, owner_id: UUID) -> bool:
    """Keep the deleting owner until every owned conversation job has removed its root."""
    return bool(await session.scalar(select(exists().where(Conversation.owner_id == owner_id))))


async def list_user_running_generation_conversations(
    session: AsyncSession, *, owner_id: UUID, batch_size: int
) -> list[UUID]:
    """Read a bounded lease inventory before user-wide remote cleanup can begin."""
    return list(
        await session.scalars(
            select(Generation.conversation_id)
            .where(Generation.owner_id == owner_id, Generation.status == "running")
            .order_by(Generation.conversation_id)
            .limit(max(1, min(100, batch_size)))
        )
    )


async def has_user_running_generations(session: AsyncSession, *, owner_id: UUID) -> bool:
    """Keep the final memory clear behind every unfinished generation lease."""
    return bool(
        await session.scalar(
            select(exists().where(Generation.owner_id == owner_id, Generation.status == "running"))
        )
    )
