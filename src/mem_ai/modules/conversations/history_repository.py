"""Keyset reads for owned conversations and authoritative message history."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.conversations.models import Conversation, Generation, Message


async def list_conversations(
    session: AsyncSession,
    *,
    owner_id: UUID,
    limit: int,
    after: tuple[datetime, UUID] | None,
) -> Sequence[Conversation]:
    """Newest active conversations first, using the common creation-time/id cursor."""
    stmt = select(Conversation).where(
        Conversation.owner_id == owner_id, Conversation.status == "active"
    )
    if after is not None:
        stmt = stmt.where(tuple_(Conversation.created_at, Conversation.id) < after)
    return (
        await session.scalars(
            stmt.order_by(Conversation.created_at.desc(), Conversation.id.desc()).limit(limit)
        )
    ).all()


async def latest_generation(session: AsyncSession, *, conversation_id: UUID) -> Generation | None:
    """Get the latest persisted turn, including unfinished and failed turns."""
    return await session.scalar(
        select(Generation)
        .where(Generation.conversation_id == conversation_id)
        .order_by(Generation.created_at.desc(), Generation.id.desc())
        .limit(1)
    )


async def list_messages(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    limit: int,
    after: tuple[datetime, UUID] | None,
) -> Sequence[tuple[Message, Generation]]:
    """Page actual messages in sequence order; resolve the cursor to its immutable sequence."""
    stmt = (
        select(Message, Generation)
        .join(Generation, Message.generation_id == Generation.id)
        .where(Message.conversation_id == conversation_id)
    )
    if after is not None:
        anchor = (
            select(Message.sequence)
            .where(
                Message.conversation_id == conversation_id,
                Message.created_at == after[0],
                Message.id == after[1],
            )
            .scalar_subquery()
        )
        stmt = stmt.where(Message.sequence > anchor)
    result = await session.execute(stmt.order_by(Message.sequence).limit(limit))
    return [(message, generation) for message, generation in result]
