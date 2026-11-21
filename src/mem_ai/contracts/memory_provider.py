"""Project-owned contract for scoped memory recall, writes, pagination, and deletion."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

MemoryScope = Literal["user", "conversation"]
MemoryOperationState = Literal["succeeded", "accepted", "reconciling", "failed"]


class MemoryRecord(BaseModel):
    """Long-term memory item returned from a memory provider."""

    model_config = ConfigDict(frozen=True)

    memory_id: str = Field(min_length=1)
    provider_id: str = Field(min_length=1)
    user_id: UUID
    scope: MemoryScope
    content: str
    conversation_id: UUID | None = None
    memory_epoch: int = Field(default=0, ge=0)
    created_at: datetime


class MemoryRecallQuery(BaseModel):
    """Query for recalling relevant memories within a user or conversation scope."""

    model_config = ConfigDict(frozen=True)

    user_id: UUID
    query: str = Field(min_length=1)
    scope: MemoryScope
    conversation_id: UUID | None = None
    limit: int = Field(default=5, ge=1, le=50)
    min_relevance: float | None = Field(default=None, ge=0.0, le=1.0)


class MemoryRecallItem(BaseModel):
    """Recalled memory metadata and text snippet returned before prompt compilation."""

    model_config = ConfigDict(frozen=True)

    memory_id: str = Field(min_length=1)
    provider_id: str = Field(min_length=1)
    scope: MemoryScope
    content: str
    relevance: float | None = None
    conversation_id: UUID | None = None
    memory_epoch: int = Field(default=0, ge=0)


class MemoryWriteInput(BaseModel):
    """Normalized payload for explicit memory creation or asynchronous turn memory write."""

    model_config = ConfigDict(frozen=True)

    write_id: str = Field(min_length=1)
    user_id: UUID
    scope: MemoryScope
    content: str = Field(min_length=1)
    conversation_id: UUID | None = None
    memory_epoch: int = Field(default=0, ge=0)


class MemoryWriteResult(BaseModel):
    """Outcome of a provider memory write or explicit create attempt."""

    model_config = ConfigDict(frozen=True)

    status: MemoryOperationState
    provider_id: str = Field(min_length=1)
    memory_id: str | None = None
    external_ref: str | None = None
    error_code: str | None = None


class MemoryListQuery(BaseModel):
    """Paginated listing query for a user's memories."""

    model_config = ConfigDict(frozen=True)

    user_id: UUID
    scope: MemoryScope | None = None
    conversation_id: UUID | None = None
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = None


class MemoryListPage(BaseModel):
    """Paginated list result returned by a memory provider."""

    model_config = ConfigDict(frozen=True)

    items: list[MemoryRecord]
    next_cursor: str | None = None
    has_more: bool = False


class MemoryDeleteResult(BaseModel):
    """Outcome of a single-memory deletion or user-scope clear operation."""

    model_config = ConfigDict(frozen=True)

    status: MemoryOperationState
    external_ref: str | None = None
    error_code: str | None = None


class MemoryOperationStatus(BaseModel):
    """Verification outcome for an external memory operation."""

    model_config = ConfigDict(frozen=True)

    status: MemoryOperationState
    external_ref: str | None = None
    memory_id: str | None = None
    error_code: str | None = None


@runtime_checkable
class MemoryProvider(Protocol):
    """Protocol implemented by replaceable memory adapters such as MemOS."""

    provider_id: str

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        """Recall relevant memories for the requested user and scope."""
        ...

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        """Explicitly create a memory record."""
        ...

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        """Persist derived memory from a completed conversation turn."""
        ...

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        """List memories with stable cursor semantics."""
        ...

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        """Delete a single memory owned by the user."""
        ...

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        """Clear all memories belonging to the user."""
        ...

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        """Verify an in-flight or uncertain external memory operation."""
        ...
