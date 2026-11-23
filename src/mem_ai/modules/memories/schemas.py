"""HTTP request/response schemas and internal DTOs for the `memories` module."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mem_ai.contracts.memory_provider import MemoryRecord, MemoryScope
from mem_ai.modules.operations.schemas import OperationRecord


class MemoryCreateRequest(BaseModel):
    """Strict request schema for `POST /v1/memories`."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    content: str = Field(min_length=1, max_length=10000)
    scope: MemoryScope = "user"
    conversation_id: UUID | None = Field(default=None, alias="conversationId")

    @field_validator("content")
    @classmethod
    def normalize_content(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("content must not be blank")
        return stripped

    @model_validator(mode="after")
    def validate_scope_binding(self) -> MemoryCreateRequest:
        if self.scope == "conversation" and self.conversation_id is None:
            raise ValueError("conversationId is required when scope is 'conversation'")
        if self.scope == "user" and self.conversation_id is not None:
            raise ValueError("conversationId must not be set when scope is 'user'")
        return self


class MemoryItemResponse(BaseModel):
    """CamelCase HTTP representation of a long-term memory record."""

    model_config = ConfigDict(populate_by_name=True)

    memory_id: str = Field(serialization_alias="memoryId")
    provider_id: str = Field(serialization_alias="providerId")
    scope: MemoryScope
    content: str
    conversation_id: UUID | None = Field(default=None, serialization_alias="conversationId")
    memory_epoch: int = Field(serialization_alias="memoryEpoch")
    created_at: datetime = Field(serialization_alias="createdAt")

    @classmethod
    def from_record(cls, record: MemoryRecord) -> MemoryItemResponse:
        return cls(
            memory_id=record.memory_id,
            provider_id=record.provider_id,
            scope=record.scope,
            content=record.content,
            conversation_id=record.conversation_id,
            memory_epoch=record.memory_epoch,
            created_at=record.created_at,
        )


class CreatedMemoryOutcome(BaseModel):
    """Synchronous 201 creation outcome carrying the confirmed memory record."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["created"] = "created"
    record: MemoryRecord


class AcceptedMemoryOutcome(BaseModel):
    """Asynchronous or reconciling 202 outcome carrying the queryable operation."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["accepted"] = "accepted"
    operation: OperationRecord


MemoryCreateOutcome = CreatedMemoryOutcome | AcceptedMemoryOutcome

MemoryTombstoneStatus = Literal["pending", "reconciling", "verified"]
MemoryTombstoneTargetKind = Literal["memory", "conversation", "user"]


class MemoryTombstoneRecord(BaseModel):
    """Immutable DTO representing a persisted `memory_tombstones` row."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    user_id: UUID
    provider_id: str
    target_kind: MemoryTombstoneTargetKind
    memory_id: str | None = None
    conversation_id: UUID | None = None
    memory_epoch: int
    operation_id: UUID | None = None
    status: MemoryTombstoneStatus
    external_ref: str | None = None
    verified_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class CompletedMemoryDeletionOutcome(BaseModel):
    """Confirmed 204 No Content outcome for a single-memory delete or user clear."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["completed"] = "completed"
    operation: OperationRecord | None = None


class AcceptedMemoryDeletionOutcome(BaseModel):
    """Asynchronous or reconciling 202 Accepted outcome carrying the queryable operation."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["accepted"] = "accepted"
    operation: OperationRecord


MemoryDeletionOutcome = CompletedMemoryDeletionOutcome | AcceptedMemoryDeletionOutcome
