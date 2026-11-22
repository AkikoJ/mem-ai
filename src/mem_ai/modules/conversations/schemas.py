"""Pydantic HTTP schemas and immutable application DTOs for `conversations`."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelMessage, ModelUsage
from mem_ai.contracts.persona_provider import PersonaSnapshot
from mem_ai.modules.conversations.prompt import PromptHistoryMessage

ConversationStatus = Literal["active", "deleting"]
GenerationStatus = Literal["running", "succeeded", "failed", "cancelled"]
MessageRole = Literal["user", "assistant"]


def _strip_required_message(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("message must not be empty")
    if len(stripped) > 32000:
        raise ValueError("message exceeds maximum length of 32000")
    return stripped


def _strip_optional_model_id(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        raise ValueError("modelId must not be blank when provided")
    if len(stripped) > 128:
        raise ValueError("modelId exceeds maximum length of 128")
    return stripped


ChatMessageStr = Annotated[str, AfterValidator(_strip_required_message)]
OptionalRequestedModelId = Annotated[str | None, AfterValidator(_strip_optional_model_id)]


class ConversationRecord(BaseModel):
    """Immutable application DTO for a persisted `conversations` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    owner_id: UUID
    agent_id: UUID
    persona_version_id: UUID
    persona_version_number: int = Field(ge=1)
    status: ConversationStatus
    next_message_sequence: int = Field(ge=1)
    deleted_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class MessageRecord(BaseModel):
    """Immutable application DTO for a persisted `messages` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    conversation_id: UUID
    generation_id: UUID
    role: MessageRole
    content: str
    sequence: int = Field(ge=1)
    created_at: datetime


class GenerationRecord(BaseModel):
    """Immutable application DTO for a persisted `generations` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    conversation_id: UUID
    owner_id: UUID
    agent_id: UUID
    persona_version_id: UUID
    user_message_id: UUID | None = None
    assistant_message_id: UUID | None = None
    model_id: str
    memory_epoch: int = Field(ge=0)
    status: GenerationStatus
    error_code: str | None = None
    error_message: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    finish_reason: str | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    started_at: datetime
    completed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class PreparedGeneration(BaseModel):
    """Project-owned snapshot returned by the shared generation preparation phase."""

    model_config = ConfigDict(frozen=True)

    conversation_id: UUID
    generation_id: UUID
    user_message_id: UUID
    user_message_content: str
    owner_id: UUID
    agent_id: UUID
    persona: PersonaSnapshot
    model_id: str
    upstream_model: str | None = None
    max_output_tokens: int | None = None
    memory_epoch: int = Field(default=0, ge=0)
    lease_token: str
    lease_expires_at: datetime
    created_conversation: bool
    history_messages: tuple[PromptHistoryMessage, ...] = ()


class PreparedContext(BaseModel):
    """Compiled prompt and memory recall metadata passed into model execution."""

    model_config = ConfigDict(frozen=True)

    prepared: PreparedGeneration
    compiled_messages: tuple[ModelMessage, ...]
    recalled_memories: tuple[MemoryRecallItem, ...] = ()


class CommittedChatResult(BaseModel):
    """Atomic commit outcome returned by sync chat execution."""

    model_config = ConfigDict(frozen=True)

    conversation_id: UUID
    generation_id: UUID
    assistant_message: MessageRecord
    model_id: str
    usage: ModelUsage
    finish_reason: str | None = None


class ChatRequest(BaseModel):
    """Strict HTTP request body for `POST /v1/chat`."""

    model_config = ConfigDict(extra="forbid")

    agent_id: UUID = Field(alias="agentId")
    conversation_id: UUID | None = Field(default=None, alias="conversationId")
    message: ChatMessageStr
    model_id: OptionalRequestedModelId = Field(default=None, alias="modelId")
    stream: bool = False


class ChatMessageResponse(BaseModel):
    """Committed assistant message payload embedded in sync chat responses."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    message_id: UUID = Field(alias="messageId", serialization_alias="messageId")
    role: MessageRole
    content: str
    sequence: int
    created_at: datetime = Field(alias="createdAt", serialization_alias="createdAt")

    @classmethod
    def from_record(cls, record: MessageRecord) -> ChatMessageResponse:
        return cls(
            message_id=record.id,
            role=record.role,
            content=record.content,
            sequence=record.sequence,
            created_at=record.created_at,
        )


class ChatUsageResponse(BaseModel):
    """Normalized token usage payload returned in sync chat responses."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    input_tokens: int = Field(alias="inputTokens", serialization_alias="inputTokens")
    output_tokens: int = Field(alias="outputTokens", serialization_alias="outputTokens")
    total_tokens: int = Field(alias="totalTokens", serialization_alias="totalTokens")

    @classmethod
    def from_usage(cls, usage: ModelUsage) -> ChatUsageResponse:
        return cls(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
        )


class ChatResponse(BaseModel):
    """Public HTTP payload for synchronous `POST /v1/chat`."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    conversation_id: UUID = Field(alias="conversationId", serialization_alias="conversationId")
    generation_id: UUID = Field(alias="generationId", serialization_alias="generationId")
    message_id: UUID = Field(alias="messageId", serialization_alias="messageId")
    message: ChatMessageResponse
    model_id: str = Field(alias="modelId", serialization_alias="modelId")
    usage: ChatUsageResponse

    @classmethod
    def from_result(cls, result: CommittedChatResult) -> ChatResponse:
        return cls(
            conversation_id=result.conversation_id,
            generation_id=result.generation_id,
            message_id=result.assistant_message.id,
            message=ChatMessageResponse.from_record(result.assistant_message),
            model_id=result.model_id,
            usage=ChatUsageResponse.from_usage(result.usage),
        )
