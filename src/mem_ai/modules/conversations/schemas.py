"""Pydantic HTTP schemas and immutable application DTOs for `conversations`."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from mem_ai.contracts.memory_provider import MemoryRecallItem, MemoryScope
from mem_ai.contracts.model_provider import ModelMessage, ModelUsage
from mem_ai.contracts.persona_provider import PersonaSnapshot
from mem_ai.modules.conversations.prompt import PromptHistoryMessage

ConversationStatus = Literal["active", "deleting"]
GenerationStatus = Literal["running", "succeeded", "failed", "cancelled"]
MessageRole = Literal["user", "assistant"]
DEFAULT_IDEMPOTENCY_RETENTION_SECONDS = 24.0 * 3600.0


def generation_status_url(conversation_id: UUID, generation_id: UUID) -> str:
    """Return the canonical status query path for a conversation generation."""
    return f"/v1/conversations/{conversation_id}/generations/{generation_id}"


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


class RecalledMemoryReferenceRecord(BaseModel):
    """Immutable application DTO for a persisted metadata-only `message_recalled_memories` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    user_message_id: UUID
    provider_id: str
    memory_id: str
    scope: MemoryScope
    relevance: float | None = None
    prompt_position: int = Field(ge=1)
    memory_epoch: int = Field(ge=0)
    recalled_at: datetime
    deleted_at: datetime | None = None
    created_at: datetime


class PreparedContext(BaseModel):
    """Compiled prompt and memory recall metadata passed into model execution."""

    model_config = ConfigDict(frozen=True)

    prepared: PreparedGeneration
    compiled_messages: tuple[ModelMessage, ...]
    recalled_memories: tuple[MemoryRecallItem, ...] = ()
    memory_recall_degraded: bool = False
    memory_degradation_reason: str | None = None


class CommittedChatResult(BaseModel):
    """Atomic commit outcome returned by sync chat execution."""

    model_config = ConfigDict(frozen=True)

    conversation_id: UUID
    generation_id: UUID
    assistant_message: MessageRecord
    model_id: str
    usage: ModelUsage
    finish_reason: str | None = None


class IdempotencyKeySpec(BaseModel):
    """Normalized request identity and semantic digest for chat idempotency."""

    model_config = ConfigDict(frozen=True)

    method: str
    canonical_route: str
    key: str
    request_digest: str
    retention_seconds: float = Field(default=DEFAULT_IDEMPOTENCY_RETENTION_SECONDS, gt=0.0)


class IdempotencyRecordDTO(BaseModel):
    """Immutable application DTO for a persisted `idempotency_records` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    user_id: UUID
    method: str
    canonical_route: str
    idempotency_key: str
    request_digest: str
    conversation_id: UUID
    generation_id: UUID
    status: GenerationStatus
    response_status_code: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    terminal_at: datetime | None = None
    expires_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class GenerationDetail(BaseModel):
    """Read-model combining a generation record with its committed assistant message if any."""

    model_config = ConfigDict(frozen=True)

    generation: GenerationRecord
    assistant_message: MessageRecord | None = None

    @property
    def status_url(self) -> str:
        return generation_status_url(self.generation.conversation_id, self.generation.id)

    def to_committed_chat_result(self) -> CommittedChatResult:
        """Convert a `succeeded` generation detail into a `CommittedChatResult`."""
        if self.generation.status != "succeeded" or self.assistant_message is None:
            raise ValueError("generation is not in a succeeded state with an assistant message")
        usage = ModelUsage(
            input_tokens=self.generation.input_tokens or 0,
            output_tokens=self.generation.output_tokens or 0,
            total_tokens=self.generation.total_tokens or 0,
        )
        return CommittedChatResult(
            conversation_id=self.generation.conversation_id,
            generation_id=self.generation.id,
            assistant_message=self.assistant_message,
            model_id=self.generation.model_id,
            usage=usage,
            finish_reason=self.generation.finish_reason,
        )


class ChatAcceptedResult(BaseModel):
    """Application outcome returned when an idempotent retry hits an active `running` generation."""

    model_config = ConfigDict(frozen=True)

    conversation_id: UUID
    generation_id: UUID
    user_message_id: UUID | None = None
    status: Literal["running"] = "running"
    status_url: str


ChatTurnOutcome = CommittedChatResult | ChatAcceptedResult


class StreamGenerationFailure(BaseModel):
    """Sanitized failure outcome returned after attempting DB convergence for a streamed turn."""

    model_config = ConfigDict(frozen=True)

    code: str
    message: str
    converged: bool


StreamGenerationOutcome = CommittedChatResult | StreamGenerationFailure


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


class ChatAcceptedResponse(BaseModel):
    """202 Accepted payload returned when retrying an in-flight idempotent chat generation."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    conversation_id: UUID = Field(alias="conversationId", serialization_alias="conversationId")
    generation_id: UUID = Field(alias="generationId", serialization_alias="generationId")
    user_message_id: UUID | None = Field(
        default=None, alias="userMessageId", serialization_alias="userMessageId"
    )
    status: Literal["running"] = "running"
    status_url: str = Field(alias="statusUrl", serialization_alias="statusUrl")

    @classmethod
    def from_result(cls, result: ChatAcceptedResult) -> ChatAcceptedResponse:
        return cls(
            conversation_id=result.conversation_id,
            generation_id=result.generation_id,
            user_message_id=result.user_message_id,
            status=result.status,
            status_url=result.status_url,
        )


class GenerationErrorResponse(BaseModel):
    """Sanitized error object exposed in `GET /v1/conversations/{cid}/generations/{gid}`."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    code: str
    message: str


class GenerationResponse(BaseModel):
    """Public HTTP payload for `GET /v1/conversations/{cid}/generations/{gid}`."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    conversation_id: UUID = Field(alias="conversationId", serialization_alias="conversationId")
    generation_id: UUID = Field(alias="generationId", serialization_alias="generationId")
    agent_id: UUID = Field(alias="agentId", serialization_alias="agentId")
    persona_version_id: UUID = Field(
        alias="personaVersionId", serialization_alias="personaVersionId"
    )
    model_id: str = Field(alias="modelId", serialization_alias="modelId")
    status: GenerationStatus
    user_message_id: UUID | None = Field(
        default=None, alias="userMessageId", serialization_alias="userMessageId"
    )
    message_id: UUID | None = Field(
        default=None, alias="messageId", serialization_alias="messageId"
    )
    message: ChatMessageResponse | None = None
    usage: ChatUsageResponse | None = None
    finish_reason: str | None = Field(
        default=None, alias="finishReason", serialization_alias="finishReason"
    )
    error: GenerationErrorResponse | None = None
    started_at: datetime = Field(alias="startedAt", serialization_alias="startedAt")
    completed_at: datetime | None = Field(
        default=None, alias="completedAt", serialization_alias="completedAt"
    )
    created_at: datetime = Field(alias="createdAt", serialization_alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt", serialization_alias="updatedAt")

    @classmethod
    def from_detail(cls, detail: GenerationDetail) -> GenerationResponse:
        gen = detail.generation
        assistant_msg = (
            ChatMessageResponse.from_record(detail.assistant_message)
            if gen.status == "succeeded" and detail.assistant_message is not None
            else None
        )
        usage_resp = (
            ChatUsageResponse(
                input_tokens=gen.input_tokens or 0,
                output_tokens=gen.output_tokens or 0,
                total_tokens=gen.total_tokens or 0,
            )
            if gen.status == "succeeded"
            else None
        )
        error_resp = (
            GenerationErrorResponse(
                code=gen.error_code
                or ("generation_cancelled" if gen.status == "cancelled" else "generation_failed"),
                message=gen.error_message or gen.error_code or "generation did not succeed",
            )
            if gen.status in {"failed", "cancelled"}
            else None
        )
        return cls(
            conversation_id=gen.conversation_id,
            generation_id=gen.id,
            agent_id=gen.agent_id,
            persona_version_id=gen.persona_version_id,
            model_id=gen.model_id,
            status=gen.status,
            user_message_id=gen.user_message_id,
            message_id=assistant_msg.message_id if assistant_msg is not None else None,
            message=assistant_msg,
            usage=usage_resp,
            finish_reason=gen.finish_reason if gen.status == "succeeded" else None,
            error=error_resp,
            started_at=gen.started_at,
            completed_at=gen.completed_at,
            created_at=gen.created_at,
            updated_at=gen.updated_at,
        )
