"""Pydantic schemas and immutable application DTOs for the `agents` module."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from mem_ai.contracts.persona_provider import MemoryPolicy, MemoryWriteScope

AgentStatus = Literal["active", "retired"]


def _strip_required_text(value: str, *, field_name: str, max_length: int) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be empty")
    if len(stripped) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length}")
    return stripped


def _strip_optional_text(value: str | None, *, field_name: str, max_length: int) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be blank when provided")
    if len(stripped) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length}")
    return stripped


def _normalize_behavior_rules(rules: list[str]) -> list[str]:
    if len(rules) > 50:
        raise ValueError("behaviorRules must contain at most 50 entries")
    normalized: list[str] = []
    for item in rules:
        stripped = item.strip()
        if not stripped:
            raise ValueError("behaviorRules entries must not be empty")
        if len(stripped) > 1000:
            raise ValueError("behaviorRules entry exceeds maximum length of 1000")
        normalized.append(stripped)
    return normalized


AgentNameStr = Annotated[
    str,
    AfterValidator(lambda v: _strip_required_text(v, field_name="name", max_length=120)),
]
SystemPromptStr = Annotated[
    str,
    AfterValidator(lambda v: _strip_required_text(v, field_name="systemPrompt", max_length=32000)),
]
BehaviorRulesList = Annotated[list[str], AfterValidator(_normalize_behavior_rules)]
OptionalToneStr = Annotated[
    str | None,
    AfterValidator(lambda v: _strip_optional_text(v, field_name="tone", max_length=64)),
]
OptionalLanguageStr = Annotated[
    str | None,
    AfterValidator(lambda v: _strip_optional_text(v, field_name="language", max_length=32)),
]
OptionalModelIdStr = Annotated[
    str | None,
    AfterValidator(lambda v: _strip_optional_text(v, field_name="defaultModelId", max_length=128)),
]


class MemoryPolicyPayload(BaseModel):
    """Strict HTTP camelCase request schema for persona memory policy."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    write_enabled: bool = Field(default=True, alias="writeEnabled")
    write_scope: MemoryWriteScope = Field(default="user", alias="writeScope")
    recall_limit: int = Field(default=5, ge=0, le=50, alias="recallLimit")

    def to_contract(self) -> MemoryPolicy:
        return MemoryPolicy(
            enabled=self.enabled,
            write_enabled=self.write_enabled,
            write_scope=self.write_scope,
            recall_limit=self.recall_limit,
        )


class MemoryPolicyResponse(BaseModel):
    """Public HTTP camelCase response schema for persona memory policy."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    enabled: bool = True
    write_enabled: bool = Field(
        default=True,
        alias="writeEnabled",
        serialization_alias="writeEnabled",
    )
    write_scope: MemoryWriteScope = Field(
        default="user",
        alias="writeScope",
        serialization_alias="writeScope",
    )
    recall_limit: int = Field(
        default=5,
        ge=0,
        le=50,
        alias="recallLimit",
        serialization_alias="recallLimit",
    )

    @classmethod
    def from_contract(cls, policy: MemoryPolicy) -> MemoryPolicyResponse:
        return cls(
            enabled=policy.enabled,
            write_enabled=policy.write_enabled,
            write_scope=policy.write_scope,
            recall_limit=policy.recall_limit,
        )


class MemoryPolicyPatch(BaseModel):
    """Partial update payload for `memoryPolicy` inside `PATCH /v1/agents/{agentId}`."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    write_enabled: bool | None = Field(default=None, alias="writeEnabled")
    write_scope: MemoryWriteScope | None = Field(default=None, alias="writeScope")
    recall_limit: int | None = Field(default=None, ge=0, le=50, alias="recallLimit")

    @model_validator(mode="after")
    def _validate_patch_fields(self) -> MemoryPolicyPatch:
        if not self.model_fields_set:
            raise ValueError("memoryPolicy must include at least one field to update")
        alias_map = {
            "enabled": "enabled",
            "write_enabled": "writeEnabled",
            "write_scope": "writeScope",
            "recall_limit": "recallLimit",
        }
        for attr, wire_name in alias_map.items():
            if attr in self.model_fields_set and getattr(self, attr) is None:
                raise ValueError(f"memoryPolicy.{wire_name} cannot be null")
        return self


class AgentDraftRecord(BaseModel):
    """Immutable application-layer snapshot of an Agent's current draft."""

    model_config = ConfigDict(frozen=True)

    system_prompt: str
    behavior_rules: tuple[str, ...] = ()
    tone: str | None = None
    language: str | None = None
    default_model_id: str | None = None
    memory_policy: MemoryPolicy = Field(default_factory=MemoryPolicy)


class PersonaVersionRecord(BaseModel):
    """Immutable application-layer DTO for a persisted `persona_versions` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    version_id: UUID = Field(validation_alias="id")
    agent_id: UUID
    version: int = Field(ge=1)
    system_prompt: str
    behavior_rules: tuple[str, ...] = ()
    tone: str | None = None
    language: str | None = None
    default_model_id: str | None = None
    memory_policy: MemoryPolicy = Field(default_factory=MemoryPolicy)
    published_at: datetime | None = None
    created_at: datetime


class AgentSummaryRecord(BaseModel):
    """Immutable application-layer summary DTO for Agent listings."""

    model_config = ConfigDict(frozen=True)

    agent_id: UUID
    owner_id: UUID
    name: str
    status: AgentStatus
    current_published_version_id: UUID | None = None
    current_published_version: int | None = None
    latest_version: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime
    retired_at: datetime | None = None


class AgentRecord(AgentSummaryRecord):
    """Immutable application-layer detail DTO for a single private Agent."""

    draft: AgentDraftRecord
    published_version: PersonaVersionRecord | None = None


class AgentCreateRequest(BaseModel):
    """Request body for `POST /v1/agents` (owner is always derived from auth context)."""

    model_config = ConfigDict(extra="forbid")

    name: AgentNameStr
    system_prompt: SystemPromptStr = Field(alias="systemPrompt")
    behavior_rules: BehaviorRulesList = Field(default_factory=list, alias="behaviorRules")
    tone: OptionalToneStr = None
    language: OptionalLanguageStr = None
    default_model_id: OptionalModelIdStr = Field(default=None, alias="defaultModelId")
    memory_policy: MemoryPolicyPayload = Field(
        default_factory=MemoryPolicyPayload,
        alias="memoryPolicy",
    )


class AgentUpdateDraftRequest(BaseModel):
    """Request body for `PATCH /v1/agents/{agentId}` updating only the Agent draft."""

    model_config = ConfigDict(extra="forbid")

    name: AgentNameStr | None = None
    system_prompt: SystemPromptStr | None = Field(default=None, alias="systemPrompt")
    behavior_rules: BehaviorRulesList | None = Field(default=None, alias="behaviorRules")
    tone: OptionalToneStr = None
    language: OptionalLanguageStr = None
    default_model_id: OptionalModelIdStr = Field(default=None, alias="defaultModelId")
    memory_policy: MemoryPolicyPatch | None = Field(default=None, alias="memoryPolicy")

    @model_validator(mode="after")
    def _validate_patch_semantics(self) -> AgentUpdateDraftRequest:
        if not self.model_fields_set:
            raise ValueError("at least one draft field must be provided for update")
        non_nullable = {
            "name": "name",
            "system_prompt": "systemPrompt",
            "behavior_rules": "behaviorRules",
            "memory_policy": "memoryPolicy",
        }
        for attr, wire_name in non_nullable.items():
            if attr in self.model_fields_set and getattr(self, attr) is None:
                raise ValueError(f"{wire_name} cannot be null")
        return self


class AgentPublishRequest(BaseModel):
    """Optional request body for `POST /v1/agents/{agentId}/publish`."""

    model_config = ConfigDict(extra="forbid")

    version_id: UUID | None = Field(default=None, alias="versionId")

    @model_validator(mode="after")
    def _reject_explicit_null_version_id(self) -> AgentPublishRequest:
        if "version_id" in self.model_fields_set and self.version_id is None:
            raise ValueError("versionId cannot be null when provided")
        return self


class AgentDraftResponse(BaseModel):
    """Public HTTP payload representing an Agent's mutable draft."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    system_prompt: str = Field(alias="systemPrompt", serialization_alias="systemPrompt")
    behavior_rules: list[str] = Field(alias="behaviorRules", serialization_alias="behaviorRules")
    tone: str | None = None
    language: str | None = None
    default_model_id: str | None = Field(
        default=None,
        alias="defaultModelId",
        serialization_alias="defaultModelId",
    )
    memory_policy: MemoryPolicyResponse = Field(
        alias="memoryPolicy",
        serialization_alias="memoryPolicy",
    )

    @classmethod
    def from_record(cls, record: AgentDraftRecord) -> AgentDraftResponse:
        return cls(
            system_prompt=record.system_prompt,
            behavior_rules=list(record.behavior_rules),
            tone=record.tone,
            language=record.language,
            default_model_id=record.default_model_id,
            memory_policy=MemoryPolicyResponse.from_contract(record.memory_policy),
        )


class PersonaVersionResponse(BaseModel):
    """Public HTTP payload for an immutable persona version snapshot."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    version_id: UUID = Field(alias="versionId", serialization_alias="versionId")
    agent_id: UUID = Field(alias="agentId", serialization_alias="agentId")
    version: int
    system_prompt: str = Field(alias="systemPrompt", serialization_alias="systemPrompt")
    behavior_rules: list[str] = Field(alias="behaviorRules", serialization_alias="behaviorRules")
    tone: str | None = None
    language: str | None = None
    default_model_id: str | None = Field(
        default=None,
        alias="defaultModelId",
        serialization_alias="defaultModelId",
    )
    memory_policy: MemoryPolicyResponse = Field(
        alias="memoryPolicy",
        serialization_alias="memoryPolicy",
    )
    published_at: datetime | None = Field(
        default=None,
        alias="publishedAt",
        serialization_alias="publishedAt",
    )
    created_at: datetime = Field(alias="createdAt", serialization_alias="createdAt")

    @classmethod
    def from_record(cls, record: PersonaVersionRecord) -> PersonaVersionResponse:
        return cls(
            version_id=record.version_id,
            agent_id=record.agent_id,
            version=record.version,
            system_prompt=record.system_prompt,
            behavior_rules=list(record.behavior_rules),
            tone=record.tone,
            language=record.language,
            default_model_id=record.default_model_id,
            memory_policy=MemoryPolicyResponse.from_contract(record.memory_policy),
            published_at=record.published_at,
            created_at=record.created_at,
        )


class _AgentSummaryFields(BaseModel):
    """Shared camelCase summary fields for Agent list and detail responses."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    agent_id: UUID = Field(alias="agentId", serialization_alias="agentId")
    owner_id: UUID = Field(alias="ownerId", serialization_alias="ownerId")
    name: str
    status: AgentStatus
    current_published_version_id: UUID | None = Field(
        default=None,
        alias="currentPublishedVersionId",
        serialization_alias="currentPublishedVersionId",
    )
    current_published_version: int | None = Field(
        default=None,
        alias="currentPublishedVersion",
        serialization_alias="currentPublishedVersion",
    )
    latest_version: int = Field(alias="latestVersion", serialization_alias="latestVersion")
    created_at: datetime = Field(alias="createdAt", serialization_alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt", serialization_alias="updatedAt")
    retired_at: datetime | None = Field(
        default=None,
        alias="retiredAt",
        serialization_alias="retiredAt",
    )


class AgentSummaryResponse(_AgentSummaryFields):
    """Public HTTP item payload returned in `GET /v1/agents`."""

    @classmethod
    def from_summary_record(cls, record: AgentSummaryRecord) -> AgentSummaryResponse:
        return cls(
            agent_id=record.agent_id,
            owner_id=record.owner_id,
            name=record.name,
            status=record.status,
            current_published_version_id=record.current_published_version_id,
            current_published_version=record.current_published_version,
            latest_version=record.latest_version,
            created_at=record.created_at,
            updated_at=record.updated_at,
            retired_at=record.retired_at,
        )


class AgentDetailResponse(_AgentSummaryFields):
    """Public HTTP detail payload returned for create, get, patch, publish, and retire."""

    draft: AgentDraftResponse
    published_version: PersonaVersionResponse | None = Field(
        default=None,
        alias="publishedVersion",
        serialization_alias="publishedVersion",
    )

    @classmethod
    def from_detail_record(cls, record: AgentRecord) -> AgentDetailResponse:
        return cls(
            agent_id=record.agent_id,
            owner_id=record.owner_id,
            name=record.name,
            status=record.status,
            current_published_version_id=record.current_published_version_id,
            current_published_version=record.current_published_version,
            latest_version=record.latest_version,
            created_at=record.created_at,
            updated_at=record.updated_at,
            retired_at=record.retired_at,
            draft=AgentDraftResponse.from_record(record.draft),
            published_version=(
                PersonaVersionResponse.from_record(record.published_version)
                if record.published_version is not None
                else None
            ),
        )
