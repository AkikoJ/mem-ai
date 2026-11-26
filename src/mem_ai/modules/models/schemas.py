"""Pydantic DTOs, routing policy, and public HTTP schemas for the `models` module."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictBool


def _strip_required_str(value: str, *, field_name: str, max_length: int) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be empty")
    if len(stripped) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length}")
    return stripped


def _strip_optional_str(value: str | None, *, field_name: str, max_length: int) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be blank when provided")
    if len(stripped) > max_length:
        raise ValueError(f"{field_name} exceeds maximum length of {max_length}")
    return stripped


RequiredModelIdStr = Annotated[
    str,
    AfterValidator(lambda v: _strip_required_str(v, field_name="model_id", max_length=128)),
]
RequiredProviderIdStr = Annotated[
    str,
    AfterValidator(lambda v: _strip_required_str(v, field_name="provider_id", max_length=64)),
]
OptionalModelText128 = Annotated[
    str | None,
    AfterValidator(lambda v: _strip_optional_str(v, field_name="value", max_length=128)),
]


class ModelConfigSeed(BaseModel):
    """Server-side model registration definition synced idempotently to PostgreSQL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    model_id: RequiredModelIdStr
    provider_id: RequiredProviderIdStr = "openai_compatible"
    upstream_model: OptionalModelText128 = None
    display_name: OptionalModelText128 = None
    enabled: StrictBool = True
    supports_streaming: StrictBool = True
    max_context_tokens: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)
    credential_ref: OptionalModelText128 = None
    capabilities_json: dict[str, Any] = Field(default_factory=dict)

    def resolved_upstream_model(self) -> str:
        return self.upstream_model if self.upstream_model is not None else self.model_id

    def resolved_display_name(self) -> str:
        return self.display_name if self.display_name is not None else self.model_id


class ModelConfigRecord(BaseModel):
    """Immutable application DTO representing a persisted `model_configs` row."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    model_id: str
    provider_id: str
    upstream_model: str
    display_name: str
    enabled: bool
    supports_streaming: bool
    max_context_tokens: int | None = None
    max_output_tokens: int | None = None
    credential_ref: str | None = None
    capabilities_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class ModelRoutingPolicy(BaseModel):
    """Server-controlled routing policy shared by model discovery and chat model resolution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    forced_model_id: OptionalModelText128 = None
    allow_request_override: bool = False
    default_model_id: OptionalModelText128 = None


class ResolvedModelSelection(BaseModel):
    """Validated model selection returned by `models.service.resolve_model_for_chat`."""

    model_config = ConfigDict(frozen=True)

    model_id: str
    provider_id: str
    upstream_model: str
    supports_streaming: bool
    max_context_tokens: int | None = None
    max_output_tokens: int | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)


class ModelItemResponse(BaseModel):
    """Public HTTP item payload exposing only registered model IDs and capabilities."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    model_id: str = Field(alias="modelId", serialization_alias="modelId")
    display_name: str = Field(alias="displayName", serialization_alias="displayName")
    supports_streaming: bool = Field(
        alias="supportsStreaming",
        serialization_alias="supportsStreaming",
    )
    max_context_tokens: int | None = Field(
        default=None,
        alias="maxContextTokens",
        serialization_alias="maxContextTokens",
    )
    max_output_tokens: int | None = Field(
        default=None,
        alias="maxOutputTokens",
        serialization_alias="maxOutputTokens",
    )
    capabilities: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_record(cls, record: ModelConfigRecord) -> ModelItemResponse:
        return cls(
            model_id=record.model_id,
            display_name=record.display_name,
            supports_streaming=record.supports_streaming,
            max_context_tokens=record.max_context_tokens,
            max_output_tokens=record.max_output_tokens,
            capabilities=dict(record.capabilities_json),
        )


class ModelListResponse(BaseModel):
    """Public HTTP list payload returned by `GET /v1/models`."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    items: list[ModelItemResponse]
