"""Pydantic DTOs and server-controlled model seed schemas for the `models` module."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ModelConfigSeed(BaseModel):
    """Server-side model registration definition synced idempotently to PostgreSQL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    model_id: str = Field(min_length=1, max_length=128)
    provider_id: str = Field(default="openai_compatible", min_length=1, max_length=64)
    upstream_model: str | None = Field(default=None, min_length=1, max_length=128)
    display_name: str | None = Field(default=None, min_length=1, max_length=128)
    enabled: bool = True
    supports_streaming: bool = True
    max_context_tokens: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)
    required_permission: str | None = Field(default=None, min_length=1, max_length=128)
    credential_ref: str | None = Field(default=None, min_length=1, max_length=128)
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
    required_permission: str | None = None
    credential_ref: str | None = None
    capabilities_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class ResolvedModelSelection(BaseModel):
    """Validated model selection returned by `models.service.resolve_model_for_chat`."""

    model_config = ConfigDict(frozen=True)

    model_id: str
    provider_id: str
    upstream_model: str
    supports_streaming: bool
    max_output_tokens: int | None = None
