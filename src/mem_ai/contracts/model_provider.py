"""Project-owned contract for model completion, streaming, cancellation, and usage."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

ModelRole = Literal["system", "user", "assistant"]


class ModelMessage(BaseModel):
    """Single prompt message passed to a model provider."""

    model_config = ConfigDict(frozen=True)

    role: ModelRole
    content: str


class ModelUsage(BaseModel):
    """Normalized token usage reported by a model provider."""

    model_config = ConfigDict(frozen=True)

    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)


class ModelRequest(BaseModel):
    """Normalized generation input for both synchronous and streaming calls."""

    model_config = ConfigDict(frozen=True)

    model_id: str = Field(min_length=1)
    upstream_model: str | None = Field(default=None, min_length=1)
    messages: list[ModelMessage] = Field(min_length=1)
    max_output_tokens: int | None = Field(default=None, ge=1)
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    timeout_seconds: float | None = Field(default=None, gt=0.0)


class ModelResponse(BaseModel):
    """Complete model generation result."""

    model_config = ConfigDict(frozen=True)

    model_id: str
    content: str
    usage: ModelUsage = Field(default_factory=ModelUsage)
    finish_reason: str | None = None


class ModelStreamChunk(BaseModel):
    """Normalized incremental text chunk emitted by streaming model providers."""

    model_config = ConfigDict(frozen=True)

    sequence: int = Field(ge=1)
    text: str = ""
    is_final: bool = False
    usage: ModelUsage | None = None
    finish_reason: str | None = None


@runtime_checkable
class ModelProvider(Protocol):
    """Protocol implemented by model provider adapters."""

    async def generate(self, request: ModelRequest) -> ModelResponse:
        """Execute a complete non-streaming generation."""
        ...

    def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        """Execute a streaming generation; callers cancel by closing or cancelling the task."""
        ...
