"""OpenAI-compatible HTTP adapter implementing the project-owned `ModelProvider` protocol."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from typing import Any

import httpx
from pydantic import SecretStr

from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.exceptions import BadGatewayError, GatewayTimeoutError

logger = logging.getLogger("mem_ai.models.openai_compatible")


def _non_negative_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and value >= 0:
        return value
    return default


def _parse_usage(raw_usage: Any) -> ModelUsage:
    if not isinstance(raw_usage, dict):
        return ModelUsage()
    input_tokens = _non_negative_int(
        raw_usage.get("prompt_tokens", raw_usage.get("input_tokens")),
    )
    output_tokens = _non_negative_int(
        raw_usage.get("completion_tokens", raw_usage.get("output_tokens")),
    )
    raw_total = raw_usage.get("total_tokens")
    total_tokens = (
        _non_negative_int(raw_total)
        if isinstance(raw_total, int) and not isinstance(raw_total, bool) and raw_total >= 0
        else input_tokens + output_tokens
    )
    return ModelUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
    )


def _extract_choice_content(payload: dict[str, Any]) -> tuple[str, str | None]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise BadGatewayError(
            code="model_upstream_error",
            message="model provider returned an empty response",
        )
    first = choices[0]
    if not isinstance(first, dict):
        raise BadGatewayError(
            code="model_upstream_error",
            message="model provider returned a malformed choice",
        )
    message = first.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("content"), str):
        raise BadGatewayError(
            code="model_upstream_error",
            message="model provider returned a malformed assistant message",
        )
    content = message["content"]
    finish_reason = first.get("finish_reason")
    return content, finish_reason if isinstance(finish_reason, str) else None


class OpenAICompatibleModelProvider:
    """Server-managed OpenAI Chat Completions compatible `ModelProvider` adapter."""

    provider_id = "openai_compatible"

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        base_url: str,
        api_key: SecretStr,
        timeout_seconds: float = 60.0,
        upstream_model_map: Mapping[str, str] | None = None,
    ) -> None:
        normalized_url = base_url.strip().rstrip("/")
        if not normalized_url.startswith(("http://", "https://")):
            raise ValueError("base_url must start with 'http://' or 'https://'")
        if not api_key.get_secret_value().strip():
            raise ValueError("api_key must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._http_client = http_client
        self._completions_url = f"{normalized_url}/chat/completions"
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._upstream_model_map: dict[str, str] = (
            dict(upstream_model_map) if upstream_model_map is not None else {}
        )

    def _resolve_upstream_model(self, request: ModelRequest) -> str:
        if request.upstream_model is not None:
            return request.upstream_model
        return self._upstream_model_map.get(request.model_id, request.model_id)

    async def generate(self, request: ModelRequest) -> ModelResponse:
        """Execute a complete non-streaming chat completion request."""
        upstream_model = self._resolve_upstream_model(request)
        payload: dict[str, Any] = {
            "model": upstream_model,
            "messages": [
                {"role": message.role, "content": message.content} for message in request.messages
            ],
            "stream": False,
        }
        if request.max_output_tokens is not None:
            payload["max_tokens"] = request.max_output_tokens
        if request.temperature is not None:
            payload["temperature"] = request.temperature

        effective_timeout = (
            request.timeout_seconds
            if request.timeout_seconds is not None
            else self._timeout_seconds
        )
        headers = {
            "Authorization": f"Bearer {self._api_key.get_secret_value()}",
            "Content-Type": "application/json",
        }

        try:
            response = await self._http_client.post(
                self._completions_url,
                json=payload,
                headers=headers,
                timeout=effective_timeout,
            )
        except httpx.TimeoutException:
            logger.warning(
                "Model provider request timed out",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "model_id": request.model_id,
                        "error_code": "generation_timeout",
                    }
                },
            )
            raise GatewayTimeoutError(
                code="generation_timeout",
                message="model generation timed out",
            ) from None
        except httpx.RequestError as exc:
            logger.warning(
                "Model provider transport request failed",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "model_id": request.model_id,
                        "error_code": "model_upstream_error",
                        "exception_type": type(exc).__name__,
                    }
                },
            )
            raise BadGatewayError(
                code="model_upstream_error",
                message="model provider request failed",
            ) from None

        if response.status_code >= 400:
            logger.warning(
                "Model provider returned upstream error status",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "model_id": request.model_id,
                        "upstream_status": response.status_code,
                        "error_code": "model_upstream_error",
                    }
                },
            )
            raise BadGatewayError(
                code="model_upstream_error",
                message="model provider returned an error response",
            )

        try:
            body = response.json()
        except ValueError:
            logger.warning(
                "Model provider returned non-JSON response",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "model_id": request.model_id,
                        "error_code": "model_upstream_error",
                    }
                },
            )
            raise BadGatewayError(
                code="model_upstream_error",
                message="model provider returned invalid JSON",
            ) from None

        if not isinstance(body, dict):
            raise BadGatewayError(
                code="model_upstream_error",
                message="model provider returned unexpected payload structure",
            )

        content, finish_reason = _extract_choice_content(body)
        usage = _parse_usage(body.get("usage"))
        return ModelResponse(
            model_id=request.model_id,
            content=content,
            usage=usage,
            finish_reason=finish_reason,
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        """Streaming completion seam extended in Ticket 12."""
        _ = request
        raise NotImplementedError("streaming generation is implemented in slice 12")
        yield  # pragma: no cover - keeps function typed as an async generator
