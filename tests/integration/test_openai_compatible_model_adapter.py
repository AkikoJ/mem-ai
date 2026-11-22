"""Contract tests for the OpenAI-compatible `ModelProvider` adapter over controlled HTTP."""

from __future__ import annotations

import json
import logging
from io import StringIO
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from mem_ai.contracts.model_provider import (
    ModelMessage,
    ModelProvider,
    ModelRequest,
    ModelUsage,
)
from mem_ai.core.exceptions import BadGatewayError, GatewayTimeoutError
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider


@pytest.mark.anyio
async def test_openai_compatible_generate_maps_request_response_and_usage() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("authorization")
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-123",
                "model": "gpt-4o-mini-2024-07-18",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello from upstream!"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 18,
                    "completion_tokens": 7,
                    "total_tokens": 25,
                },
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1/",
            api_key=SecretStr("sk-server-secret-token-xyz"),
            timeout_seconds=12.0,
            upstream_model_map={"registered-fast": "vendor-fast-v1"},
        )
        assert isinstance(adapter, ModelProvider)

        response = await adapter.generate(
            ModelRequest(
                model_id="registered-fast",
                messages=[
                    ModelMessage(role="system", content="Be concise."),
                    ModelMessage(role="user", content="Say hi"),
                ],
                max_output_tokens=128,
                temperature=0.3,
            )
        )

    assert captured["method"] == "POST"
    assert captured["url"] == "https://llm.example.test/v1/chat/completions"
    assert captured["authorization"] == "Bearer sk-server-secret-token-xyz"
    assert captured["body"] == {
        "model": "vendor-fast-v1",
        "messages": [
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Say hi"},
        ],
        "stream": False,
        "max_tokens": 128,
        "temperature": 0.3,
    }
    assert response.model_id == "registered-fast"
    assert response.content == "Hello from upstream!"
    assert response.finish_reason == "stop"
    assert response.usage == ModelUsage(input_tokens=18, output_tokens=7, total_tokens=25)


@pytest.mark.anyio
async def test_openai_compatible_generate_maps_timeout_to_generation_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("upstream timed out", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-secret-timeout"),
            timeout_seconds=1.5,
        )
        with pytest.raises(GatewayTimeoutError) as exc_info:
            await adapter.generate(
                ModelRequest(
                    model_id="gpt-4o-mini",
                    messages=[ModelMessage(role="user", content="sensitive user prompt")],
                    timeout_seconds=0.5,
                )
            )

    assert exc_info.value.status_code == 504
    assert exc_info.value.code == "generation_timeout"
    assert "sk-secret-timeout" not in exc_info.value.message
    assert "sensitive user prompt" not in exc_info.value.message


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (500, {"error": {"message": "internal vendor failure with sk-leak-999"}}),
        (429, {"error": {"message": "rate limit exceeded"}}),
        (200, {"choices": []}),
        (200, {"choices": [{"message": {"role": "assistant", "content": None}}]}),
    ],
)
async def test_openai_compatible_generate_maps_upstream_and_malformed_errors_with_redaction(
    status_code: int,
    payload: dict[str, Any],
) -> None:
    log_buffer = StringIO()
    handler_log = logging.StreamHandler(log_buffer)
    handler_log.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(handler_log)
    logger.setLevel(logging.INFO)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=payload)

    transport = httpx.MockTransport(handler)
    try:
        async with httpx.AsyncClient(transport=transport) as http_client:
            adapter = OpenAICompatibleModelProvider(
                http_client=http_client,
                base_url="https://llm.example.test/v1",
                api_key=SecretStr("sk-secret-do-not-log"),
            )
            with pytest.raises(BadGatewayError) as exc_info:
                await adapter.generate(
                    ModelRequest(
                        model_id="gpt-4o-mini",
                        messages=[ModelMessage(role="user", content="private message body")],
                    )
                )
    finally:
        logger.removeHandler(handler_log)

    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "model_upstream_error"
    combined_error = f"{exc_info.value.message} {json.dumps(exc_info.value.details)}"
    assert "sk-secret-do-not-log" not in combined_error
    assert "sk-leak-999" not in combined_error
    assert "private message body" not in combined_error

    raw_logs = log_buffer.getvalue()
    assert "sk-secret-do-not-log" not in raw_logs
    assert "private message body" not in raw_logs
