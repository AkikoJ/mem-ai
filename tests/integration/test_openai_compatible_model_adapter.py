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


class _ChunkedAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.closed = False

    async def __aiter__(self) -> Any:
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_openai_compatible_stream_generate_parses_deltas_usage_and_done() -> None:
    captured: dict[str, Any] = {}
    sse_payload = b"".join(
        [
            b": keep-alive comment\n\n",
            b'data: {"id":"cmpl-1","choices":[{"index":0,"delta":{"role":"assistant"},'
            b'"finish_reason":null}]}\n\n',
            b'data: {"id":"cmpl-1","choices":[{"index":0,"delta":{"content":"Hello "},'
            b'"finish_reason":null}]}\n\n',
            b'data: {"id":"cmpl-1","choices":[{"index":0,"delta":{"content":"stream!"},'
            b'"finish_reason":"stop"}]}\n\n',
            b'data: {"id":"cmpl-1","choices":[],"usage":{"prompt_tokens":11,'
            b'"completion_tokens":4,"total_tokens":15}}\n\n',
            b"data: [DONE]\n\n",
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("authorization")
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=_ChunkedAsyncStream([sse_payload[:90], sse_payload[90:]]),
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1/",
            api_key=SecretStr("sk-stream-secret"),
            upstream_model_map={"registered-fast": "vendor-fast-v1"},
        )
        chunks = [
            chunk
            async for chunk in adapter.stream_generate(
                ModelRequest(
                    model_id="registered-fast",
                    messages=[ModelMessage(role="user", content="Hi")],
                    max_output_tokens=64,
                )
            )
        ]

    assert captured["method"] == "POST"
    assert captured["url"] == "https://llm.example.test/v1/chat/completions"
    assert captured["authorization"] == "Bearer sk-stream-secret"
    assert captured["body"] == {
        "model": "vendor-fast-v1",
        "messages": [{"role": "user", "content": "Hi"}],
        "stream": True,
        "stream_options": {"include_usage": True},
        "max_tokens": 64,
    }
    assert [(c.sequence, c.text, c.is_final) for c in chunks] == [
        (1, "Hello ", False),
        (2, "stream!", False),
        (3, "", True),
    ]
    assert chunks[-1].finish_reason == "stop"
    assert chunks[-1].usage == ModelUsage(input_tokens=11, output_tokens=4, total_tokens=15)


@pytest.mark.anyio
async def test_openai_compatible_stream_generate_maps_truncated_http_and_timeout_errors() -> None:
    partial_frame = (
        b'data: {"choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}\n\n'
    )

    def truncated_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=_ChunkedAsyncStream([partial_frame]),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(truncated_handler)) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-stream-secret"),
        )
        seen: list[str] = []
        with pytest.raises(BadGatewayError) as exc_info:
            async for chunk in adapter.stream_generate(
                ModelRequest(
                    model_id="gpt-4o-mini",
                    messages=[ModelMessage(role="user", content="Hi")],
                )
            ):
                seen.append(chunk.text)
        assert seen == ["partial"]
        assert exc_info.value.code == "model_upstream_error"

    def error_status_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, json={"error": {"message": "vendor leak sk-stream-secret"}})

    error_transport = httpx.MockTransport(error_status_handler)
    async with httpx.AsyncClient(transport=error_transport) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-stream-secret"),
        )
        with pytest.raises(BadGatewayError) as exc_info:
            async for _ in adapter.stream_generate(
                ModelRequest(
                    model_id="gpt-4o-mini",
                    messages=[ModelMessage(role="user", content="sensitive body")],
                )
            ):
                pass
        assert exc_info.value.code == "model_upstream_error"
        assert "sk-stream-secret" not in exc_info.value.message
        assert "sensitive body" not in exc_info.value.message

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("stream timed out", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout_handler)) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-stream-secret"),
        )
        with pytest.raises(GatewayTimeoutError) as exc_info:
            async for _ in adapter.stream_generate(
                ModelRequest(
                    model_id="gpt-4o-mini",
                    messages=[ModelMessage(role="user", content="Hi")],
                )
            ):
                pass
        assert exc_info.value.code == "generation_timeout"


@pytest.mark.anyio
async def test_openai_compatible_stream_generate_closes_upstream_stream_on_cancel() -> None:
    first_frame = (
        b'data: {"choices":[{"index":0,"delta":{"content":"first"},"finish_reason":null}]}\n\n'
    )
    second_frame = (
        b'data: {"choices":[{"index":0,"delta":{"content":"second"},"finish_reason":"stop"}]}\n\n'
    )
    stream = _ChunkedAsyncStream([first_frame, second_frame, b"data: [DONE]\n\n"])

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=stream,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        adapter = OpenAICompatibleModelProvider(
            http_client=http_client,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-stream-secret"),
        )
        agen = adapter.stream_generate(
            ModelRequest(
                model_id="gpt-4o-mini",
                messages=[ModelMessage(role="user", content="Hi")],
            )
        )
        first_chunk = await agen.__anext__()
        assert first_chunk.text == "first"
        await agen.aclose()

    assert stream.closed is True
