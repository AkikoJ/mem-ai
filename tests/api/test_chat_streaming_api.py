"""HTTP & ASGI API tests for SSE streaming chat, heartbeats, cancellation, and recovery."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import select

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import BadGatewayError
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import FakeMemoryProvider, parse_sse_text


def _with_idem(headers: dict[str, str], key: str | None = None) -> dict[str, str]:
    return {**headers, "Idempotency-Key": key or f"idem-{uuid4().hex}"}


async def _create_and_publish_agent(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    name: str = "Stream Agent",
    system_prompt: str = "PINNED_STREAM_PERSONA",
    default_model_id: str = "gpt-4o-mini",
) -> dict[str, Any]:
    created = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": name,
            "systemPrompt": system_prompt,
            "defaultModelId": default_model_id,
        },
    )
    assert created.status_code == 201
    agent_id = created.json()["data"]["agentId"]
    published = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert published.status_code == 200
    return dict(published.json()["data"])


class _ControllableStreamModelProvider:
    """Model provider double supporting scripted delays, errors, gates, and cancel tracking."""

    def __init__(self) -> None:
        self.sync_calls: list[ModelRequest] = []
        self.stream_calls: list[ModelRequest] = []
        self.pre_first_delay: float = 0.0
        self.between_delta_delay: float = 0.0
        self.chunks: list[str] = ["Hello ", "from ", "SSE!"]
        self.fail_after_deltas: int | None = None
        self.stall_after_deltas: int | None = None
        self.gate_after_first_delta: asyncio.Event | None = None
        self.first_delta_emitted: asyncio.Event | None = None
        self.sync_started: asyncio.Event | None = None
        self.sync_release: asyncio.Event | None = None
        self.sync_finished: asyncio.Event | None = None
        self.upstream_closed = asyncio.Event()

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.sync_calls.append(request)
        if self.sync_started is not None:
            self.sync_started.set()
        if self.sync_release is not None:
            await self.sync_release.wait()
        try:
            return ModelResponse(
                model_id=request.model_id,
                content=f"sync:{request.messages[-1].content}",
                usage=ModelUsage(input_tokens=9, output_tokens=4, total_tokens=13),
                finish_reason="stop",
            )
        finally:
            if self.sync_finished is not None:
                self.sync_finished.set()

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        self.stream_calls.append(request)
        self.upstream_closed.clear()
        try:
            if self.pre_first_delay > 0:
                await asyncio.sleep(self.pre_first_delay)
            for idx, text in enumerate(self.chunks, start=1):
                if idx > 1 and self.between_delta_delay > 0:
                    await asyncio.sleep(self.between_delta_delay)
                yield ModelStreamChunk(sequence=idx, text=text, is_final=False)
                if idx == 1 and self.first_delta_emitted is not None:
                    self.first_delta_emitted.set()
                if idx == 1 and self.gate_after_first_delta is not None:
                    await self.gate_after_first_delta.wait()
                if self.fail_after_deltas is not None and idx == self.fail_after_deltas:
                    raise BadGatewayError(
                        code="model_upstream_error",
                        message="model provider returned an error response",
                    )
                if self.stall_after_deltas is not None and idx == self.stall_after_deltas:
                    await asyncio.sleep(60.0)
            yield ModelStreamChunk(
                sequence=len(self.chunks) + 1,
                text="",
                is_final=True,
                usage=ModelUsage(input_tokens=14, output_tokens=7, total_tokens=21),
                finish_reason="stop",
            )
        finally:
            self.upstream_closed.set()


async def _run_live_asgi_post(
    app: FastAPI,
    *,
    path: str,
    headers: dict[str, str],
    body: dict[str, Any],
    disconnect_event: asyncio.Event,
    on_chunk: Any | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Execute an ASGI HTTP POST with real-time chunk delivery and controlled http.disconnect."""
    raw_body = json.dumps(body).encode("utf-8")
    raw_headers = [
        (b"host", b"test"),
        (b"content-type", b"application/json"),
        (b"content-length", str(len(raw_body)).encode("ascii")),
        *[(k.lower().encode("ascii"), v.encode("ascii")) for k, v in headers.items()],
    ]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": raw_headers,
        "server": ("test", 80),
        "client": ("127.0.0.1", 12345),
        "root_path": "",
    }
    body_sent = False
    status_code = 500
    response_headers: dict[str, str] = {}
    collected = bytearray()

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": raw_body, "more_body": False}
        await disconnect_event.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            response_headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in message.get("headers", [])
            }
        elif message["type"] == "http.response.body":
            chunk = bytes(message.get("body", b""))
            if chunk:
                collected.extend(chunk)
                if on_chunk is not None:
                    on_chunk(chunk.decode("utf-8"))

    await app(scope, receive, send)
    return status_code, response_headers, bytes(collected)


@pytest.mark.integration
@pytest.mark.anyio
async def test_sse_chat_happy_path_headers_heartbeats_memory_and_idempotent_replay(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableStreamModelProvider()
    provider.pre_first_delay = 0.08
    provider.between_delta_delay = 0.06
    memory_provider = FakeMemoryProvider()

    settings = migrated_pg_settings.model_copy(
        update={
            "chat_stream_heartbeat_interval_seconds": 0.03,
            "chat_stream_timeout_seconds": 10.0,
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=provider,
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="GPT-4o Mini",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent = await _create_and_publish_agent(client, headers=alice_headers)
            mem_resp = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "User prefers concise Python answers"},
            )
            assert mem_resp.status_code == 201

            idem_key = f"sse-happy-{uuid4().hex}"
            stream_resp = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, idem_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "Write a greeting",
                    "stream": True,
                },
            )

            assert stream_resp.status_code == 200
            assert stream_resp.headers["content-type"].startswith("text/event-stream")
            assert stream_resp.headers["cache-control"] == "no-cache, no-transform"
            assert stream_resp.headers["x-accel-buffering"] == "no"
            assert "x-request-id" in stream_resp.headers

            events = parse_sse_text(stream_resp.text)
            assert events[0].event == "conversation.started"
            assert events[0].event_id == "1"
            conv_id = events[0].data["conversation_id"]
            gen_id = events[0].data["generation_id"]
            user_msg_id = events[0].data["message_id"]

            heartbeats = [e for e in events if e.event == "heartbeat"]
            assert len(heartbeats) >= 1
            assert all(hb.event_id is None and hb.data == {} for hb in heartbeats)

            business = [e for e in events if e.event != "heartbeat"]
            assert [int(e.event_id or "0") for e in business] == list(range(1, len(business) + 1))

            deltas = [e for e in business if e.event == "message.delta"]
            assert [d.data["sequence"] for d in deltas] == [1, 2, 3]
            assert "".join(d.data["text"] for d in deltas) == "Hello from SSE!"
            assert all(d.data["generation_id"] == gen_id for d in deltas)

            completed = business[-1]
            assert completed.event == "conversation.completed"
            assert completed.data["generation_id"] == gen_id
            assert completed.data["message_id"] != user_msg_id
            assert completed.data["usage"] == {
                "input_tokens": 14,
                "output_tokens": 7,
                "total_tokens": 21,
            }

            # Verify recalled memory & pinned persona were injected into the streaming prompt.
            assert len(provider.stream_calls) == 1
            prompt_blob = "\n".join(m.content for m in provider.stream_calls[0].messages)
            assert "PINNED_STREAM_PERSONA" in prompt_blob
            assert "User prefers concise Python answers" in prompt_blob

            # Verify atomic turn memory write job was enqueued for the committed streaming turn.
            async with app.state.session_factory() as session:
                jobs = (
                    (await session.execute(select(Job).where(Job.business_id == gen_id)))
                    .scalars()
                    .all()
                )
            assert len(jobs) == 1
            assert jobs[0].job_type == "memory_turn_write"

            # Query generation detail returns committed assistant message & usage.
            status_resp = await client.get(
                f"/v1/conversations/{conv_id}/generations/{gen_id}",
                headers=alice_headers,
            )
            assert status_resp.status_code == 200
            status_data = status_resp.json()["data"]
            assert status_data["status"] == "succeeded"
            assert status_data["userMessageId"] == user_msg_id
            assert status_data["messageId"] == completed.data["message_id"]
            assert status_data["message"]["content"] == "Hello from SSE!"

            # Retrying the same key (stream=True or stream=False) returns 200 application/json
            # and reuses the original generation without calling the model again.
            replay_stream = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, idem_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "Write a greeting",
                    "stream": True,
                },
            )
            replay_sync = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, idem_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "Write a greeting",
                    "stream": False,
                },
            )
            assert replay_stream.status_code == 200
            assert replay_stream.headers["content-type"].startswith("application/json")
            assert replay_sync.status_code == 200
            assert replay_stream.json()["data"] == replay_sync.json()["data"]
            assert replay_stream.json()["data"]["generationId"] == gen_id
            assert replay_stream.json()["data"]["message"]["content"] == "Hello from SSE!"
            assert len(provider.stream_calls) == 1
            assert len(provider.sync_calls) == 0


@pytest.mark.integration
@pytest.mark.anyio
async def test_sse_chat_pre_stream_errors_running_202_and_retired_agent_rules(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableStreamModelProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(model_provider=provider),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}
    bob_headers = {"Authorization": "Bearer token-bob"}

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="GPT-4o Mini",
                    enabled=True,
                    supports_streaming=True,
                ),
                ModelConfigSeed(
                    model_id="sync-only-model",
                    provider_id="openai_compatible",
                    upstream_model="sync-only-model",
                    display_name="Sync Only",
                    enabled=True,
                    supports_streaming=False,
                ),
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            stream_agent = await _create_and_publish_agent(
                client, headers=alice_headers, name="Streamable"
            )
            sync_only_agent = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="SyncOnly",
                default_model_id="sync-only-model",
            )

            # 1. Missing Idempotency-Key -> 422 JSON envelope before stream starts.
            missing_key = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={"agentId": stream_agent["agentId"], "message": "hi", "stream": True},
            )
            assert missing_key.status_code == 422
            assert missing_key.headers["content-type"].startswith("application/json")
            assert missing_key.json()["code"] == "idempotency_key_required"

            # 2. Non-streaming model -> 422 model_capability_unsupported JSON envelope.
            unsupported = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={"agentId": sync_only_agent["agentId"], "message": "hi", "stream": True},
            )
            assert unsupported.status_code == 422
            assert unsupported.headers["content-type"].startswith("application/json")
            assert unsupported.json()["code"] == "model_capability_unsupported"
            assert len(provider.stream_calls) == 0

            # 3. Start a live stream paused after delta 1 to test running 202 & 409 lease conflict.
            provider.first_delta_emitted = asyncio.Event()
            provider.gate_after_first_delta = asyncio.Event()
            disconnect_ev = asyncio.Event()
            running_key = f"stream-running-{uuid4().hex}"
            live_chunks: list[str] = []

            live_task = asyncio.create_task(
                _run_live_asgi_post(
                    app,
                    path="/v1/chat",
                    headers=_with_idem(alice_headers, running_key),
                    body={
                        "agentId": stream_agent["agentId"],
                        "message": "Hold this stream open",
                        "stream": True,
                    },
                    disconnect_event=disconnect_ev,
                    on_chunk=live_chunks.append,
                )
            )
            await asyncio.wait_for(provider.first_delta_emitted.wait(), timeout=3.0)
            await asyncio.sleep(0.02)
            started_events = parse_sse_text("".join(live_chunks))
            conv_id = started_events[0].data["conversation_id"]
            gen_id = started_events[0].data["generation_id"]

            # Same-key retry while streaming is running -> 202 JSON with Location & statusUrl!
            retry_running = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, running_key),
                json={
                    "agentId": stream_agent["agentId"],
                    "message": "Hold this stream open",
                    "stream": True,
                },
            )
            assert retry_running.status_code == 202
            assert retry_running.headers["content-type"].startswith("application/json")
            expected_url = f"/v1/conversations/{conv_id}/generations/{gen_id}"
            assert retry_running.headers["location"] == expected_url
            assert retry_running.json()["data"]["status"] == "running"
            assert retry_running.json()["data"]["generationId"] == gen_id

            # Different-key overlapping stream on same conversation -> 409 JSON conflict.
            overlap = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": stream_agent["agentId"],
                    "conversationId": conv_id,
                    "message": "Overlap while running",
                    "stream": True,
                },
            )
            assert overlap.status_code == 409
            assert overlap.headers["content-type"].startswith("application/json")
            assert overlap.json()["code"] == "conversation_generation_conflict"

            # Same-key different payload -> 409 idempotency_key_conflict JSON envelope.
            digest_conflict = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, running_key),
                json={
                    "agentId": stream_agent["agentId"],
                    "message": "Mutated body",
                    "stream": True,
                },
            )
            assert digest_conflict.status_code == 409
            assert digest_conflict.json()["code"] == "idempotency_key_conflict"

            # Release stream to finish cleanly.
            provider.gate_after_first_delta.set()
            status_code, _, raw_bytes = await asyncio.wait_for(live_task, timeout=3.0)
            assert status_code == 200
            final_events = parse_sse_text(raw_bytes.decode("utf-8"))
            assert final_events[-1].event == "conversation.completed"
            assert len(provider.stream_calls) == 1

            # 4. Cross-user continuation returns 404 JSON envelope.
            bob_cont = await client.post(
                "/v1/chat",
                headers=_with_idem(bob_headers),
                json={
                    "agentId": stream_agent["agentId"],
                    "conversationId": conv_id,
                    "message": "Cross-user attempt",
                    "stream": True,
                },
            )
            assert bob_cont.status_code == 404
            assert bob_cont.headers["content-type"].startswith("application/json")

            # 5. Retiring the Agent blocks new streaming conversations (404 JSON)
            #    while existing conversations can still continue streaming with pinned persona.
            provider.first_delta_emitted = None
            provider.gate_after_first_delta = None
            retire_resp = await client.delete(
                f"/v1/agents/{stream_agent['agentId']}", headers=alice_headers
            )
            assert retire_resp.status_code == 200

            new_conv_after_retire = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": stream_agent["agentId"],
                    "message": "New conv after retire",
                    "stream": True,
                },
            )
            assert new_conv_after_retire.status_code == 404
            assert new_conv_after_retire.json()["code"] == "agent_persona_not_found"

            cont_after_retire = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": stream_agent["agentId"],
                    "conversationId": conv_id,
                    "message": "Continue after retire",
                    "stream": True,
                },
            )
            assert cont_after_retire.status_code == 200
            assert cont_after_retire.headers["content-type"].startswith("text/event-stream")
            cont_events = parse_sse_text(cont_after_retire.text)
            assert cont_events[-1].event == "conversation.completed"


@pytest.mark.integration
@pytest.mark.anyio
async def test_sse_chat_partial_failure_and_timeout_emit_failed_event_and_filter_history(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableStreamModelProvider()
    memory_provider = FakeMemoryProvider()
    settings = migrated_pg_settings.model_copy(
        update={
            "chat_stream_timeout_seconds": 0.15,
            "chat_stream_heartbeat_interval_seconds": 0.05,
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=provider,
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="GPT-4o Mini",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent = await _create_and_publish_agent(client, headers=alice_headers)

            # 1. Partial failure after 2 deltas -> conversation.failed (no partial save).
            provider.fail_after_deltas = 2
            fail_key = f"sse-partial-fail-{uuid4().hex}"
            fail_resp = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, fail_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "This turn fails mid-stream",
                    "stream": True,
                },
            )
            assert fail_resp.status_code == 200
            fail_events = parse_sse_text(fail_resp.text)
            business_fail = [e for e in fail_events if e.event != "heartbeat"]
            assert [e.event for e in business_fail] == [
                "conversation.started",
                "message.delta",
                "message.delta",
                "conversation.failed",
            ]
            conv_id = business_fail[0].data["conversation_id"]
            fail_gen_id = business_fail[0].data["generation_id"]
            assert business_fail[-1].data["code"] == "model_upstream_error"
            assert business_fail[-1].data["partial_output_persisted"] is False

            # Query generation -> failed, no assistant message.
            fail_query = await client.get(
                f"/v1/conversations/{conv_id}/generations/{fail_gen_id}",
                headers=alice_headers,
            )
            assert fail_query.status_code == 200
            assert fail_query.json()["data"]["status"] == "failed"
            assert fail_query.json()["data"]["error"]["code"] == "model_upstream_error"
            assert fail_query.json()["data"]["message"] is None

            # Same-key retry replays saved 502 error JSON without re-invoking model.
            fail_replay = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, fail_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "This turn fails mid-stream",
                    "stream": True,
                },
            )
            assert fail_replay.status_code == 502
            assert fail_replay.json()["code"] == "model_upstream_error"
            assert len(provider.stream_calls) == 1

            # 2. Stream deadline timeout -> closes upstream & emits generation_timeout.
            provider.fail_after_deltas = None
            provider.stall_after_deltas = 1
            timeout_key = f"sse-timeout-{uuid4().hex}"
            timeout_resp = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, timeout_key),
                json={
                    "agentId": agent["agentId"],
                    "conversationId": conv_id,
                    "message": "This turn stalls and times out",
                    "stream": True,
                },
            )
            assert timeout_resp.status_code == 200
            timeout_events = parse_sse_text(timeout_resp.text)
            business_timeout = [e for e in timeout_events if e.event != "heartbeat"]
            assert business_timeout[0].event == "conversation.started"
            assert business_timeout[1].event == "message.delta"
            assert business_timeout[-1].event == "conversation.failed"
            assert business_timeout[-1].data["code"] == "generation_timeout"
            assert business_timeout[-1].data["partial_output_persisted"] is False
            assert provider.upstream_closed.is_set()

            timeout_gen_id = business_timeout[0].data["generation_id"]
            timeout_query = await client.get(
                f"/v1/conversations/{conv_id}/generations/{timeout_gen_id}",
                headers=alice_headers,
            )
            assert timeout_query.status_code == 200
            assert timeout_query.json()["data"]["status"] == "failed"
            assert timeout_query.json()["data"]["error"]["code"] == "generation_timeout"

            # Same-key retry returns 504 generation_timeout JSON.
            timeout_replay = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, timeout_key),
                json={
                    "agentId": agent["agentId"],
                    "conversationId": conv_id,
                    "message": "This turn stalls and times out",
                    "stream": True,
                },
            )
            assert timeout_replay.status_code == 504
            assert timeout_replay.json()["code"] == "generation_timeout"

            # 3. Fresh key succeeds and filters out both earlier failed user turns from prompt.
            provider.stall_after_deltas = None
            ok_resp = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": agent["agentId"],
                    "conversationId": conv_id,
                    "message": "Clean follow-up turn",
                    "stream": True,
                },
            )
            assert ok_resp.status_code == 200
            ok_events = parse_sse_text(ok_resp.text)
            assert ok_events[-1].event == "conversation.completed"

            last_prompt = "\n".join(m.content for m in provider.stream_calls[-1].messages)
            assert "This turn fails mid-stream" not in last_prompt
            assert "This turn stalls and times out" not in last_prompt
            assert "Clean follow-up turn" in last_prompt

            # No memory write jobs exist for the two failed turns; only the succeeded turn has one.
            async with app.state.session_factory() as session:
                jobs = (
                    (await session.execute(select(Job).where(Job.job_type == "memory_turn_write")))
                    .scalars()
                    .all()
                )
            assert len(jobs) == 1
            assert str(jobs[0].business_id) == ok_events[0].data["generation_id"]


@pytest.mark.integration
@pytest.mark.anyio
async def test_sse_disconnect_cancels_upstream_and_generation_unlike_sync_disconnect(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableStreamModelProvider()
    memory_provider = FakeMemoryProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=provider,
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="GPT-4o Mini",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent = await _create_and_publish_agent(client, headers=alice_headers)

            # 1. Streaming disconnect after delta 1: cancels upstream & marks cancelled.
            provider.first_delta_emitted = asyncio.Event()
            provider.gate_after_first_delta = asyncio.Event()
            stream_disconnect = asyncio.Event()
            stream_key = f"sse-disc-{uuid4().hex}"
            sent_chunks: list[str] = []

            def _on_chunk(chunk: str) -> None:
                sent_chunks.append(chunk)
                if "message.delta" in "".join(sent_chunks):
                    stream_disconnect.set()

            status_code, resp_headers, raw = await asyncio.wait_for(
                _run_live_asgi_post(
                    app,
                    path="/v1/chat",
                    headers=_with_idem(alice_headers, stream_key),
                    body={
                        "agentId": agent["agentId"],
                        "message": "Cancelled streaming turn",
                        "stream": True,
                    },
                    disconnect_event=stream_disconnect,
                    on_chunk=_on_chunk,
                ),
                timeout=3.0,
            )
            assert status_code == 200
            assert resp_headers["content-type"].startswith("text/event-stream")
            assert provider.upstream_closed.is_set()

            received_events = parse_sse_text(raw.decode("utf-8"))
            assert received_events[0].event == "conversation.started"
            conv_id = received_events[0].data["conversation_id"]
            gen_id = received_events[0].data["generation_id"]

            # Query generation -> cancelled, no assistant message, no turn memory job.
            cancel_query = await client.get(
                f"/v1/conversations/{conv_id}/generations/{gen_id}",
                headers=alice_headers,
            )
            assert cancel_query.status_code == 200
            cancel_data = cancel_query.json()["data"]
            assert cancel_data["status"] == "cancelled"
            assert cancel_data["error"]["code"] == "generation_cancelled"
            assert cancel_data["message"] is None

            async with app.state.session_factory() as session:
                cancelled_jobs = (
                    (await session.execute(select(Job).where(Job.business_id == gen_id)))
                    .scalars()
                    .all()
                )
            assert cancelled_jobs == []

            # Retrying the cancelled key returns 409 generation_cancelled JSON.
            cancel_replay = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, stream_key),
                json={
                    "agentId": agent["agentId"],
                    "message": "Cancelled streaming turn",
                    "stream": True,
                },
            )
            assert cancel_replay.status_code == 409
            assert cancel_replay.json()["code"] == "generation_cancelled"

            # 2. Contrast with sync disconnect: shielded sync call completes and is recoverable!
            provider.sync_started = asyncio.Event()
            provider.sync_release = asyncio.Event()
            provider.sync_finished = asyncio.Event()
            sync_key = f"sync-disc-{uuid4().hex}"
            sync_disconnect = asyncio.Event()

            sync_task = asyncio.create_task(
                _run_live_asgi_post(
                    app,
                    path="/v1/chat",
                    headers=_with_idem(alice_headers, sync_key),
                    body={
                        "agentId": agent["agentId"],
                        "conversationId": conv_id,
                        "message": "Sync turn survives disconnect",
                        "stream": False,
                    },
                    disconnect_event=sync_disconnect,
                )
            )
            await asyncio.wait_for(provider.sync_started.wait(), timeout=3.0)
            sync_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await sync_task

            provider.sync_release.set()
            await asyncio.wait_for(provider.sync_finished.wait(), timeout=3.0)
            await app.state.sync_generation_tasks.drain(timeout_seconds=3.0)

            sync_replay = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers, sync_key),
                json={
                    "agentId": agent["agentId"],
                    "conversationId": conv_id,
                    "message": "Sync turn survives disconnect",
                    "stream": False,
                },
            )
            assert sync_replay.status_code == 200
            assert sync_replay.json()["data"]["message"]["content"] == (
                "sync:Sync turn survives disconnect"
            )
            # And the sync prompt excluded the earlier cancelled streaming turn!
            sync_prompt = "\n".join(m.content for m in provider.sync_calls[-1].messages)
            assert "Cancelled streaming turn" not in sync_prompt


@pytest.mark.integration
@pytest.mark.anyio
async def test_sse_chat_end_to_end_with_openai_compatible_adapter(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    upstream_sse = b"".join(
        [
            b'data: {"choices":[{"index":0,"delta":{"content":"Real "},"finish_reason":null}]}\n\n',
            b'data: {"choices":[{"index":0,"delta":{"content":"adapter!"},'
            b'"finish_reason":"stop"}]}\n\n',
            b'data: {"choices":[],"usage":{"prompt_tokens":6,"completion_tokens":2,'
            b'"total_tokens":8}}\n\n',
            b"data: [DONE]\n\n",
        ]
    )

    def upstream_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=upstream_sse,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream_handler)) as upstream_http:
        adapter = OpenAICompatibleModelProvider(
            http_client=upstream_http,
            base_url="https://llm.example.test/v1",
            api_key=SecretStr("sk-server-sse-key"),
        )
        app = create_app(
            migrated_pg_settings,
            providers=ProviderRegistry(model_provider=adapter),
            credential_verifier=test_auth_adapter,
        )
        alice_headers = {"Authorization": "Bearer token-alice"}

        async with app.router.lifespan_context(app):
            await models_service.sync_model_configs(
                app.state.session_factory,
                seeds=[
                    ModelConfigSeed(
                        model_id="gpt-4o-mini",
                        provider_id="openai_compatible",
                        upstream_model="gpt-4o-mini",
                        display_name="GPT-4o Mini",
                        enabled=True,
                        supports_streaming=True,
                    )
                ],
            )
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                agent = await _create_and_publish_agent(client, headers=alice_headers)
                resp = await client.post(
                    "/v1/chat",
                    headers=_with_idem(alice_headers),
                    json={
                        "agentId": agent["agentId"],
                        "message": "Ping",
                        "stream": True,
                    },
                )
                assert resp.status_code == 200
                events = parse_sse_text(resp.text)
                assert [e.event for e in events] == [
                    "conversation.started",
                    "message.delta",
                    "message.delta",
                    "conversation.completed",
                ]
                assert (
                    "".join(e.data["text"] for e in events if e.event == "message.delta")
                    == "Real adapter!"
                )
                assert events[-1].data["usage"] == {
                    "input_tokens": 6,
                    "output_tokens": 2,
                    "total_tokens": 8,
                }
