"""API tests for `POST /v1/chat`, disconnect shielding, OpenAI-compatible flow, and isolation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

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
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import BuiltinUsers, FakeMemoryProvider


class _ScriptedModelProvider:
    """Controlled ModelProvider for API tests verifying exact calls, prompts, and failures."""

    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []
        self.next_error: Exception | None = None
        self.reply_prefix = "reply:"

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if self.next_error is not None:
            exc = self.next_error
            self.next_error = None
            raise exc
        last_user = request.messages[-1].content
        return ModelResponse(
            model_id=request.model_id,
            content=f"{self.reply_prefix}{last_user}",
            usage=ModelUsage(input_tokens=12, output_tokens=6, total_tokens=18),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


class _GateModelProvider:
    """ModelProvider that pauses inside `generate` to test client disconnect shielding."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.started.set()
        await self.release.wait()
        try:
            return ModelResponse(
                model_id=request.model_id,
                content="completed after disconnect",
                usage=ModelUsage(input_tokens=9, output_tokens=4, total_tokens=13),
                finish_reason="stop",
            )
        finally:
            self.finished.set()

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


def _with_idem(headers: dict[str, str], key: str | None = None) -> dict[str, str]:
    return {**headers, "Idempotency-Key": key if key is not None else f"chat-{uuid4().hex}"}


async def _create_and_publish_agent(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    name: str,
    system_prompt: str,
    default_model_id: str | None = "gpt-4o-mini",
    publish: bool = True,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": name,
        "systemPrompt": system_prompt,
        "behaviorRules": ["Answer directly"],
    }
    if default_model_id is not None:
        payload["defaultModelId"] = default_model_id

    create_resp = await client.post("/v1/agents", headers=headers, json=payload)
    assert create_resp.status_code == 201
    agent_data: dict[str, object] = create_resp.json()["data"]
    if not publish:
        return agent_data

    pub_resp = await client.post(
        f"/v1/agents/{agent_data['agentId']}/publish",
        headers=headers,
    )
    assert pub_resp.status_code == 200
    published: dict[str, object] = pub_resp.json()["data"]
    return published


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_start_continue_pinned_persona_rejections_and_isolation(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
    fake_memory_provider: FakeMemoryProvider,
) -> None:
    model_provider = _ScriptedModelProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=fake_memory_provider,
        ),
    )
    alice_headers = builtin_users.alice.headers
    bob_headers = builtin_users.bob.headers

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
                ),
                ModelConfigSeed(
                    model_id="disabled-model",
                    provider_id="openai_compatible",
                    upstream_model="disabled-model",
                    display_name="Disabled",
                    enabled=False,
                ),
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated chat is rejected with 401.
            unauth = await client.post(
                "/v1/chat",
                json={"agentId": str(uuid4()), "message": "hi"},
            )
            assert unauth.status_code == 401

            alice_me = await client.get("/v1/users/me", headers=alice_headers)
            alice_user_id = UUID(alice_me.json()["data"]["userId"])

            # 2. Unpublished Agent, unavailable default model, extra fields, stream=true rejected
            # BEFORE any external model call.
            unpublished = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="Draft Only",
                system_prompt="Unpublished prompt",
                publish=False,
            )
            res_unpub = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={"agentId": unpublished["agentId"], "message": "hello"},
            )
            assert res_unpub.status_code == 404
            assert res_unpub.json()["code"] == "agent_persona_not_found"

            disabled_model_agent = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="Disabled Default Model",
                system_prompt="Prompt",
                default_model_id="disabled-model",
            )
            res_disabled = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={"agentId": disabled_model_agent["agentId"], "message": "hello"},
            )
            assert res_disabled.status_code == 409
            assert res_disabled.json()["code"] == "model_unavailable"

            res_extra = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": disabled_model_agent["agentId"],
                    "message": "hello",
                    "apiKey": "sk-forged",
                },
            )
            assert res_extra.status_code == 422

            res_stream = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": disabled_model_agent["agentId"],
                    "message": "hello",
                    "stream": True,
                },
            )
            assert res_stream.status_code == 409
            assert res_stream.json()["code"] == "model_unavailable"
            assert len(model_provider.requests) == 0

            # 3. Start a new conversation (omitting conversationId): pins v1 persona & returns IDs.
            alice_agent = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="Alice Writer",
                system_prompt="PINNED_V1_INSTRUCTIONS",
                default_model_id="gpt-4o-mini",
            )
            alice_other_agent = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="Alice Reviewer",
                system_prompt="OTHER_AGENT_INSTRUCTIONS",
                default_model_id="gpt-4o-mini",
            )

            turn1 = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": alice_agent["agentId"],
                    "message": "First message",
                },
            )
            assert turn1.status_code == 200
            body1 = turn1.json()
            assert body1["success"] is True
            assert body1["code"] == "ok"
            data1 = body1["data"]
            conv_id = UUID(data1["conversationId"])
            gen1_id = UUID(data1["generationId"])
            msg1_id = UUID(data1["messageId"])
            assert data1["modelId"] == "gpt-4o-mini"
            assert data1["message"]["messageId"] == str(msg1_id)
            assert data1["message"]["role"] == "assistant"
            assert data1["message"]["content"] == "reply:First message"
            assert data1["message"]["sequence"] == 2
            assert data1["usage"] == {
                "inputTokens": 12,
                "outputTokens": 6,
                "totalTokens": 18,
            }
            assert "PINNED_V1_INSTRUCTIONS" in model_provider.requests[-1].messages[0].content
            assert gen1_id is not None

            # 4. Update Agent draft and publish v2 -> existing conversation MUST keep v1 persona!
            patch_resp = await client.patch(
                f"/v1/agents/{alice_agent['agentId']}",
                headers=alice_headers,
                json={"systemPrompt": "NEW_V2_INSTRUCTIONS"},
            )
            assert patch_resp.status_code == 200
            pub_v2 = await client.post(
                f"/v1/agents/{alice_agent['agentId']}/publish",
                headers=alice_headers,
            )
            assert pub_v2.status_code == 200

            # 5. Cross-user Agent, cross-user conversation, and Agent/conversation mismatch
            # are all rejected before calling ModelProvider.
            calls_before_reject = len(model_provider.requests)
            bob_use_alice_agent = await client.post(
                "/v1/chat",
                headers=_with_idem(bob_headers),
                json={"agentId": alice_agent["agentId"], "message": "steal"},
            )
            assert bob_use_alice_agent.status_code == 404

            bob_use_alice_conv = await client.post(
                "/v1/chat",
                headers=_with_idem(bob_headers),
                json={
                    "agentId": alice_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "steal conv",
                },
            )
            assert bob_use_alice_conv.status_code == 404

            mismatch = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": alice_other_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "wrong agent",
                },
            )
            assert mismatch.status_code == 409
            assert mismatch.json()["code"] == "conversation_agent_mismatch"
            assert len(model_provider.requests) == calls_before_reject

            # 6. Model failure retains user message, returns 502, and is filtered from next prompt.
            model_provider.next_error = BadGatewayError(
                code="model_upstream_error",
                message="upstream failed",
            )
            failed_turn = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": alice_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "This turn will fail",
                },
            )
            assert failed_turn.status_code == 502
            assert failed_turn.json()["code"] == "model_upstream_error"

            # 7. Continue conversation: keeps pinned v1 persona & excludes failed turn from prompt.
            turn3 = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={
                    "agentId": alice_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "Third message",
                },
            )
            assert turn3.status_code == 200
            last_request = model_provider.requests[-1]
            assert "PINNED_V1_INSTRUCTIONS" in last_request.messages[0].content
            assert "NEW_V2_INSTRUCTIONS" not in last_request.messages[0].content
            assert [(m.role, m.content) for m in last_request.messages[1:]] == [
                ("user", "First message"),
                ("assistant", "reply:First message"),
                ("user", "Third message"),
            ]

            # Confirm failed user message was still preserved in DB history.
            persisted = await generation_store.list_conversation_messages_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_id,
            )
            assert [(m.role, m.content) for m in persisted] == [
                ("user", "First message"),
                ("assistant", "reply:First message"),
                ("user", "This turn will fail"),
                ("user", "Third message"),
                ("assistant", "reply:Third message"),
            ]

            # 8. Real ConversationAccessChecker wired into `/v1/memories`:
            # Alice can create conversation-scoped memory for her conversation; Bob gets 404.
            mem_ok = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={
                    "content": "Conversation specific note",
                    "scope": "conversation",
                    "conversationId": str(conv_id),
                },
            )
            assert mem_ok.status_code == 201

            mem_bob = await client.post(
                "/v1/memories",
                headers=bob_headers,
                json={
                    "content": "Foreign injection attempt",
                    "scope": "conversation",
                    "conversationId": str(conv_id),
                },
            )
            assert mem_bob.status_code == 404


@pytest.mark.integration
@pytest.mark.anyio
async def test_sync_chat_disconnect_does_not_abort_in_flight_model_commit(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    gate_provider = _GateModelProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(model_provider=gate_provider),
    )
    alice_headers = builtin_users.alice.headers

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
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_me = await client.get("/v1/users/me", headers=alice_headers)
            alice_user_id = UUID(alice_me.json()["data"]["userId"])
            agent = await _create_and_publish_agent(
                client,
                headers=alice_headers,
                name="Shielded Agent",
                system_prompt="Keep going even if client disconnects.",
            )

            # Start an initial turn so we know the conversationId deterministically.
            gate_provider.release.set()
            seed_resp = await client.post(
                "/v1/chat",
                headers=_with_idem(alice_headers),
                json={"agentId": agent["agentId"], "message": "seed"},
            )
            assert seed_resp.status_code == 200
            conv_id = UUID(seed_resp.json()["data"]["conversationId"])

            # Reset gate and launch Turn 2, then cancel the HTTP request task mid-model-call!
            gate_provider.started.clear()
            gate_provider.release.clear()
            gate_provider.finished.clear()

            http_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers=_with_idem(alice_headers),
                    json={
                        "agentId": agent["agentId"],
                        "conversationId": str(conv_id),
                        "message": "client will disconnect",
                    },
                )
            )
            await asyncio.wait_for(gate_provider.started.wait(), timeout=3.0)
            http_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await http_task

            # Release upstream model and drain background sync tasks: commit MUST still succeed!
            gate_provider.release.set()
            await asyncio.wait_for(gate_provider.finished.wait(), timeout=3.0)
            await app.state.sync_generation_tasks.drain(timeout_seconds=3.0)

            messages = await generation_store.list_conversation_messages_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_id,
            )
            assert [(m.role, m.content) for m in messages[-2:]] == [
                ("user", "client will disconnect"),
                ("assistant", "completed after disconnect"),
            ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_end_to_end_with_openai_compatible_adapter_and_startup_model_sync(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    def upstream_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        if body["messages"][-1]["content"] == "trigger-timeout":
            raise httpx.ReadTimeout("upstream slow", request=request)
        prompt_last = body["messages"][-1]["content"]
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-e2e",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": f"upstream:{body['model']}:{prompt_last}",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 21,
                    "completion_tokens": 9,
                    "total_tokens": 30,
                },
            },
        )

    configured_settings = migrated_pg_settings.model_copy(
        update={
            "model_default_id": "registered-default",
            "model_registry_entries": [
                {
                    "model_id": "registered-default",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-gpt-4o-mini",
                    "display_name": "Registered Default",
                    "enabled": True,
                }
            ],
        }
    )

    mock_http = httpx.AsyncClient(transport=httpx.MockTransport(upstream_handler))
    # Do NOT pass a static upstream_model_map: verify PostgreSQL `model_configs.upstream_model`
    # flows end-to-end into `ModelRequest.upstream_model`!
    adapter = OpenAICompatibleModelProvider(
        http_client=mock_http,
        base_url="https://llm.example.test/v1",
        api_key=SecretStr("sk-e2e-server-key"),
    )
    app = create_app(
        configured_settings,
        providers=ProviderRegistry(model_provider=adapter),
    )
    alice_headers = builtin_users.alice.headers

    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                # Agent omits defaultModelId -> falls back to server-configured model_default_id.
                agent = await _create_and_publish_agent(
                    client,
                    headers=alice_headers,
                    name="Fallback Model Agent",
                    system_prompt="Use fallback model.",
                    default_model_id=None,
                )

                ok_resp = await client.post(
                    "/v1/chat",
                    headers=_with_idem(alice_headers),
                    json={"agentId": agent["agentId"], "message": "hello openai"},
                )
                assert ok_resp.status_code == 200
                data = ok_resp.json()["data"]
                assert data["modelId"] == "registered-default"
                assert data["message"]["content"] == "upstream:vendor-gpt-4o-mini:hello openai"
                assert data["usage"] == {
                    "inputTokens": 21,
                    "outputTokens": 9,
                    "totalTokens": 30,
                }

                # Timeout from OpenAI-compatible adapter maps cleanly to 504 `generation_timeout`.
                timeout_resp = await client.post(
                    "/v1/chat",
                    headers=_with_idem(alice_headers),
                    json={
                        "agentId": agent["agentId"],
                        "conversationId": data["conversationId"],
                        "message": "trigger-timeout",
                    },
                )
                assert timeout_resp.status_code == 504
                assert timeout_resp.json()["code"] == "generation_timeout"
    finally:
        await mock_http.aclose()
