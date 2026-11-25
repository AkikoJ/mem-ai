"""End-to-end API tests for scoped memory recall, degradation, and MemOS integration in chat."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import FakeMemoryProvider


class _RecordingModelProvider:
    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        last_user = request.messages[-1].content
        return ModelResponse(
            model_id=request.model_id,
            content=f"echo:{last_user}",
            usage=ModelUsage(input_tokens=15, output_tokens=7, total_tokens=22),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


class _FailingMemoryProvider:
    provider_id = "flaky-memory"

    def __init__(self, *, exc: Exception) -> None:
        self.exc = exc
        self.calls: list[MemoryRecallQuery] = []

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        self.calls.append(query)
        raise self.exc

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        _ = item
        raise NotImplementedError

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        _ = item
        raise NotImplementedError

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        _ = query
        return MemoryListPage(items=[])

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        _ = (user_id, memory_id)
        raise NotImplementedError

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        _ = user_id
        raise NotImplementedError

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        _ = (user_id, external_ref)
        raise NotImplementedError


async def _create_published_agent(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    name: str,
    memory_enabled: bool = True,
    recall_limit: int = 5,
) -> dict[str, object]:
    create_resp = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": name,
            "systemPrompt": "Be helpful and respect user memory rules.",
            "defaultModelId": "gpt-4o-mini",
            "memoryPolicy": {
                "enabled": memory_enabled,
                "recallLimit": recall_limit,
                "writeEnabled": True,
                "writeScope": "user",
            },
        },
    )
    assert create_resp.status_code == 201
    agent_data: dict[str, object] = create_resp.json()["data"]
    pub_resp = await client.post(
        f"/v1/agents/{agent_data['agentId']}/publish",
        headers=headers,
    )
    assert pub_resp.status_code == 200
    published: dict[str, object] = pub_resp.json()["data"]
    return published


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_api_isolates_user_vs_conversation_memories_and_persists_injected_refs(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
    fake_memory_provider: FakeMemoryProvider,
) -> None:
    model_provider = _RecordingModelProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=fake_memory_provider,
        ),
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
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_user_id = UUID(
                (await client.get("/v1/users/me", headers=alice_headers)).json()["data"]["userId"]
            )
            alice_agent = await _create_published_agent(
                client,
                headers=alice_headers,
                name="Alice Assistant",
                recall_limit=5,
            )
            disabled_mem_agent = await _create_published_agent(
                client,
                headers=alice_headers,
                name="No Memory Agent",
                memory_enabled=False,
                recall_limit=5,
            )

            # Seed Alice's user-level memory and Bob's user-level memory via public `/v1/memories`.
            alice_user_mem = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Alice prefers concise SQLAlchemy examples", "scope": "user"},
            )
            assert alice_user_mem.status_code == 201
            alice_user_mem_id = alice_user_mem.json()["data"]["memoryId"]

            bob_user_mem = await client.post(
                "/v1/memories",
                headers=bob_headers,
                json={"content": "Bob top secret preference", "scope": "user"},
            )
            assert bob_user_mem.status_code == 201

            # Start Conv A and Conv B for Alice.
            conv_a_turn1 = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                json={"agentId": alice_agent["agentId"], "message": "Start Conv A"},
            )
            assert conv_a_turn1.status_code == 200
            conv_a_id = UUID(conv_a_turn1.json()["data"]["conversationId"])

            conv_b_turn1 = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                json={"agentId": alice_agent["agentId"], "message": "Start Conv B"},
            )
            assert conv_b_turn1.status_code == 200
            conv_b_id = UUID(conv_b_turn1.json()["data"]["conversationId"])

            # Add a conversation-scoped memory strictly bound to Conv A.
            conv_a_mem = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={
                    "content": "Conv A targets PostgreSQL 17 partitioning",
                    "scope": "conversation",
                    "conversationId": str(conv_a_id),
                },
            )
            assert conv_a_mem.status_code == 201
            conv_a_mem_id = conv_a_mem.json()["data"]["memoryId"]

            # Continue Conv A -> sees BOTH Alice's user memory AND Conv A memory, NEVER Bob's.
            conv_a_turn2 = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                json={
                    "agentId": alice_agent["agentId"],
                    "conversationId": str(conv_a_id),
                    "message": "Next step in Conv A",
                },
            )
            assert conv_a_turn2.status_code == 200
            prompt_a2 = model_provider.requests[-1].messages[0].content
            assert "- [conversation] Conv A targets PostgreSQL 17 partitioning" in prompt_a2
            assert "- [user] Alice prefers concise SQLAlchemy examples" in prompt_a2
            assert "Bob top secret preference" not in prompt_a2

            gen_a2 = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_a_id,
                generation_id=UUID(conv_a_turn2.json()["data"]["generationId"]),
            )
            assert gen_a2.user_message_id is not None
            refs_a2 = await generation_store.list_recalled_memories_for_message_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_a_id,
                user_message_id=gen_a2.user_message_id,
            )
            assert [(r.provider_id, r.memory_id, r.scope, r.prompt_position) for r in refs_a2] == [
                ("fake-memory", conv_a_mem_id, "conversation", 1),
                ("fake-memory", alice_user_mem_id, "user", 2),
            ]

            # Continue Conv B -> sees Alice's user memory ONLY; Conv A memory MUST NOT leak!
            conv_b_turn2 = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                json={
                    "agentId": alice_agent["agentId"],
                    "conversationId": str(conv_b_id),
                    "message": "Next step in Conv B",
                },
            )
            assert conv_b_turn2.status_code == 200
            prompt_b2 = model_provider.requests[-1].messages[0].content
            assert "- [user] Alice prefers concise SQLAlchemy examples" in prompt_b2
            assert "Conv A targets PostgreSQL 17 partitioning" not in prompt_b2
            assert "Bob top secret preference" not in prompt_b2

            gen_b2 = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_b_id,
                generation_id=UUID(conv_b_turn2.json()["data"]["generationId"]),
            )
            assert gen_b2.user_message_id is not None
            refs_b2 = await generation_store.list_recalled_memories_for_message_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=conv_b_id,
                user_message_id=gen_b2.user_message_id,
            )
            assert [(r.memory_id, r.scope, r.prompt_position) for r in refs_b2] == [
                (alice_user_mem_id, "user", 1)
            ]

            # Agent with `memoryPolicy.enabled=False` skips recall and saves 0 refs.
            no_mem_resp = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                json={"agentId": disabled_mem_agent["agentId"], "message": "Hello without memory"},
            )
            assert no_mem_resp.status_code == 200
            assert "Relevant Memories:" not in model_provider.requests[-1].messages[0].content
            no_mem_data = no_mem_resp.json()["data"]
            gen_no_mem = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(no_mem_data["conversationId"]),
                generation_id=UUID(no_mem_data["generationId"]),
            )
            assert gen_no_mem.user_message_id is not None
            refs_no_mem = await generation_store.list_recalled_memories_for_message_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(no_mem_data["conversationId"]),
                user_message_id=gen_no_mem.user_message_id,
            )
            assert refs_no_mem == []


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_api_degrades_gracefully_on_recall_failure_with_sanitized_logs(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    model_provider = _RecordingModelProvider()
    secret_user_text = "PRIVATE_USER_MESSAGE_MUST_NOT_APPEAR_IN_LOGS"
    failing_provider = _FailingMemoryProvider(exc=RuntimeError("vendor boom sk-secret-do-not-leak"))
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=failing_provider,
        ),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    try:
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
                alice_user_id = UUID(
                    (await client.get("/v1/users/me", headers=alice_headers)).json()["data"][
                        "userId"
                    ]
                )
                agent = await _create_published_agent(
                    client,
                    headers=alice_headers,
                    name="Resilient Agent",
                )

                # Turn 1 succeeds despite memory provider failing.
                resp1 = await client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                    json={"agentId": agent["agentId"], "message": "First turn before outage"},
                )
                assert resp1.status_code == 200
                conv_id = UUID(resp1.json()["data"]["conversationId"])

                # Turn 2 also succeeds with fixed persona + Turn 1 history + current input.
                resp2 = await client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                    json={
                        "agentId": agent["agentId"],
                        "conversationId": str(conv_id),
                        "message": secret_user_text,
                    },
                )
                assert resp2.status_code == 200
                last_req = model_provider.requests[-1]
                assert "Be helpful and respect user memory rules." in last_req.messages[0].content
                assert "Relevant Memories:" not in last_req.messages[0].content
                assert [(m.role, m.content) for m in last_req.messages[1:]] == [
                    ("user", "First turn before outage"),
                    ("assistant", "echo:First turn before outage"),
                    ("user", secret_user_text),
                ]

                gen2 = await generation_store.get_generation_for_owner(
                    app.state.session_factory,
                    owner_id=alice_user_id,
                    conversation_id=conv_id,
                    generation_id=UUID(resp2.json()["data"]["generationId"]),
                )
                assert gen2.user_message_id is not None
                refs = await generation_store.list_recalled_memories_for_message_for_owner(
                    app.state.session_factory,
                    owner_id=alice_user_id,
                    conversation_id=conv_id,
                    user_message_id=gen2.user_message_id,
                )
                assert refs == []
    finally:
        logger.removeHandler(log_handler)

    logs = log_buffer.getvalue()
    assert "memory_provider_error" in logs
    assert secret_user_text not in logs
    assert "sk-secret-do-not-leak" not in logs


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_api_end_to_end_with_memos_adapter_encapsulates_vendor_format(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    secret_memos_key = "memos-e2e-secret-token-777"
    captured_memos_calls: list[dict[str, Any]] = []
    model_provider = _RecordingModelProvider()

    def memos_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        captured_memos_calls.append(body)
        if body.get("conversation_id") is not None:
            conv_id = body["conversation_id"]
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {
                        "memory_detail_list": [
                            {
                                "id": "memos_conv_1",
                                "user_id": body["user_id"],
                                "memory_value": "Current sprint focuses on memory recall",
                                "conversation_id": conv_id,
                                "tags": [
                                    "memai:scope:conversation",
                                    f"memai:conv:{conv_id}",
                                    "memai:epoch:0",
                                ],
                                "relativity": 0.94,
                            }
                        ]
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "message": "ok",
                "data": {
                    "memory_detail_list": [
                        {
                            "id": "memos_user_1",
                            "user_id": body["user_id"],
                            "memory_value": "User prefers terse bullet points",
                            "scene": "scope:user",
                            "tags": ["memai:scope:user", "memai:epoch:0"],
                            "relativity": 0.87,
                        }
                    ]
                },
            },
        )

    mock_http = httpx.AsyncClient(transport=httpx.MockTransport(memos_handler))
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(model_provider=model_provider),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    try:
        async with app.router.lifespan_context(app):
            memos_provider = MemOSMemoryProvider(
                http_client=mock_http,
                session_factory=app.state.session_factory,
                api_key=SecretStr(secret_memos_key),
                base_url="https://memos.e2e.test/api/openmem/v1",
            )
            app.state.providers = ProviderRegistry(
                model_provider=model_provider,
                memory_provider=memos_provider,
            )

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
                alice_user_id = UUID(
                    (await client.get("/v1/users/me", headers=alice_headers)).json()["data"][
                        "userId"
                    ]
                )
                agent = await _create_published_agent(
                    client,
                    headers=alice_headers,
                    name="MemOS Agent",
                )

                resp = await client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": f"recall-{uuid4().hex}"},
                    json={"agentId": agent["agentId"], "message": "Summarize my sprint"},
                )
                assert resp.status_code == 200
                data = resp.json()["data"]
                conv_id = UUID(data["conversationId"])

                # Both user and conversation scoped MemOS `/search/memory` queries ran.
                assert len(captured_memos_calls) == 2

                # Prompt uses uniform format with zero MemOS vendor keys/IDs leaked.
                system_prompt = model_provider.requests[-1].messages[0].content
                assert "- [conversation] Current sprint focuses on memory recall" in system_prompt
                assert "- [user] User prefers terse bullet points" in system_prompt
                assert "memory_detail_list" not in system_prompt
                assert "memos_conv_1" not in system_prompt
                assert "memos_user_1" not in system_prompt
                assert secret_memos_key not in system_prompt

                gen = await generation_store.get_generation_for_owner(
                    app.state.session_factory,
                    owner_id=alice_user_id,
                    conversation_id=conv_id,
                    generation_id=UUID(data["generationId"]),
                )
                assert gen.user_message_id is not None
                refs = await generation_store.list_recalled_memories_for_message_for_owner(
                    app.state.session_factory,
                    owner_id=alice_user_id,
                    conversation_id=conv_id,
                    user_message_id=gen.user_message_id,
                )
                assert [(r.provider_id, r.memory_id, r.scope, r.prompt_position) for r in refs] == [
                    ("memos", "memos_conv_1", "conversation", 1),
                    ("memos", "memos_user_1", "user", 2),
                ]
    finally:
        await mock_http.aclose()
