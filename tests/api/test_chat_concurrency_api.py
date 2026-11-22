"""API tests for conversation generation concurrency, lease release, and expired takeover."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from sqlalchemy import update

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
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations.models import Generation
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed


class _ControllableChatModelProvider:
    """Controlled ModelProvider supporting gated prompts, errors, and slow timeouts."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.gates: dict[str, tuple[asyncio.Event, asyncio.Event]] = {}
        self.error_prompts: dict[str, Exception] = {}
        self.slow_prompts: dict[str, float] = {}

    def gate(self, prompt: str) -> tuple[asyncio.Event, asyncio.Event]:
        pair = (asyncio.Event(), asyncio.Event())
        self.gates[prompt] = pair
        return pair

    async def generate(self, request: ModelRequest) -> ModelResponse:
        prompt = request.messages[-1].content
        self.calls.append(prompt)
        if prompt in self.gates:
            started, release = self.gates[prompt]
            started.set()
            await release.wait()
        if prompt in self.slow_prompts:
            await asyncio.sleep(self.slow_prompts[prompt])
        if prompt in self.error_prompts:
            raise self.error_prompts[prompt]
        return ModelResponse(
            model_id=request.model_id,
            content=f"reply:{prompt}",
            usage=ModelUsage(input_tokens=10, output_tokens=5, total_tokens=15),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


async def _create_published_agent(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
) -> str:
    create_resp = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": "Concurrent Agent",
            "systemPrompt": "Respond concisely.",
            "defaultModelId": "gpt-4o-mini",
        },
    )
    assert create_resp.status_code == 201
    agent_id: str = create_resp.json()["data"]["agentId"]
    pub_resp = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert pub_resp.status_code == 200
    return agent_id


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_api_rejects_overlapping_generation_allows_parallel_and_recovers_expired_lease(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableChatModelProvider()
    settings = migrated_pg_settings.model_copy(
        update={
            "chat_generation_timeout_seconds": 0.2,
            "chat_lease_ttl_seconds": 10,
            "chat_lease_renew_interval_seconds": 3.0,
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(model_provider=provider),
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
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_me = await client.get("/v1/users/me", headers=alice_headers)
            alice_user_id = UUID(alice_me.json()["data"]["userId"])
            agent_id = await _create_published_agent(client, headers=alice_headers)

            # Seed Conversation A and Conversation B.
            resp_a = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={"agentId": agent_id, "message": "init-a"},
            )
            assert resp_a.status_code == 200
            conv_a = resp_a.json()["data"]["conversationId"]

            resp_b = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={"agentId": agent_id, "message": "init-b"},
            )
            assert resp_b.status_code == 200
            conv_b = resp_b.json()["data"]["conversationId"]

            # 1. Pause Conversation A in model call; competing POST returns 409 immediately
            #    while Conversation B succeeds in parallel.
            started_a, release_a = provider.gate("hold-a")
            task_a = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers=alice_headers,
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_a,
                        "message": "hold-a",
                    },
                )
            )
            await asyncio.wait_for(started_a.wait(), timeout=3.0)

            calls_snapshot = list(provider.calls)
            conflict_resp = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_a,
                    "message": "blocked-a",
                },
            )
            assert conflict_resp.status_code == 409
            conflict_body = conflict_resp.json()
            assert conflict_body["success"] is False
            assert conflict_body["code"] == "conversation_generation_conflict"
            assert conflict_body["data"] is None
            assert conflict_body["details"]["conversation_id"] == conv_a
            assert provider.calls == calls_snapshot

            parallel_b = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_b,
                    "message": "parallel-b",
                },
            )
            assert parallel_b.status_code == 200
            assert parallel_b.json()["data"]["message"]["content"] == "reply:parallel-b"

            release_a.set()
            done_a = await asyncio.wait_for(task_a, timeout=3.0)
            assert done_a.status_code == 200

            # 2. Failure (502) and timeout (504) both release the lease immediately.
            provider.error_prompts["fail-a"] = BadGatewayError(
                code="model_upstream_error",
                message="upstream failed",
            )
            fail_resp = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_a,
                    "message": "fail-a",
                },
            )
            assert fail_resp.status_code == 502

            provider.slow_prompts["timeout-a"] = 1.0
            timeout_resp = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_a,
                    "message": "timeout-a",
                },
            )
            assert timeout_resp.status_code == 504
            assert timeout_resp.json()["code"] == "generation_timeout"

            after_errors_resp = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_a,
                    "message": "ok-after-errors",
                },
            )
            assert after_errors_resp.status_code == 200

            # 3. Orphaned expired running generation (crashed process, no Worker) is reclaimed
            #    inline by the next `POST /v1/chat` without returning 409.
            persona_provider = app.state.providers.persona_provider
            conv_a_record = await generation_store.ensure_conversation_accessible(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(conv_a),
            )
            pinned_persona = await persona_provider.get_version(
                owner_id=alice_user_id,
                agent_id=UUID(agent_id),
                version_id=conv_a_record.persona_version_id,
            )
            assert pinned_persona is not None
            orphaned = await generation_store.prepare_generation_phase(
                app.state.session_factory,
                owner_id=alice_user_id,
                agent_id=UUID(agent_id),
                conversation_id=UUID(conv_a),
                persona=pinned_persona,
                model_id="gpt-4o-mini",
                user_message_content="crashed-process-msg",
                lease_ttl_seconds=1.0,
            )
            async with app.state.session_factory.begin() as session:
                await session.execute(
                    update(Generation)
                    .where(Generation.id == orphaned.generation_id)
                    .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=5))
                )

            takeover_resp = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_a,
                    "message": "reclaim-via-api",
                },
            )
            assert takeover_resp.status_code == 200
            assert takeover_resp.json()["data"]["message"]["content"] == "reply:reclaim-via-api"

            reclaimed_gen = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(conv_a),
                generation_id=orphaned.generation_id,
            )
            assert reclaimed_gen.status == "failed"
            assert reclaimed_gen.error_code == "generation_lease_lost"
            assert reclaimed_gen.lease_token is None

            # Confirm the competing `blocked-a` message was never written to DB.
            persisted = await generation_store.list_conversation_messages_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(conv_a),
            )
            assert "blocked-a" not in [m.content for m in persisted]

            # 4. Simultaneous race on the same conversation: exactly one 200 and one 409.
            provider.slow_prompts["race-1"] = 0.08
            provider.slow_prompts["race-2"] = 0.08
            race_r1, race_r2 = await asyncio.gather(
                client.post(
                    "/v1/chat",
                    headers=alice_headers,
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_a,
                        "message": "race-1",
                    },
                ),
                client.post(
                    "/v1/chat",
                    headers=alice_headers,
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_a,
                        "message": "race-2",
                    },
                ),
            )
            statuses = sorted([race_r1.status_code, race_r2.status_code])
            assert statuses == [200, 409]
            loser = race_r1 if race_r1.status_code == 409 else race_r2
            assert loser.json()["code"] == "conversation_generation_conflict"
