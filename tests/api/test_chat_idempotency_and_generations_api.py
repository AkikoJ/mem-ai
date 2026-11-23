"""API tests for chat `Idempotency-Key`, lost-response recovery, stream switch, and query."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

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
from mem_ai.core.exceptions import BadGatewayError, GatewayTimeoutError
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations.models import Conversation, Generation, IdempotencyRecord
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.users.models import User


class _ControllableModelProvider:
    """ModelProvider supporting per-prompt gating, scripted errors, and invocation counting."""

    def __init__(self) -> None:
        self.calls: list[ModelRequest] = []
        self._gates: dict[str, tuple[asyncio.Event, asyncio.Event, asyncio.Event]] = {}
        self.errors: dict[str, Exception] = {}

    def gate(self, prompt_text: str) -> tuple[asyncio.Event, asyncio.Event, asyncio.Event]:
        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()
        self._gates[prompt_text] = (started, release, finished)
        return started, release, finished

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.calls.append(request)
        text = request.messages[-1].content
        if text in self._gates:
            started, release, finished = self._gates[text]
            started.set()
            await release.wait()
            try:
                if text in self.errors:
                    raise self.errors.pop(text)
                return ModelResponse(
                    model_id=request.model_id,
                    content=f"reply:{text}",
                    usage=ModelUsage(input_tokens=15, output_tokens=5, total_tokens=20),
                    finish_reason="stop",
                )
            finally:
                finished.set()
        if text in self.errors:
            raise self.errors.pop(text)
        return ModelResponse(
            model_id=request.model_id,
            content=f"reply:{text}",
            usage=ModelUsage(input_tokens=15, output_tokens=5, total_tokens=20),
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
    name: str = "Idem Agent",
    default_model_id: str = "gpt-4o-mini",
) -> str:
    created = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": name,
            "systemPrompt": "Original v1 prompt.",
            "defaultModelId": default_model_id,
        },
    )
    assert created.status_code == 201
    agent_id: str = created.json()["data"]["agentId"]
    pub = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert pub.status_code == 200
    return agent_id


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_idempotency_required_header_lost_response_stream_switch_and_config_drift(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableModelProvider()
    app = create_app(
        migrated_pg_settings,
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
                ),
                ModelConfigSeed(
                    model_id="gpt-4o-v2",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-v2",
                    display_name="GPT-4o V2",
                    enabled=True,
                ),
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent_id = await _create_published_agent(client, headers=alice_headers)

            # 0. OpenAPI advertises `Idempotency-Key` as a required header on `POST /v1/chat`.
            openapi_doc = (await client.get("/openapi.json")).json()
            chat_params = openapi_doc["paths"]["/v1/chat"]["post"]["parameters"]
            idem_param = next(p for p in chat_params if p["name"] == "Idempotency-Key")
            assert idem_param["in"] == "header"
            assert idem_param["required"] is True

            # 1. Missing and invalid Idempotency-Key return 422 before model call.
            missing = await client.post(
                "/v1/chat",
                headers=alice_headers,
                json={"agentId": agent_id, "message": "hello"},
            )
            assert missing.status_code == 422
            assert missing.json()["code"] == "idempotency_key_required"

            blank = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "   "},
                json={"agentId": agent_id, "message": "hello"},
            )
            assert blank.status_code == 422
            assert blank.json()["code"] == "invalid_idempotency_key"
            assert len(provider.calls) == 0

            # 2. Lost first response on implicit conversation creation:
            #    Start request, drop client connection mid-model, recover IDs via 202 retry!
            started, release, finished = provider.gate("lost-response-turn")
            idem_headers = {**alice_headers, "Idempotency-Key": "idem-lost-1"}
            initial_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers=idem_headers,
                    json={"agentId": agent_id, "message": "lost-response-turn", "stream": False},
                )
            )
            await asyncio.wait_for(started.wait(), timeout=3.0)
            initial_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await initial_task

            # Retry with same key & same business params (switching stream=True!):
            # MUST return 202 + Location + original conversationId/generationId/statusUrl.
            running_retry = await client.post(
                "/v1/chat",
                headers=idem_headers,
                json={"agentId": agent_id, "message": "lost-response-turn", "stream": True},
            )
            assert running_retry.status_code == 202
            running_data = running_retry.json()["data"]
            conv_id = running_data["conversationId"]
            gen_id = running_data["generationId"]
            status_url = running_data["statusUrl"]
            assert running_data["status"] == "running"
            assert running_data["userMessageId"] is not None
            assert status_url == f"/v1/conversations/{conv_id}/generations/{gen_id}"
            assert running_retry.headers["location"] == status_url

            # Query generation while running -> 200 OK with running status.
            running_get = await client.get(status_url, headers=alice_headers)
            assert running_get.status_code == 200
            running_gen_data = running_get.json()["data"]
            assert running_gen_data["status"] == "running"
            assert running_gen_data["conversationId"] == conv_id
            assert running_gen_data["generationId"] == gen_id
            assert running_gen_data["modelId"] == "gpt-4o-mini"
            assert running_gen_data["message"] is None
            assert running_gen_data["error"] is None

            # Same key with different business digest (different message OR explicit conversationId
            # instead of omitted) -> 409 idempotency_key_conflict!
            diff_msg = await client.post(
                "/v1/chat",
                headers=idem_headers,
                json={"agentId": agent_id, "message": "different-message"},
            )
            assert diff_msg.status_code == 409
            assert diff_msg.json()["code"] == "idempotency_key_conflict"

            diff_conv_flag = await client.post(
                "/v1/chat",
                headers=idem_headers,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "lost-response-turn",
                },
            )
            assert diff_conv_flag.status_code == 409
            assert diff_conv_flag.json()["code"] == "idempotency_key_conflict"

            # Release model and wait for shielded background commit to complete.
            release.set()
            await asyncio.wait_for(finished.wait(), timeout=3.0)
            await app.state.sync_generation_tasks.drain(timeout_seconds=3.0)
            assert len(provider.calls) == 1

            # 3. Mutate Agent (publish v2 with new model) AND disable original `gpt-4o-mini`:
            #    Retrying the original key MUST return the saved 200 JSON (for both stream=False
            #    and stream=True) without calling the model again!
            patch_resp = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=alice_headers,
                json={"systemPrompt": "Mutated v2 prompt", "defaultModelId": "gpt-4o-v2"},
            )
            assert patch_resp.status_code == 200
            pub_v2 = await client.post(f"/v1/agents/{agent_id}/publish", headers=alice_headers)
            assert pub_v2.status_code == 200
            await models_service.sync_model_configs(
                app.state.session_factory,
                seeds=[
                    ModelConfigSeed(
                        model_id="gpt-4o-mini",
                        provider_id="openai_compatible",
                        upstream_model="gpt-4o-mini",
                        display_name="GPT-4o Mini",
                        enabled=False,
                    )
                ],
            )

            replay_sync = await client.post(
                "/v1/chat",
                headers=idem_headers,
                json={"agentId": agent_id, "message": "lost-response-turn", "stream": False},
            )
            replay_stream_flag = await client.post(
                "/v1/chat",
                headers=idem_headers,
                json={"agentId": agent_id, "message": "lost-response-turn", "stream": True},
            )
            assert replay_sync.status_code == 200
            assert replay_stream_flag.status_code == 200
            assert replay_stream_flag.headers["content-type"].startswith("application/json")
            assert replay_sync.json()["data"] == replay_stream_flag.json()["data"]
            saved_data = replay_sync.json()["data"]
            assert saved_data["conversationId"] == conv_id
            assert saved_data["generationId"] == gen_id
            assert saved_data["modelId"] == "gpt-4o-mini"
            assert saved_data["message"]["content"] == "reply:lost-response-turn"
            assert saved_data["usage"] == {
                "inputTokens": 15,
                "outputTokens": 5,
                "totalTokens": 20,
            }
            assert len(provider.calls) == 1

            # Query generation after success -> 200 OK with full message & usage.
            done_get = await client.get(status_url, headers=alice_headers)
            assert done_get.status_code == 200
            done_data = done_get.json()["data"]
            assert done_data["status"] == "succeeded"
            assert done_data["messageId"] == saved_data["messageId"]
            assert done_data["message"]["content"] == "reply:lost-response-turn"
            assert done_data["usage"] == saved_data["usage"]
            assert done_data["error"] is None
            assert done_data["completedAt"] is not None


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_idempotency_concurrency_failures_cancellation_and_lease_precedence(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableModelProvider()
    app = create_app(
        migrated_pg_settings,
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
            alice_user_id = UUID(
                (await client.get("/v1/users/me", headers=alice_headers)).json()["data"]["userId"]
            )
            agent_id = await _create_published_agent(client, headers=alice_headers)

            # 1. True simultaneous same-key HTTP requests racing Tx 1 create ONE conversation,
            #    ONE generation, and invoke the model ONCE (one returns 200, the other 202 or 200).
            started_c, release_c, _ = provider.gate("concurrent-http")
            same_key_headers = {**alice_headers, "Idempotency-Key": "idem-concurrent-http"}

            async def _release_after_start() -> None:
                await asyncio.wait_for(started_c.wait(), timeout=3.0)
                await asyncio.sleep(0.05)
                release_c.set()

            releaser = asyncio.create_task(_release_after_start())
            r_a, r_b = await asyncio.gather(
                client.post(
                    "/v1/chat",
                    headers=same_key_headers,
                    json={"agentId": agent_id, "message": "concurrent-http"},
                ),
                client.post(
                    "/v1/chat",
                    headers=same_key_headers,
                    json={"agentId": agent_id, "message": "concurrent-http"},
                ),
            )
            await releaser
            assert sorted([r_a.status_code, r_b.status_code]) in ([200, 202], [200, 200])
            r_first = r_a if r_a.status_code == 200 else r_b
            assert r_a.json()["data"]["conversationId"] == r_b.json()["data"]["conversationId"]
            assert r_a.json()["data"]["generationId"] == r_b.json()["data"]["generationId"]
            assert len(provider.calls) == 1
            conv_id = r_first.json()["data"]["conversationId"]

            # 2. Idempotency lookup precedes lease contention:
            #    While a NEW key is running on `conv_id`, retrying Turn 1's key returns 200 (same)
            #    or 409 idempotency_key_conflict (different), NOT conversation_generation_conflict.
            started_t2, release_t2, _ = provider.gate("turn-2-running")
            t2 = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": "idem-turn-2"},
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_id,
                        "message": "turn-2-running",
                    },
                )
            )
            await asyncio.wait_for(started_t2.wait(), timeout=3.0)

            replay_turn1 = await client.post(
                "/v1/chat",
                headers=same_key_headers,
                json={"agentId": agent_id, "message": "concurrent-http"},
            )
            assert replay_turn1.status_code == 200
            assert (
                replay_turn1.json()["data"]["generationId"]
                == r_first.json()["data"]["generationId"]
            )

            conflict_turn1 = await client.post(
                "/v1/chat",
                headers=same_key_headers,
                json={"agentId": agent_id, "message": "altered-while-conv-busy"},
            )
            assert conflict_turn1.status_code == 409
            assert conflict_turn1.json()["code"] == "idempotency_key_conflict"

            release_t2.set()
            assert (await asyncio.wait_for(t2, timeout=3.0)).status_code == 200

            # 3. Failed (502) and timed-out (504) generations replay their exact status & details,
            #    while a NEW key can retry and succeed.
            provider.errors["upstream-boom"] = BadGatewayError(
                code="model_upstream_error", message="upstream crashed"
            )
            calls_before_fail = len(provider.calls)
            fail_key = {**alice_headers, "Idempotency-Key": "idem-fail-502"}
            fail_resp = await client.post(
                "/v1/chat",
                headers=fail_key,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "upstream-boom",
                },
            )
            assert fail_resp.status_code == 502
            assert len(provider.calls) == calls_before_fail + 1

            fail_replay = await client.post(
                "/v1/chat",
                headers=fail_key,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "upstream-boom",
                },
            )
            assert fail_replay.status_code == 502
            fail_body = fail_replay.json()
            assert fail_body["code"] == "model_upstream_error"
            assert fail_body["details"]["status"] == "failed"
            assert fail_body["details"]["conversation_id"] == conv_id
            failed_gen_id = fail_body["details"]["generation_id"]
            assert len(provider.calls) == calls_before_fail + 1

            # Query failed generation -> 200 OK with saved error object.
            fail_get = await client.get(
                f"/v1/conversations/{conv_id}/generations/{failed_gen_id}",
                headers=alice_headers,
            )
            assert fail_get.status_code == 200
            assert fail_get.json()["data"]["status"] == "failed"
            assert fail_get.json()["data"]["error"]["code"] == "model_upstream_error"

            # New key after failure succeeds!
            retry_new_key = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "idem-new-after-fail"},
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "upstream-boom",
                },
            )
            assert retry_new_key.status_code == 200
            assert retry_new_key.json()["data"]["message"]["content"] == "reply:upstream-boom"

            # Timeout (504) replays 504 `generation_timeout`.
            provider.errors["timeout-msg"] = GatewayTimeoutError(
                code="generation_timeout", message="timed out"
            )
            timeout_key = {**alice_headers, "Idempotency-Key": "idem-timeout-504"}
            assert (
                await client.post(
                    "/v1/chat",
                    headers=timeout_key,
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_id,
                        "message": "timeout-msg",
                    },
                )
            ).status_code == 504
            timeout_replay = await client.post(
                "/v1/chat",
                headers=timeout_key,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "timeout-msg",
                },
            )
            assert timeout_replay.status_code == 504
            assert timeout_replay.json()["code"] == "generation_timeout"
            assert timeout_replay.json()["details"]["status"] == "failed"

            # 4. Cancelled generation replays 409 `generation_cancelled` and GET returns 200.
            started_can, release_can, _ = provider.gate("cancel-me")
            cancel_key = {**alice_headers, "Idempotency-Key": "idem-cancel-409"}
            cancel_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers=cancel_key,
                    json={
                        "agentId": agent_id,
                        "conversationId": conv_id,
                        "message": "cancel-me",
                    },
                )
            )
            await asyncio.wait_for(started_can.wait(), timeout=3.0)
            running_can = await client.post(
                "/v1/chat",
                headers=cancel_key,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "cancel-me",
                },
            )
            assert running_can.status_code == 202
            can_gen_id = UUID(running_can.json()["data"]["generationId"])
            can_gen_row = await conversation_queries.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(conv_id),
                generation_id=can_gen_id,
            )
            assert can_gen_row.lease_token is not None
            await generation_store.converge_generation_failure_phase(
                app.state.session_factory,
                owner_id=alice_user_id,
                conversation_id=UUID(conv_id),
                generation_id=can_gen_id,
                lease_token=can_gen_row.lease_token,
                target_status="cancelled",
                error_code="generation_cancelled",
                error_message="cancelled by client",
            )
            release_can.set()
            _ = await asyncio.wait_for(cancel_task, timeout=3.0)

            cancel_replay = await client.post(
                "/v1/chat",
                headers=cancel_key,
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "cancel-me",
                },
            )
            assert cancel_replay.status_code == 409
            assert cancel_replay.json()["code"] == "generation_cancelled"
            assert cancel_replay.json()["details"]["status"] == "cancelled"

            cancel_get = await client.get(
                f"/v1/conversations/{conv_id}/generations/{can_gen_id}",
                headers=alice_headers,
            )
            assert cancel_get.status_code == 200
            assert cancel_get.json()["data"]["status"] == "cancelled"
            assert cancel_get.json()["data"]["error"]["code"] == "generation_cancelled"


@pytest.mark.integration
@pytest.mark.anyio
async def test_generation_query_cross_user_lease_recovery_and_deleted_resources(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    provider = _ControllableModelProvider()
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
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_agent = await _create_published_agent(client, headers=alice_headers, name="Alice")
            bob_agent = await _create_published_agent(client, headers=bob_headers, name="Bob")
            bob_user_id = UUID(
                (await client.get("/v1/users/me", headers=bob_headers)).json()["data"]["userId"]
            )

            # 1. Cross-user isolation: Alice and Bob can both use the exact same Idempotency-Key.
            alice_chat = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "shared-key-01"},
                json={"agentId": alice_agent, "message": "alice hello"},
            )
            bob_chat = await client.post(
                "/v1/chat",
                headers={**bob_headers, "Idempotency-Key": "shared-key-01"},
                json={"agentId": bob_agent, "message": "bob hello"},
            )
            assert alice_chat.status_code == 200
            assert bob_chat.status_code == 200
            alice_conv = alice_chat.json()["data"]["conversationId"]
            alice_gen = alice_chat.json()["data"]["generationId"]
            bob_conv = bob_chat.json()["data"]["conversationId"]
            bob_gen = bob_chat.json()["data"]["generationId"]
            assert alice_conv != bob_conv

            # Bob cannot read Alice's generation (404 hides existence); unknown generation is 404.
            bob_read_alice = await client.get(
                f"/v1/conversations/{alice_conv}/generations/{alice_gen}",
                headers=bob_headers,
            )
            assert bob_read_alice.status_code == 404
            missing_gen = await client.get(
                f"/v1/conversations/{alice_conv}/generations/{uuid4()}",
                headers=alice_headers,
            )
            assert missing_gen.status_code == 404

            # 2. Expired lease recovered by Worker -> retry returns 409 `generation_lease_lost`,
            #    and GET returns 200 with `status="failed"`.
            started_exp, release_exp, _ = provider.gate("will-expire")
            exp_key = {**alice_headers, "Idempotency-Key": "idem-expired-lease"}
            exp_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers=exp_key,
                    json={
                        "agentId": alice_agent,
                        "conversationId": alice_conv,
                        "message": "will-expire",
                    },
                )
            )
            await asyncio.wait_for(started_exp.wait(), timeout=3.0)
            exp_202 = await client.post(
                "/v1/chat",
                headers=exp_key,
                json={
                    "agentId": alice_agent,
                    "conversationId": alice_conv,
                    "message": "will-expire",
                },
            )
            assert exp_202.status_code == 202
            exp_gen_id = UUID(exp_202.json()["data"]["generationId"])

            async with app.state.session_factory.begin() as session:
                await session.execute(
                    update(Generation)
                    .where(Generation.id == exp_gen_id)
                    .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=10))
                )
            recovered = await generation_store.recover_expired_generations_phase(
                app.state.session_factory
            )
            assert any(g.id == exp_gen_id for g in recovered)
            release_exp.set()
            _ = await asyncio.wait_for(exp_task, timeout=3.0)

            exp_replay = await client.post(
                "/v1/chat",
                headers=exp_key,
                json={
                    "agentId": alice_agent,
                    "conversationId": alice_conv,
                    "message": "will-expire",
                },
            )
            assert exp_replay.status_code == 409
            assert exp_replay.json()["code"] == "generation_lease_lost"
            assert exp_replay.json()["details"]["status"] == "failed"

            exp_get = await client.get(
                f"/v1/conversations/{alice_conv}/generations/{exp_gen_id}",
                headers=alice_headers,
            )
            assert exp_get.status_code == 200
            assert exp_get.json()["data"]["status"] == "failed"
            assert exp_get.json()["data"]["error"]["code"] == "generation_lease_lost"

            # 3. Conversation `deleting`: GET & replay return 404; new-key continuation returns 409.
            async with app.state.session_factory.begin() as session:
                await session.execute(
                    update(Conversation)
                    .where(Conversation.id == UUID(alice_conv))
                    .values(status="deleting")
                )

            assert (
                await client.get(
                    f"/v1/conversations/{alice_conv}/generations/{alice_gen}",
                    headers=alice_headers,
                )
            ).status_code == 404
            assert (
                await client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": "shared-key-01"},
                    json={"agentId": alice_agent, "message": "alice hello"},
                )
            ).status_code == 404
            cont_deleting = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "brand-new-on-deleting"},
                json={
                    "agentId": alice_agent,
                    "conversationId": alice_conv,
                    "message": "new turn",
                },
            )
            assert cont_deleting.status_code == 409
            assert cont_deleting.json()["code"] == "conversation_deleting"

            # 4. HTTP 24-hour idempotency expiry boundary:
            #    - Before 24h expiry -> replays saved 200 JSON (no new model call).
            #    - At/past 24h expiry -> key is reusable as a brand-new generation while the
            #      original generation stays queryable via GET.
            calls_before_expiry = len(provider.calls)
            bob_idem_headers = {**bob_headers, "Idempotency-Key": "shared-key-01"}
            almost_expired_term = datetime.now(UTC) - timedelta(hours=23, minutes=59)
            async with app.state.session_factory.begin() as session:
                await session.execute(
                    update(IdempotencyRecord)
                    .where(IdempotencyRecord.generation_id == UUID(bob_gen))
                    .values(
                        terminal_at=almost_expired_term,
                        expires_at=almost_expired_term + timedelta(hours=24),
                    )
                )
            still_live_resp = await client.post(
                "/v1/chat",
                headers=bob_idem_headers,
                json={"agentId": bob_agent, "message": "bob hello"},
            )
            assert still_live_resp.status_code == 200
            assert still_live_resp.json()["data"]["generationId"] == bob_gen
            assert len(provider.calls) == calls_before_expiry

            past_expired_term = datetime.now(UTC) - timedelta(hours=24, seconds=5)
            async with app.state.session_factory.begin() as session:
                await session.execute(
                    update(IdempotencyRecord)
                    .where(IdempotencyRecord.generation_id == UUID(bob_gen))
                    .values(
                        terminal_at=past_expired_term,
                        expires_at=past_expired_term + timedelta(hours=24),
                    )
                )
            reused_resp = await client.post(
                "/v1/chat",
                headers=bob_idem_headers,
                json={"agentId": bob_agent, "message": "bob fresh after 24h"},
            )
            assert reused_resp.status_code == 200
            new_bob_gen = reused_resp.json()["data"]["generationId"]
            assert new_bob_gen != bob_gen
            assert len(provider.calls) == calls_before_expiry + 1

            # Original generation remains queryable independently of expired idempotency record!
            old_bob_get = await client.get(
                f"/v1/conversations/{bob_conv}/generations/{bob_gen}",
                headers=bob_headers,
            )
            assert old_bob_get.status_code == 200
            assert old_bob_get.json()["data"]["status"] == "succeeded"

            # 5. User `disabled` and `deleting`: GET & POST /v1/chat retry both return 403.
            for user_status, expected_code in (
                ("disabled", "user_disabled"),
                ("deleting", "user_deleting"),
            ):
                async with app.state.session_factory.begin() as session:
                    await session.execute(
                        update(User).where(User.id == bob_user_id).values(status=user_status)
                    )

                bob_get_blocked = await client.get(
                    f"/v1/conversations/{bob_conv}/generations/{bob_gen}",
                    headers=bob_headers,
                )
                assert bob_get_blocked.status_code == 403
                assert bob_get_blocked.json()["code"] == expected_code
                bob_replay_blocked = await client.post(
                    "/v1/chat",
                    headers=bob_idem_headers,
                    json={"agentId": bob_agent, "message": "bob fresh after 24h"},
                )
                assert bob_replay_blocked.status_code == 403
                assert bob_replay_blocked.json()["code"] == expected_code
