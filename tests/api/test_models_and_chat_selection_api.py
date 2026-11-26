"""API tests for `GET /v1/models` discovery and `POST /v1/chat` model selection policies."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest

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
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import BuiltinUsers


class _RecordingModelProvider:
    """Controlled ModelProvider asserting exact model IDs, call counts, and failures."""

    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []
        self.next_error: Exception | None = None

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if self.next_error is not None:
            exc = self.next_error
            self.next_error = None
            raise exc
        return ModelResponse(
            model_id=request.model_id,
            content=f"answered-by:{request.model_id}:{request.upstream_model}",
            usage=ModelUsage(input_tokens=11, output_tokens=7, total_tokens=18),
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
    name: str,
    default_model_id: str | None,
) -> dict[str, object]:
    body: dict[str, object] = {
        "name": name,
        "systemPrompt": f"Instructions for {name}",
        "behaviorRules": ["Be concise"],
    }
    if default_model_id is not None:
        body["defaultModelId"] = default_model_id

    created = await client.post("/v1/agents", headers=headers, json=body)
    assert created.status_code == 201
    agent_id = created.json()["data"]["agentId"]
    published = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert published.status_code == 200
    data: dict[str, object] = published.json()["data"]
    return data


@pytest.mark.integration
@pytest.mark.anyio
async def test_models_endpoint_lists_all_enabled_models_equally_and_hides_server_secrets(
    builtin_users: BuiltinUsers,
    migrated_pg_settings: Settings,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "model_registry_entries": [
                {
                    "model_id": "gpt-4o-mini",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-secret-mini-2026",
                    "display_name": "GPT-4o Mini",
                    "enabled": True,
                    "supports_streaming": True,
                    "max_context_tokens": 128000,
                    "max_output_tokens": 4096,
                    "credential_ref": "ENV_OPENAI_MINI_SECRET",
                    "capabilities_json": {"json_mode": True},
                },
                {
                    "model_id": "gpt-4o-pro",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-secret-pro-2026",
                    "display_name": "GPT-4o Pro",
                    "enabled": True,
                    "supports_streaming": True,
                    "max_context_tokens": 256000,
                    "max_output_tokens": 16384,
                    "credential_ref": "ENV_OPENAI_PRO_SECRET",
                    "capabilities_json": {"json_mode": True, "reasoning": True},
                },
                {
                    "model_id": "batch-non-stream",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-batch",
                    "display_name": "Batch Non-Stream",
                    "enabled": True,
                    "supports_streaming": False,
                    "max_context_tokens": 32000,
                    "max_output_tokens": 2048,
                    "credential_ref": "ENV_BATCH_SECRET",
                },
                {
                    "model_id": "retired-disabled",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-retired",
                    "display_name": "Retired",
                    "enabled": False,
                    "supports_streaming": True,
                },
            ]
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(model_provider=_RecordingModelProvider()),
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated request is rejected with 401.
            unauth = await client.get("/v1/models")
            assert unauth.status_code == 401

            # 2. Every active user sees all enabled models, for both registered users.
            std_resp = await client.get(
                "/v1/models",
                headers=builtin_users.alice.headers,
            )
            assert std_resp.status_code == 200
            std_body = std_resp.json()
            assert std_body["success"] is True
            assert std_body["code"] == "ok"
            std_items = std_body["data"]["items"]
            assert [item["modelId"] for item in std_items] == [
                "batch-non-stream",
                "gpt-4o-mini",
                "gpt-4o-pro",
            ]

            # 3. Another registered user sees the same enabled model list.
            pro_resp = await client.get(
                "/v1/models",
                headers=builtin_users.bob.headers,
            )
            assert pro_resp.status_code == 200
            pro_items = pro_resp.json()["data"]["items"]
            assert [item["modelId"] for item in pro_items] == [
                "batch-non-stream",
                "gpt-4o-mini",
                "gpt-4o-pro",
            ]

            # 4. Streaming capability query filter uses the same capability metadata.
            stream_resp = await client.get(
                "/v1/models?supportsStreaming=true",
                headers=builtin_users.bob.headers,
            )
            assert stream_resp.status_code == 200
            assert [item["modelId"] for item in stream_resp.json()["data"]["items"]] == [
                "gpt-4o-mini",
                "gpt-4o-pro",
            ]

            # 5. Internal provider/upstream/credential fields are never exposed.
            raw_json = json.dumps(pro_resp.json())
            for secret_or_internal in (
                "providerId",
                "provider_id",
                "openai_compatible",
                "upstreamModel",
                "upstream_model",
                "vendor-secret-mini-2026",
                "vendor-secret-pro-2026",
                "credentialRef",
                "credential_ref",
                "ENV_OPENAI_MINI_SECRET",
                "ENV_OPENAI_PRO_SECRET",
            ):
                assert secret_or_internal not in raw_json


@pytest.mark.integration
@pytest.mark.anyio
async def test_builtin_users_share_enabled_models_and_select_requested_model(
    builtin_users: BuiltinUsers,
    migrated_pg_settings: Settings,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "model_allow_request_override": True,
            "model_registry_entries": [
                {"model_id": "standard", "enabled": True},
                {
                    "model_id": "pro",
                    "upstream_model": "vendor-pro",
                    "enabled": True,
                },
                {"model_id": "disabled", "enabled": False},
            ],
        }
    )
    provider = _RecordingModelProvider()
    app = create_app(settings, providers=ProviderRegistry(model_provider=provider))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        alice_headers = builtin_users.alice.headers
        for headers in (alice_headers, builtin_users.bob.headers):
            response = await client.get("/v1/models", headers=headers)
            assert response.status_code == 200, response.text
            assert [item["modelId"] for item in response.json()["data"]["items"]] == [
                "pro",
                "standard",
            ]

        agent = await _create_published_agent(
            client,
            headers=alice_headers,
            name="Built-in Model Selection",
            default_model_id="standard",
        )
        chat = await client.post(
            "/v1/chat",
            headers={**alice_headers, "Idempotency-Key": f"builtin-model-{uuid4().hex}"},
            json={
                "agentId": agent["agentId"],
                "message": "use the pro model",
                "modelId": "pro",
            },
        )
        assert chat.status_code == 200, chat.text
        assert chat.json()["data"]["modelId"] == "pro"
        assert [(request.model_id, request.upstream_model) for request in provider.requests] == [
            ("pro", "vendor-pro")
        ]


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_model_override_disabled_pinned_default_failure_recording_and_schema_guard(
    builtin_users: BuiltinUsers,
    migrated_pg_settings: Settings,
) -> None:
    provider = _RecordingModelProvider()
    settings = migrated_pg_settings.model_copy(
        update={
            "model_allow_request_override": True,
            "model_default_id": "gpt-4o-mini",
            "model_registry_entries": [
                {
                    "model_id": "gpt-4o-mini",
                    "upstream_model": "vendor-mini",
                    "display_name": "Mini",
                    "enabled": True,
                },
                {
                    "model_id": "gpt-4o-alt",
                    "upstream_model": "vendor-alt",
                    "display_name": "Alt",
                    "enabled": True,
                },
                {
                    "model_id": "gpt-4o-pro",
                    "upstream_model": "vendor-pro",
                    "display_name": "Pro",
                    "enabled": True,
                },
                {
                    "model_id": "gpt-disabled",
                    "upstream_model": "vendor-disabled",
                    "display_name": "Disabled",
                    "enabled": False,
                },
            ],
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(model_provider=provider),
    )
    std_headers = builtin_users.alice.headers
    pro_headers = builtin_users.bob.headers

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            pro_me = await client.get("/v1/users/me", headers=pro_headers)
            pro_user_id = UUID(pro_me.json()["data"]["userId"])

            # 1. Schema strictly rejects client-supplied API keys, Base URLs, and provider configs.
            std_agent = await _create_published_agent(
                client,
                headers=std_headers,
                name="Standard Agent",
                default_model_id="gpt-4o-mini",
            )
            for forbidden_payload in (
                {"apiKey": "sk-client"},
                {"baseUrl": "https://attacker.example/v1"},
                {"providerConfig": {"url": "https://attacker.example"}},
                {"providerId": "openai_compatible"},
            ):
                bad = await client.post(
                    "/v1/chat",
                    headers={**std_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                    json={
                        "agentId": std_agent["agentId"],
                        "message": "hijack attempt",
                        **forbidden_payload,
                    },
                )
                assert bad.status_code == 422
            assert len(provider.requests) == 0

            # 2. Unregistered and disabled requested models fail before any model call.
            for bad_model, expected_status, expected_code in (
                ("does-not-exist", 409, "model_unavailable"),
                ("gpt-disabled", 409, "model_unavailable"),
            ):
                rejected = await client.post(
                    "/v1/chat",
                    headers={**std_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                    json={
                        "agentId": std_agent["agentId"],
                        "message": "should reject",
                        "modelId": bad_model,
                    },
                )
                assert rejected.status_code == expected_status
                assert rejected.json()["code"] == expected_code
            assert len(provider.requests) == 0

            # 3. Every active user can select every enabled model.
            pro_agent = await _create_published_agent(
                client,
                headers=pro_headers,
                name="Pro Agent",
                default_model_id="gpt-4o-mini",
            )
            turn1 = await client.post(
                "/v1/chat",
                headers={**pro_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={"agentId": pro_agent["agentId"], "message": "use default"},
            )
            assert turn1.status_code == 200
            conv_id = UUID(turn1.json()["data"]["conversationId"])
            assert turn1.json()["data"]["modelId"] == "gpt-4o-mini"
            assert len(provider.requests) == 1
            assert provider.requests[-1].model_id == "gpt-4o-mini"
            assert provider.requests[-1].upstream_model == "vendor-mini"

            turn2 = await client.post(
                "/v1/chat",
                headers={**pro_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={
                    "agentId": pro_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "override to pro",
                    "modelId": "gpt-4o-pro",
                },
            )
            assert turn2.status_code == 200
            assert turn2.json()["data"]["modelId"] == "gpt-4o-pro"
            assert len(provider.requests) == 2
            assert provider.requests[-1].model_id == "gpt-4o-pro"
            assert provider.requests[-1].upstream_model == "vendor-pro"

            # Verify persisted generation saved the actual selected modelId.
            gen2 = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=pro_user_id,
                conversation_id=conv_id,
                generation_id=UUID(turn2.json()["data"]["generationId"]),
            )
            assert gen2.model_id == "gpt-4o-pro"
            assert gen2.status == "succeeded"

            # 4. Disable the pinned Agent default model (`gpt-4o-mini`) via server sync:
            #    - Continuing without modelId returns 409 `model_unavailable` (NO auto failover!)
            #    - Continuing with an explicit enabled modelId (`gpt-4o-alt`) succeeds!
            await models_service.sync_model_configs(
                app.state.session_factory,
                seeds=[
                    ModelConfigSeed(
                        model_id="gpt-4o-mini",
                        upstream_model="vendor-mini",
                        display_name="Mini",
                        enabled=False,
                    )
                ],
            )
            calls_before = len(provider.requests)
            pinned_disabled_resp = await client.post(
                "/v1/chat",
                headers={**pro_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={
                    "agentId": pro_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "default disabled now",
                },
            )
            assert pinned_disabled_resp.status_code == 409
            assert pinned_disabled_resp.json()["code"] == "model_unavailable"
            assert len(provider.requests) == calls_before

            recovered_resp = await client.post(
                "/v1/chat",
                headers={**pro_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={
                    "agentId": pro_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "continue with explicit alt",
                    "modelId": "gpt-4o-alt",
                },
            )
            assert recovered_resp.status_code == 200
            assert recovered_resp.json()["data"]["modelId"] == "gpt-4o-alt"
            assert len(provider.requests) == calls_before + 1
            assert provider.requests[-1].model_id == "gpt-4o-alt"

            # 5. Selected model upstream failure produces single call, no failover, and persists
            #    the exact failed generation with selected modelId.
            calls_before_fail = len(provider.requests)
            provider.next_error = BadGatewayError(
                code="model_upstream_error",
                message="pro upstream failed",
            )
            fail_resp = await client.post(
                "/v1/chat",
                headers={**pro_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={
                    "agentId": pro_agent["agentId"],
                    "conversationId": str(conv_id),
                    "message": "fail on pro",
                    "modelId": "gpt-4o-pro",
                },
            )
            assert fail_resp.status_code == 502
            assert fail_resp.json()["code"] == "model_upstream_error"
            assert len(provider.requests) == calls_before_fail + 1
            assert provider.requests[-1].model_id == "gpt-4o-pro"

            persisted_msgs = await generation_store.list_conversation_messages_for_owner(
                app.state.session_factory,
                owner_id=pro_user_id,
                conversation_id=conv_id,
            )
            failed_user_msg = persisted_msgs[-1]
            assert failed_user_msg.role == "user"
            assert failed_user_msg.content == "fail on pro"
            failed_gen = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=pro_user_id,
                conversation_id=conv_id,
                generation_id=failed_user_msg.generation_id,
            )
            assert failed_gen.status == "failed"
            assert failed_gen.model_id == "gpt-4o-pro"
            assert failed_gen.error_code == "model_upstream_error"


@pytest.mark.integration
@pytest.mark.anyio
async def test_server_forced_policy_takes_precedence_over_request_and_agent_default(
    builtin_users: BuiltinUsers,
    migrated_pg_settings: Settings,
) -> None:
    alice_headers = builtin_users.alice.headers

    provider = _RecordingModelProvider()
    forced_settings = migrated_pg_settings.model_copy(
        update={
            "model_forced_id": "forced-model",
            "model_allow_request_override": True,
            "model_default_id": "default-model",
            "model_registry_entries": [
                {
                    "model_id": "forced-model",
                    "upstream_model": "vendor-forced",
                    "display_name": "Forced",
                    "enabled": True,
                },
                {
                    "model_id": "default-model",
                    "upstream_model": "vendor-default",
                    "display_name": "Default",
                    "enabled": True,
                },
            ],
        }
    )
    app_forced = create_app(
        forced_settings,
        providers=ProviderRegistry(model_provider=provider),
    )

    async with app_forced.router.lifespan_context(app_forced):
        transport = httpx.ASGITransport(app=app_forced)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            models_resp = await client.get("/v1/models", headers=alice_headers)
            assert models_resp.status_code == 200
            assert [item["modelId"] for item in models_resp.json()["data"]["items"]] == [
                "forced-model"
            ]

            agent = await _create_published_agent(
                client,
                headers=alice_headers,
                name="Forced Test Agent",
                default_model_id="default-model",
            )
            chat_resp = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={
                    "agentId": agent["agentId"],
                    "message": "forced wins",
                    "modelId": "default-model",
                },
            )
            assert chat_resp.status_code == 200
            assert chat_resp.json()["data"]["modelId"] == "forced-model"
            assert len(provider.requests) == 1
            assert provider.requests[0].model_id == "forced-model"
            assert provider.requests[0].upstream_model == "vendor-forced"


@pytest.mark.integration
@pytest.mark.anyio
async def test_missing_server_model_config_returns_503_before_upstream_call(
    builtin_users: BuiltinUsers,
    migrated_pg_settings: Settings,
) -> None:
    alice_headers = builtin_users.alice.headers

    empty_provider = _RecordingModelProvider()
    missing_cfg_settings = migrated_pg_settings.model_copy(
        update={
            "model_default_id": None,
            "model_forced_id": None,
            "model_registry_entries": [],
        }
    )
    app_missing = create_app(
        missing_cfg_settings,
        providers=ProviderRegistry(model_provider=empty_provider),
    )
    async with app_missing.router.lifespan_context(app_missing):
        transport = httpx.ASGITransport(app=app_missing)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent_no_default = await _create_published_agent(
                client,
                headers=alice_headers,
                name="No Default Model Agent",
                default_model_id=None,
            )
            missing_resp = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={"agentId": agent_no_default["agentId"], "message": "no model configured"},
            )
            assert missing_resp.status_code == 503
            assert missing_resp.json()["code"] == "model_configuration_missing"
            assert len(empty_provider.requests) == 0

            # Even when an Agent specifies defaultModelId, an empty synced `model_configs`
            # table returns stable 503 `model_configuration_missing` rather than 409.
            agent_with_default = await _create_published_agent(
                client,
                headers=alice_headers,
                name="Unsynced Registry Agent",
                default_model_id="gpt-4o-mini",
            )
            unsynced_resp = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": f"model-{uuid4().hex}"},
                json={"agentId": agent_with_default["agentId"], "message": "registry empty"},
            )
            assert unsynced_resp.status_code == 503
            assert unsynced_resp.json()["code"] == "model_configuration_missing"
            assert len(empty_provider.requests) == 0
