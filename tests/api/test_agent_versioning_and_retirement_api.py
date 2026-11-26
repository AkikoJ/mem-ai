"""API tests verifying persona draft isolation, publish rollout, retirement, and chat prompts."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

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
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import BuiltinUsers, FakeMemoryProvider


class _RecordingModelProvider:
    """Controlled ModelProvider capturing exact prompts and model IDs across persona versions."""

    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        last_user = request.messages[-1].content
        return ModelResponse(
            model_id=request.model_id,
            content=f"[{request.model_id}] reply to: {last_user}",
            usage=ModelUsage(input_tokens=10, output_tokens=5, total_tokens=15),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


def _chat_headers(token: str, *, key: str | None = None) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Idempotency-Key": key if key is not None else f"t11-{uuid4().hex}",
    }


@pytest.mark.integration
@pytest.mark.anyio
async def test_chat_prompts_respect_draft_isolation_publish_rollout_and_retirement_across_restart(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
    fake_memory_provider: FakeMemoryProvider,
) -> None:
    model_provider_1 = _RecordingModelProvider()
    app1 = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider_1,
            memory_provider=fake_memory_provider,
        ),
    )

    async with app1.router.lifespan_context(app1):
        await models_service.sync_model_configs(
            app1.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="model-v1",
                    provider_id="openai_compatible",
                    upstream_model="upstream-v1",
                    display_name="Model V1",
                ),
                ModelConfigSeed(
                    model_id="model-v2",
                    provider_id="openai_compatible",
                    upstream_model="upstream-v2",
                    display_name="Model V2",
                ),
                ModelConfigSeed(
                    model_id="model-v3",
                    provider_id="openai_compatible",
                    upstream_model="upstream-v3",
                    display_name="Model V3",
                ),
            ],
        )

        transport1 = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport1, base_url="http://test") as client:
            # 1. Create Agent and publish v1.
            create_resp = await client.post(
                "/v1/agents",
                headers=builtin_users.alice.headers,
                json={
                    "name": "Versioned Coach",
                    "systemPrompt": "PERSONA_V1_INSTRUCTIONS",
                    "behaviorRules": ["RULE_V1_CONCISE"],
                    "tone": "formal",
                    "language": "en",
                    "defaultModelId": "model-v1",
                    "memoryPolicy": {"enabled": True, "writeEnabled": True, "writeScope": "user"},
                },
            )
            assert create_resp.status_code == 201
            agent_id = create_resp.json()["data"]["agentId"]

            pub_v1 = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=builtin_users.alice.headers,
            )
            assert pub_v1.status_code == 200
            v1_id = pub_v1.json()["data"]["currentPublishedVersionId"]

            # 2. Start Conversation 1 (pins v1).
            conv1_first_key = f"conv1-turn1-{uuid4().hex}"
            conv1_t1 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token, key=conv1_first_key),
                json={"agentId": agent_id, "message": "Hello from conv1"},
            )
            assert conv1_t1.status_code == 200
            conv1_data = conv1_t1.json()["data"]
            conv1_id = conv1_data["conversationId"]
            conv1_gen1_id = conv1_data["generationId"]
            assert conv1_data["modelId"] == "model-v1"

            req_1 = model_provider_1.requests[-1]
            assert req_1.model_id == "model-v1"
            assert req_1.upstream_model == "upstream-v1"
            assert "PERSONA_V1_INSTRUCTIONS" in req_1.messages[0].content
            assert "RULE_V1_CONCISE" in req_1.messages[0].content
            assert "Tone: formal" in req_1.messages[0].content

            # 3. Edit draft AND create unpublished v2 snapshot: published v1 must stay active!
            patch_resp = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=builtin_users.alice.headers,
                json={
                    "systemPrompt": "PERSONA_V2_INSTRUCTIONS",
                    "behaviorRules": ["RULE_V2_PLAYFUL"],
                    "tone": "playful",
                    "language": "zh-CN",
                    "defaultModelId": "model-v2",
                    "memoryPolicy": {"writeScope": "conversation"},
                },
            )
            assert patch_resp.status_code == 200
            assert patch_resp.json()["data"]["currentPublishedVersionId"] == v1_id

            ver_v2_resp = await client.post(
                f"/v1/agents/{agent_id}/versions",
                headers=builtin_users.alice.headers,
            )
            assert ver_v2_resp.status_code == 201
            v2_id = ver_v2_resp.json()["data"]["versionId"]
            assert ver_v2_resp.json()["data"]["version"] == 2
            assert ver_v2_resp.json()["data"]["publishedAt"] is None

            # New conversation before publishing v2 MUST still receive v1 prompt & model-v1.
            conv_before_pub = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={"agentId": agent_id, "message": "Before v2 publish"},
            )
            assert conv_before_pub.status_code == 200
            assert conv_before_pub.json()["data"]["modelId"] == "model-v1"
            req_before_pub = model_provider_1.requests[-1]
            assert "PERSONA_V1_INSTRUCTIONS" in req_before_pub.messages[0].content
            assert "PERSONA_V2_INSTRUCTIONS" not in req_before_pub.messages[0].content

            # 4. Publish explicit v2 snapshot.
            pub_v2 = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=builtin_users.alice.headers,
                json={"versionId": v2_id},
            )
            assert pub_v2.status_code == 200
            assert pub_v2.json()["data"]["currentPublishedVersionId"] == v2_id
            assert pub_v2.json()["data"]["currentPublishedVersion"] == 2

            # New Conversation 2 MUST use v2 prompt & model-v2.
            conv2_t1 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={"agentId": agent_id, "message": "Hello from conv2"},
            )
            assert conv2_t1.status_code == 200
            conv2_id = conv2_t1.json()["data"]["conversationId"]
            assert conv2_t1.json()["data"]["modelId"] == "model-v2"
            req_conv2 = model_provider_1.requests[-1]
            assert req_conv2.model_id == "model-v2"
            assert req_conv2.upstream_model == "upstream-v2"
            assert "PERSONA_V2_INSTRUCTIONS" in req_conv2.messages[0].content
            assert "RULE_V2_PLAYFUL" in req_conv2.messages[0].content
            assert "Tone: playful" in req_conv2.messages[0].content
            assert "Language: zh-CN" in req_conv2.messages[0].content

            # Continuing Conversation 1 MUST stay pinned to v1 prompt & model-v1.
            conv1_t2 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={
                    "agentId": agent_id,
                    "conversationId": conv1_id,
                    "message": "Follow-up in conv1 after v2 publish",
                },
            )
            assert conv1_t2.status_code == 200
            assert conv1_t2.json()["data"]["modelId"] == "model-v1"
            req_conv1_t2 = model_provider_1.requests[-1]
            assert req_conv1_t2.model_id == "model-v1"
            assert req_conv1_t2.upstream_model == "upstream-v1"
            assert "PERSONA_V1_INSTRUCTIONS" in req_conv1_t2.messages[0].content
            assert "PERSONA_V2_INSTRUCTIONS" not in req_conv1_t2.messages[0].content

            # 5. Update draft again and publish v3 directly from draft (empty body).
            patch_v3 = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=builtin_users.alice.headers,
                json={
                    "systemPrompt": "PERSONA_V3_INSTRUCTIONS",
                    "defaultModelId": "model-v3",
                },
            )
            assert patch_v3.status_code == 200
            pub_v3 = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=builtin_users.alice.headers,
            )
            assert pub_v3.status_code == 200
            v3_id = pub_v3.json()["data"]["currentPublishedVersionId"]
            assert pub_v3.json()["data"]["currentPublishedVersion"] == 3

            # 6. Retire Agent (and repeat idempotently).
            retire_1 = await client.delete(
                f"/v1/agents/{agent_id}",
                headers=builtin_users.alice.headers,
            )
            assert retire_1.status_code == 200
            retired_at = retire_1.json()["data"]["retiredAt"]
            assert retire_1.json()["data"]["status"] == "retired"
            assert retired_at is not None

            retire_2 = await client.delete(
                f"/v1/agents/{agent_id}",
                headers=builtin_users.alice.headers,
            )
            assert retire_2.status_code == 200
            assert retire_2.json()["data"]["retiredAt"] == retired_at

            # New conversation MUST be blocked after retirement, without invoking ModelProvider.
            calls_before_blocked = len(model_provider_1.requests)
            blocked_new_chat = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={"agentId": agent_id, "message": "Should be blocked"},
            )
            assert blocked_new_chat.status_code == 404
            assert blocked_new_chat.json()["code"] == "agent_persona_not_found"
            assert len(model_provider_1.requests) == calls_before_blocked

            # Existing Conversation 1 (v1) and Conversation 2 (v2) MUST continue normally!
            conv1_t3 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={
                    "agentId": agent_id,
                    "conversationId": conv1_id,
                    "message": "Continue conv1 after retire",
                },
            )
            assert conv1_t3.status_code == 200
            assert conv1_t3.json()["data"]["modelId"] == "model-v1"
            assert "PERSONA_V1_INSTRUCTIONS" in model_provider_1.requests[-1].messages[0].content

            conv2_t2 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={
                    "agentId": agent_id,
                    "conversationId": conv2_id,
                    "message": "Continue conv2 after retire",
                },
            )
            assert conv2_t2.status_code == 200
            assert conv2_t2.json()["data"]["modelId"] == "model-v2"
            assert "PERSONA_V2_INSTRUCTIONS" in model_provider_1.requests[-1].messages[0].content

            # Replaying original Idempotency-Key from before v2/v3/retire returns original result.
            replay_conv1_t1 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token, key=conv1_first_key),
                json={"agentId": agent_id, "message": "Hello from conv1"},
            )
            assert replay_conv1_t1.status_code == 200
            assert replay_conv1_t1.json()["data"]["generationId"] == conv1_gen1_id
            assert replay_conv1_t1.json()["data"]["modelId"] == "model-v1"

    # 7. Simulate full application restart with fresh engine and ModelProvider.
    model_provider_2 = _RecordingModelProvider()
    app2 = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider_2,
            memory_provider=fake_memory_provider,
        ),
    )
    async with app2.router.lifespan_context(app2):
        transport2 = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport2, base_url="http://test") as client:
            # Retired Agent and its last published persona snapshot (v3) remain readable.
            detail_after_restart = await client.get(
                f"/v1/agents/{agent_id}",
                headers=builtin_users.alice.headers,
            )
            assert detail_after_restart.status_code == 200
            detail_data = detail_after_restart.json()["data"]
            assert detail_data["status"] == "retired"
            assert detail_data["retiredAt"] == retired_at
            assert detail_data["currentPublishedVersionId"] == v3_id
            assert detail_data["publishedVersion"]["systemPrompt"] == "PERSONA_V3_INSTRUCTIONS"

            # New conversation remains blocked after restart.
            blocked_after_restart = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={"agentId": agent_id, "message": "Blocked after restart"},
            )
            assert blocked_after_restart.status_code == 404
            assert blocked_after_restart.json()["code"] == "agent_persona_not_found"
            assert model_provider_2.requests == []

            # Existing Conversation 1 continues after restart with pinned v1 & prior history.
            conv1_t4 = await client.post(
                "/v1/chat",
                headers=_chat_headers(builtin_users.alice.access_token),
                json={
                    "agentId": agent_id,
                    "conversationId": conv1_id,
                    "message": "After restart in conv1",
                },
            )
            assert conv1_t4.status_code == 200
            assert conv1_t4.json()["data"]["modelId"] == "model-v1"
            assert len(model_provider_2.requests) == 1
            restart_req = model_provider_2.requests[0]
            assert "PERSONA_V1_INSTRUCTIONS" in restart_req.messages[0].content
            history_contents = [m.content for m in restart_req.messages[1:]]
            assert "Hello from conv1" in history_contents
            assert "Continue conv1 after retire" in history_contents
            assert history_contents[-1] == "After restart in conv1"

            # Prior generation record remains readable after restart.
            gen_resp = await client.get(
                f"/v1/conversations/{conv1_id}/generations/{conv1_gen1_id}",
                headers=builtin_users.alice.headers,
            )
            assert gen_resp.status_code == 200
            assert gen_resp.json()["data"]["status"] == "succeeded"
            assert gen_resp.json()["data"]["modelId"] == "model-v1"
