"""End-to-end API + Worker tests for durable asynchronous turn memory writes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest

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
from mem_ai.core.config import Settings
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.auth.context import VerifiedPrincipal
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobBackoffPolicy
from mem_ai.modules.memories.jobs import JOB_TYPE_MEMORY_TURN_WRITE
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.workers.main import build_worker_registry
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.conftest import FakeMemoryProvider, FakeModelProvider


class _FlakyThenWorkingMemoryProvider(FakeMemoryProvider):
    """Fails the first `fail_writes` turn writes, then succeeds using `FakeMemoryProvider`."""

    def __init__(self, *, fail_writes: int = 1) -> None:
        super().__init__()
        self._remaining_failures = fail_writes
        self.turn_write_inputs: list[MemoryWriteInput] = []

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        self.turn_write_inputs.append(item)
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            return MemoryWriteResult(
                status="failed",
                provider_id=self.provider_id,
                external_ref=item.write_id,
                error_code="memory_provider_unavailable",
            )
        return await super().create_memory(item)

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        return await super().recall(query)

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        return await super().list_memories(query)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        return await super().delete_memory(user_id=user_id, memory_id=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        return await super().clear_memories(user_id=user_id)

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        return await super().verify_operation(user_id=user_id, external_ref=external_ref)


@pytest.mark.integration
@pytest.mark.anyio
async def test_post_chat_enqueues_turn_memory_and_worker_recovers_after_outage(
    migrated_pg_settings: Settings,
) -> None:
    memory_provider = _FlakyThenWorkingMemoryProvider(fail_writes=1)
    model_provider = FakeModelProvider(prefix="assistant:")
    verifier = StaticTokenAuthAdapter(
        {
            "tok-alice": VerifiedPrincipal(
                issuer="https://test.mem-ai.local",
                subject="alice-chat-jobs",
                actor_id="actor-alice",
                auth_method="static_token",
                permissions=frozenset(
                    {"chat:use", "agents:manage", "memories:manage", "users:self"}
                ),
            )
        }
    )

    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
        credential_verifier=verifier,
    )
    headers = {"Authorization": "Bearer tok-alice"}

    async with app.router.lifespan_context(app):
        session_factory = app.state.session_factory
        await models_service.sync_model_configs(
            session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="chat-default",
                    provider_id="openai_compatible",
                    upstream_model="chat-default",
                    display_name="Chat Default",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # Create & publish a conversation-scoped write Agent.
            agent_resp = await client.post(
                "/v1/agents",
                headers=headers,
                json={
                    "name": "Scoped Memory Agent",
                    "systemPrompt": "Remember task details per conversation.",
                    "defaultModelId": "chat-default",
                    "memoryPolicy": {
                        "enabled": True,
                        "writeEnabled": True,
                        "writeScope": "conversation",
                        "recallLimit": 5,
                    },
                },
            )
            assert agent_resp.status_code == 201
            agent_id = agent_resp.json()["data"]["agentId"]

            pub_resp = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
            assert pub_resp.status_code == 200

            # POST /v1/chat succeeds immediately without waiting for background memory provider.
            chat_resp = await client.post(
                "/v1/chat",
                headers=headers,
                json={
                    "agentId": agent_id,
                    "message": "Project codename is Orion.",
                },
            )
            assert chat_resp.status_code == 200
            chat_data = chat_resp.json()["data"]
            conv_id = UUID(chat_data["conversationId"])
            assert chat_data["message"]["content"] == "assistant:Project codename is Orion."

            # Memory is not written synchronously on the chat HTTP path.
            assert len(memory_provider.turn_write_inputs) == 0

            # Standalone Worker processes the job: first attempt fails -> retry_wait -> second
            # attempt succeeds and writes a conversation-scoped memory.
            runner = WorkerRunner(
                session_factory=session_factory,
                registry=build_worker_registry(session_factory, memory_provider=memory_provider),
                config=WorkerConfig(
                    worker_id="api-test-worker",
                    concurrency=2,
                    lease_ttl_seconds=30.0,
                    lease_renew_interval_seconds=None,
                    backoff_policy=JobBackoffPolicy(
                        base_delay_seconds=5.0,
                        max_delay_seconds=30.0,
                        jitter_ratio=0.0,
                    ),
                ),
            )
            t1 = datetime.now(UTC) + timedelta(seconds=1)
            assert await runner.run_once(now=t1) == 1
            mem_jobs_after_1 = await jobs_service.list_memory_jobs(
                session_factory,
                conversation_id=conv_id,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
            )
            assert len(mem_jobs_after_1) == 1
            assert mem_jobs_after_1[0].status == "retry_wait"

            t2 = t1 + timedelta(seconds=5.0)
            assert await runner.run_once(now=t2) == 1
            mem_jobs_after_2 = await jobs_service.list_memory_jobs(
                session_factory,
                conversation_id=conv_id,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
            )
            assert len(mem_jobs_after_2) == 1
            assert mem_jobs_after_2[0].status == "succeeded"

            # Verify persisted memory is visible via GET /v1/memories with conversation scope.
            list_resp = await client.get(
                "/v1/memories",
                headers=headers,
                params={"scope": "conversation", "conversationId": str(conv_id)},
            )
            assert list_resp.status_code == 200
            items = list_resp.json()["data"]["items"]
            assert len(items) == 1
            assert items[0]["scope"] == "conversation"
            assert items[0]["conversationId"] == str(conv_id)
            assert "Project codename is Orion." in items[0]["content"]
