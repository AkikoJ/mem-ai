"""HTTP API tests for memory single-delete, confirm=true clear, and operation polling."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryOperationStatus,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import GatewayTimeoutError
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations.queries import list_recalled_memories_for_message_for_owner
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.users import service as users_service
from mem_ai.workers.assembly import build_worker_registry
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.conftest import BuiltinUsers, FakeMemoryProvider, FakeModelProvider


class _ScriptedDeletionMemoryProvider(FakeMemoryProvider):
    """FakeMemoryProvider with scriptable delete/clear/verify outcomes for API tests."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_mode: str = "normal"
        self.clear_mode: str = "normal"
        self.verify_mode: str = "succeeded"

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        if self.delete_mode == "reconciling":
            return MemoryDeleteResult(
                status="reconciling",
                external_ref=f"delete_memory:{memory_id}",
                error_code="memory_provider_timeout",
            )
        if self.delete_mode == "unavailable":
            return MemoryDeleteResult(
                status="failed",
                external_ref=f"delete_memory:{memory_id}",
                error_code="memory_provider_unavailable",
            )
        return await super().delete_memory(user_id=user_id, memory_id=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        if self.clear_mode == "timeout":
            raise GatewayTimeoutError(
                code="memory_provider_timeout",
                message="clear request timed out",
            )
        if self.clear_mode == "unavailable":
            return MemoryDeleteResult(
                status="failed",
                external_ref=f"clear_memories:{user_id}",
                error_code="memory_provider_unavailable",
            )
        return await super().clear_memories(user_id=user_id)

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        if self.verify_mode == "succeeded":
            if external_ref.startswith("delete_memory:"):
                mid = external_ref[len("delete_memory:") :]
                rec = self.records.get(mid)
                if rec is not None and rec.user_id == user_id:
                    del self.records[mid]
            elif external_ref.startswith("clear_memories:"):
                for mid in [k for k, v in self.records.items() if v.user_id == user_id]:
                    del self.records[mid]
            return MemoryOperationStatus(status="succeeded", external_ref=external_ref)
        return MemoryOperationStatus(status="reconciling", external_ref=external_ref)

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await super().create_memory(item)


@pytest.mark.integration
@pytest.mark.anyio
async def test_single_delete_and_clear_http_contracts_204_202_404_409_422_and_audit(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "api-deletion-audit.jsonl"
    settings = migrated_pg_settings.model_copy(
        update={"deletion_audit_archive_path": str(audit_path)}
    )
    memory_provider = _ScriptedDeletionMemoryProvider()
    model_provider = FakeModelProvider(prefix="answer:")
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
    )
    alice_headers = builtin_users.alice.headers
    bob_headers = builtin_users.bob.headers

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="test-model",
                    provider_id="openai_compatible",
                    upstream_model="test-model",
                    display_name="Test Model",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated delete/clear rejected with 401.
            assert (await client.delete("/v1/memories/mem_x")).status_code == 401
            assert (await client.delete("/v1/memories?confirm=true")).status_code == 401

            # 2. Clear without confirm=true rejected with 422 clear_confirmation_required.
            no_confirm = await client.delete("/v1/memories", headers=alice_headers)
            assert no_confirm.status_code == 422
            assert no_confirm.json()["code"] == "clear_confirmation_required"
            false_confirm = await client.delete(
                "/v1/memories", headers=alice_headers, params={"confirm": "false"}
            )
            assert false_confirm.status_code == 422
            assert false_confirm.json()["code"] == "clear_confirmation_required"

            # 3. Create two memories for Alice and publish an Agent to chat with recall.
            m1_resp = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Alice prefers dark mode"},
            )
            assert m1_resp.status_code == 201
            m1_id = m1_resp.json()["data"]["memoryId"]

            m2_resp = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Alice speaks Mandarin"},
            )
            assert m2_resp.status_code == 201
            m2_id = m2_resp.json()["data"]["memoryId"]

            agent_resp = await client.post(
                "/v1/agents",
                headers=alice_headers,
                json={
                    "name": "Memory Agent",
                    "systemPrompt": "Be helpful.",
                    "defaultModelId": "test-model",
                    "memoryPolicy": {"enabled": True, "recallLimit": 5, "writeEnabled": True},
                },
            )
            assert agent_resp.status_code == 201
            agent_id = agent_resp.json()["data"]["agentId"]
            pub_resp = await client.post(f"/v1/agents/{agent_id}/publish", headers=alice_headers)
            assert pub_resp.status_code == 200

            chat1 = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "idem-del-1"},
                json={"agentId": agent_id, "message": "Tell me my preferences"},
            )
            assert chat1.status_code == 200
            conv_id = UUID(chat1.json()["data"]["conversationId"])
            gen_id = UUID(chat1.json()["data"]["generationId"])

            # 4. Bob cannot delete Alice's memory (404 memory_not_found, Alice's memory intact).
            bob_del = await client.delete(f"/v1/memories/{m1_id}", headers=bob_headers)
            assert bob_del.status_code == 404
            assert bob_del.json()["code"] == "memory_not_found"
            assert m1_id in memory_provider.records

            # 5. Alice deletes m1 synchronously -> 204 No Content, historical ref marked deleted_at.
            del_m1 = await client.delete(f"/v1/memories/{m1_id}", headers=alice_headers)
            assert del_m1.status_code == 204
            assert del_m1.content == b""

            # Idempotent second delete of m1 returns 204.
            del_m1_again = await client.delete(f"/v1/memories/{m1_id}", headers=alice_headers)
            assert del_m1_again.status_code == 204

            alice_me = await client.get("/v1/users/me", headers=alice_headers)
            alice_id = UUID(alice_me.json()["data"]["userId"])
            gen_rec = await generation_store.get_generation_for_owner(
                app.state.session_factory,
                owner_id=alice_id,
                conversation_id=conv_id,
                generation_id=gen_id,
            )
            assert gen_rec.user_message_id is not None
            refs = await list_recalled_memories_for_message_for_owner(
                app.state.session_factory,
                owner_id=alice_id,
                conversation_id=conv_id,
                user_message_id=gen_rec.user_message_id,
            )
            by_mid = {r.memory_id: r for r in refs}
            assert by_mid[m1_id].deleted_at is not None
            assert by_mid[m2_id].deleted_at is None

            # 6. Alice deletes m2 with reconciling provider -> 202 + Location + queryable operation.
            memory_provider.delete_mode = "reconciling"
            del_m2 = await client.delete(f"/v1/memories/{m2_id}", headers=alice_headers)
            assert del_m2.status_code == 202
            del_m2_data = del_m2.json()["data"]
            assert del_m2_data["status"] == "reconciling"
            assert del_m2.headers["location"] == del_m2_data["statusUrl"]

            # Owner can read operation; Bob receives uniform 404.
            op_alice = await client.get(del_m2_data["statusUrl"], headers=alice_headers)
            assert op_alice.status_code == 200
            assert op_alice.json()["data"]["status"] == "reconciling"
            assert op_alice.json()["data"]["operationType"] == "memory_delete"
            op_bob = await client.get(del_m2_data["statusUrl"], headers=bob_headers)
            assert op_bob.status_code == 404

            # Worker verifies m2 delete -> operation becomes `succeeded`.
            worker = WorkerRunner(
                session_factory=app.state.session_factory,
                registry=build_worker_registry(
                    app.state.session_factory,
                    memory_provider=memory_provider,
                    audit_archiver=app.state.deletion_audit_archiver,
                ),
                config=WorkerConfig(lease_renew_interval_seconds=None),
            )
            await worker.run_once(now=datetime.now(UTC) + timedelta(seconds=20))
            op_m2_done = await client.get(del_m2_data["statusUrl"], headers=alice_headers)
            assert op_m2_done.status_code == 200
            assert op_m2_done.json()["data"]["status"] == "succeeded"

            # 7. Clear with timeout -> 202 reconciling; barrier blocks explicit create (409)
            #    while chat still succeeds with degraded recall!
            memory_provider.delete_mode = "normal"
            memory_provider.clear_mode = "timeout"
            clear_202 = await client.delete(
                "/v1/memories", headers=alice_headers, params={"confirm": "true"}
            )
            assert clear_202.status_code == 202
            clear_op_url = clear_202.json()["data"]["statusUrl"]
            assert clear_202.headers["location"] == clear_op_url
            assert clear_202.json()["data"]["status"] == "reconciling"

            # Explicit create blocked with 409 memory_scope_clearing during barrier.
            blocked_create = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Blocked during clear"},
            )
            assert blocked_create.status_code == 409
            assert blocked_create.json()["code"] == "memory_scope_clearing"

            # Chat still succeeds during barrier (graceful degradation).
            chat_during = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": "idem-del-2"},
                json={"agentId": agent_id, "conversationId": str(conv_id), "message": "Hello"},
            )
            assert chat_during.status_code == 200

            # Worker verifies clear -> barrier released & operation succeeded.
            memory_provider.clear_mode = "normal"
            await worker.run_once(now=datetime.now(UTC) + timedelta(seconds=40))
            clear_op_done = await client.get(clear_op_url, headers=alice_headers)
            assert clear_op_done.status_code == 200
            assert clear_op_done.json()["data"]["status"] == "succeeded"

            # 8. Clear with zero in-flight writes & synchronous provider confirm -> 204 No Content!
            clear_204 = await client.delete(
                "/v1/memories", headers=alice_headers, params={"confirm": "true"}
            )
            assert clear_204.status_code == 204

            # 9. Deterministic provider failure on single delete -> 503 stable error envelope.
            memory_provider.delete_mode = "unavailable"
            m3_resp = await client.post(
                "/v1/memories", headers=alice_headers, json={"content": "Will fail delete"}
            )
            assert m3_resp.status_code == 201
            m3_id = m3_resp.json()["data"]["memoryId"]
            fail_del = await client.delete(f"/v1/memories/{m3_id}", headers=alice_headers)
            assert fail_del.status_code == 503
            assert fail_del.json()["code"] == "memory_provider_unavailable"

            # 10. Disabled user blocked with 403.
            await users_service.set_user_status(
                app.state.session_factory, user_id=alice_id, status="disabled"
            )
            assert (
                await client.delete("/v1/memories?confirm=true", headers=alice_headers)
            ).status_code == 403

    # Verify independent JSONL deletion audit archive captured operations & epoch events.
    assert audit_path.exists()
    archived: list[dict[str, Any]] = [
        json.loads(line) for line in audit_path.read_text("utf-8").splitlines() if line.strip()
    ]
    assert any(ev["event_type"] == "memory_epoch_advanced" for ev in archived)
    assert any(ev["event_type"] == "memory_barrier_released" for ev in archived)
    assert any(ev["event_type"] == "memory_tombstone_verified" for ev in archived)
