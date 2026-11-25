"""User deletion through real PostgreSQL and the shared Worker."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryOperationStatus,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.core.config import Settings
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.schemas import PreparedGeneration
from mem_ai.modules.conversations.service import ChatOptions
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories.snapshots import MemoryListScopeKey, MemorySnapshotStore
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations.audit import DeletionAuditEvent
from mem_ai.modules.users.cleanup import JOB_TYPE_USER_DELETE, UserCleanupJobHandler
from mem_ai.workers.main import build_worker_registry
from mem_ai.workers.runner import JobHandlerRegistry, WorkerConfig, WorkerRunner
from tests.api.test_chat_idempotency_and_generations_api import _ControllableModelProvider
from tests.api.test_conversations_api import GatedMemoryProvider

ALICE = {"Authorization": "Bearer token-alice"}
BOB = {"Authorization": "Bearer token-bob"}


class DeletionMemoryProvider(GatedMemoryProvider):
    """Remote boundary with durable deferred clears surviving Worker replacement."""

    supports_write_status_verification = True

    def __init__(self) -> None:
        super().__init__()
        self.defer_clear = False
        self.allow_clear_verification = False
        self.clear_calls: list[UUID] = []
        self.pending_clears: dict[str, UUID] = {}
        self.clear_error_code: str | None = None
        self.partial_clear = False
        self.unknown_writes = False
        self.late_record_after_first_clear = False

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        result = await super().create_memory(item)
        return (
            result.model_copy(update={"status": "reconciling"}) if self.unknown_writes else result
        )

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        self.clear_calls.append(user_id)
        if self.clear_error_code is not None:
            return MemoryDeleteResult(status="failed", error_code=self.clear_error_code)
        if self.defer_clear:
            ref = f"clear:{user_id}"
            self.pending_clears[ref] = user_id
            if self.partial_clear:
                first = next(
                    record for record in self.records.values() if record.user_id == user_id
                )
                await super().delete_memory(user_id=user_id, memory_id=first.memory_id)
            return MemoryDeleteResult(status="reconciling", external_ref=ref)
        result = await super().clear_memories(user_id=user_id)
        if self.late_record_after_first_clear and len(self.clear_calls) == 1:
            await super().create_memory(
                MemoryWriteInput(
                    write_id="late-unknown-write",
                    user_id=user_id,
                    scope="user",
                    content="Late record after first clear",
                )
            )
        return result

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        if external_ref in self.pending_clears:
            assert self.pending_clears[external_ref] == user_id
            if not self.allow_clear_verification:
                return MemoryOperationStatus(status="reconciling", external_ref=external_ref)
            await super().clear_memories(user_id=user_id)
            return MemoryOperationStatus(status="succeeded", external_ref=external_ref)
        return await super().verify_operation(user_id=user_id, external_ref=external_ref)


class _RecordingAuditArchiver:
    def __init__(self) -> None:
        self.events: list[DeletionAuditEvent] = []

    async def export_event(self, event: DeletionAuditEvent) -> None:
        self.events.append(event)


@dataclass
class UserDeletionHarness:
    client: httpx.AsyncClient
    sessions: async_sessionmaker[AsyncSession]
    memory: DeletionMemoryProvider
    model: _ControllableModelProvider
    settings: Settings
    audit_archiver: _RecordingAuditArchiver = field(default_factory=_RecordingAuditArchiver)
    clock_time: datetime | None = None

    def worker(
        self,
        *,
        concurrency: int = 3,
        batch_size: int = 50,
        job_types: tuple[str, ...] | None = None,
    ) -> WorkerRunner:
        assembled = build_worker_registry(
            self.sessions,
            memory_provider=self.memory,
            audit_archiver=self.audit_archiver,
            clock=lambda: self.clock_time or datetime.now(UTC),
        )
        registry = JobHandlerRegistry()
        for job_type in assembled.job_types():
            if job_types is not None and job_type not in job_types:
                continue
            handler = assembled.get(job_type)
            assert handler is not None
            registry.register(
                job_type,
                UserCleanupJobHandler(
                    session_factory=self.sessions,
                    batch_size=batch_size,
                    audit_archiver=self.audit_archiver,
                )
                if job_type == JOB_TYPE_USER_DELETE
                else handler,
            )
        return WorkerRunner(
            session_factory=self.sessions,
            registry=registry,
            config=WorkerConfig(
                worker_id=f"user-delete-{uuid4().hex[:8]}",
                concurrency=concurrency,
                claim_batch_size=concurrency,
                lease_renew_interval_seconds=None,
            ),
        )

    async def run(self, worker: WorkerRunner, *, now: datetime) -> None:
        self.clock_time = now
        await worker.run_once(now=now)

    async def operation(self, accepted: dict[str, Any]) -> dict[str, Any]:
        response = await self.client.get(
            accepted["statusUrl"],
            headers={"Authorization": f"DeletionReceipt {accepted['deletionReceipt']}"},
        )
        assert response.status_code == 200, response.text
        data: dict[str, Any] = response.json()["data"]
        return data

    async def finish(
        self,
        accepted: dict[str, Any],
        *,
        worker: WorkerRunner | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        runner = worker or self.worker()
        now = now or datetime.now(UTC)
        for step in range(40):
            await self.run(runner, now=now + timedelta(minutes=step + 1))
            operation = await self.operation(accepted)
            if operation["status"] == "succeeded":
                await self.run(runner, now=now + timedelta(minutes=step + 2))
                return operation
        pytest.fail(f"User deletion did not converge: {operation}")


@pytest.fixture
async def deletion_harness(
    migrated_pg_settings: Settings, test_auth_adapter: StaticTokenAuthAdapter
) -> AsyncIterator[UserDeletionHarness]:
    memory = DeletionMemoryProvider()
    model = _ControllableModelProvider()
    audit_archiver = _RecordingAuditArchiver()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(model_provider=model, memory_provider=memory),
        credential_verifier=test_auth_adapter,
    )
    async with app.router.lifespan_context(app):
        app.state.deletion_audit_archiver = audit_archiver
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="test-model",
                    provider_id="openai_compatible",
                    upstream_model="test-model",
                    display_name="Deletion test model",
                    enabled=True,
                )
            ],
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield UserDeletionHarness(
                client,
                app.state.session_factory,
                memory,
                model,
                migrated_pg_settings,
                audit_archiver,
            )


async def create_agent(harness: UserDeletionHarness, headers: dict[str, str] = ALICE) -> str:
    response = await harness.client.post(
        "/v1/agents",
        headers=headers,
        json={"name": "Deletion", "systemPrompt": "Be helpful", "defaultModelId": "test-model"},
    )
    assert response.status_code == 201, response.text
    agent_id: str = response.json()["data"]["agentId"]
    published = await harness.client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert published.status_code == 200, published.text
    return agent_id


async def chat(
    harness: UserDeletionHarness,
    agent_id: str,
    key: str,
    *,
    headers: dict[str, str] = ALICE,
    conversation_id: str | None = None,
) -> dict[str, Any]:
    response = await harness.client.post(
        "/v1/chat",
        headers={**headers, "Idempotency-Key": key},
        json={"agentId": agent_id, "message": key, "conversationId": conversation_id},
    )
    assert response.status_code == 200, response.text
    data: dict[str, Any] = response.json()["data"]
    return data


async def create_memory(
    harness: UserDeletionHarness,
    content: str,
    *,
    headers: dict[str, str] = ALICE,
    conversation_id: str | None = None,
) -> str:
    response = await harness.client.post(
        "/v1/memories",
        headers=headers,
        json={
            "content": content,
            "scope": "conversation" if conversation_id else "user",
            "conversationId": conversation_id,
        },
    )
    assert response.status_code == 201, response.text
    memory_id: str = response.json()["data"]["memoryId"]
    return memory_id


async def accept_deletion(harness: UserDeletionHarness) -> dict[str, Any]:
    response = await harness.client.delete("/v1/users/me", headers=ALICE)
    assert response.status_code == 202, response.text
    data: dict[str, Any] = response.json()["data"]
    return data


async def resource_counts(harness: UserDeletionHarness, owner_id: UUID) -> dict[str, int]:
    """Observe the agreed PostgreSQL cleanup contract, including indirectly owned rows."""
    query = text(
        "SELECT 'users', count(*) FROM users WHERE id = :owner "
        "UNION ALL SELECT 'identities', count(*) FROM user_identities WHERE user_id = :owner "
        "UNION ALL SELECT 'agents', count(*) FROM agents WHERE owner_id = :owner "
        "UNION ALL SELECT 'versions', count(*) FROM persona_versions WHERE agent_id IN "
        "(SELECT id FROM agents WHERE owner_id = :owner) "
        "UNION ALL SELECT 'conversations', count(*) FROM conversations WHERE owner_id = :owner "
        "UNION ALL SELECT 'generations', count(*) FROM generations WHERE owner_id = :owner "
        "UNION ALL SELECT 'messages', count(*) FROM messages WHERE conversation_id IN "
        "(SELECT id FROM conversations WHERE owner_id = :owner) "
        "UNION ALL SELECT 'references', count(*) FROM message_recalled_memories "
        "WHERE user_message_id IN (SELECT id FROM messages WHERE conversation_id IN "
        "(SELECT id FROM conversations WHERE owner_id = :owner)) "
        "UNION ALL SELECT 'idempotency', count(*) FROM idempotency_records WHERE user_id = :owner "
        "UNION ALL SELECT 'snapshots', count(*) FROM memory_list_snapshots WHERE user_id = :owner "
        "UNION ALL SELECT 'scopes', count(*) FROM memory_scopes WHERE user_id = :owner "
        "UNION ALL SELECT 'tombstones', count(*) FROM memory_tombstones WHERE user_id = :owner "
        "UNION ALL SELECT 'jobs', count(*) FROM jobs WHERE user_id = :owner "
        "UNION ALL SELECT 'operations', count(*) FROM operations WHERE owner_id = :owner"
    )
    async with harness.sessions() as session:
        rows = (await session.execute(query, {"owner": owner_id})).all()
    counts: dict[str, int] = {}
    for name, count in rows:
        assert isinstance(count, int)
        counts[str(name)] = count
    return counts


@pytest.mark.integration
@pytest.mark.anyio
async def test_deletes_complete_resource_graph_retains_receipt_and_recreates_identity(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    alice_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    bob_id = UUID((await harness.client.get("/v1/users/me", headers=BOB)).json()["data"]["userId"])
    alice_agent, bob_agent = await create_agent(harness), await create_agent(harness, BOB)
    await create_memory(harness, "Alice user memory")
    alice_turn = await chat(harness, alice_agent, "Alice remembered turn")
    cid = alice_turn["conversationId"]
    conversation_memory = await create_memory(
        harness, "Alice conversation memory", conversation_id=cid
    )
    await chat(harness, alice_agent, "Alice recalls both scopes", conversation_id=cid)
    bob_memory = await create_memory(harness, "Bob retained memory", headers=BOB)
    bob_turn = await chat(harness, bob_agent, "Bob retained conversation", headers=BOB)
    harness.memory.defer_delete = True
    deleted_memory = await harness.client.delete(
        f"/v1/memories/{conversation_memory}", headers=ALICE
    )
    assert deleted_memory.status_code == 202, deleted_memory.text
    old_operation = deleted_memory.json()["data"]["operationId"]
    snapshots = MemorySnapshotStore(session_factory=harness.sessions)
    alice_records = [
        record for record in harness.memory.records.values() if record.user_id == alice_id
    ]
    page = await snapshots.create_first_page(
        scope_key=MemoryListScopeKey(user_id=alice_id, provider_id=harness.memory.provider_id),
        records=alice_records,
        limit=1,
    )
    assert page.has_more
    before = await resource_counts(harness, alice_id)
    assert not [name for name, count in before.items() if count == 0]
    bob_before = await resource_counts(harness, bob_id)

    accepted = await accept_deletion(harness)
    assert (await harness.client.get("/v1/users/me", headers=ALICE)).status_code == 403
    during = await resource_counts(harness, alice_id)
    assert during["users"] == during["identities"] == 1
    operation = await harness.finish(accepted)
    assert operation["progress"]["local_complete"] is True
    assert operation["progress"]["remote_complete"] is True
    assert operation["result"] == {"deleted": True}
    assert all(count == 0 for count in (await resource_counts(harness, alice_id)).values())
    assert not any(record.user_id == alice_id for record in harness.memory.records.values())
    assert bob_memory in harness.memory.records
    bob_after = await resource_counts(harness, bob_id)
    assert all(bob_after[name] == bob_before[name] for name in bob_before if name != "jobs")
    history = await harness.client.get(
        f"/v1/conversations/{bob_turn['conversationId']}", headers=BOB
    )
    assert history.status_code == 200
    async with harness.sessions() as session:
        remaining = (
            await session.execute(
                text(
                    "SELECT owner_id, target_id, job_id, deletion_receipt_digest FROM operations "
                    "WHERE id = :operation"
                ),
                {"operation": UUID(accepted["operationId"])},
            )
        ).one()
        assert remaining.owner_id is None and remaining.target_id is None
        assert remaining.deletion_receipt_digest != accepted["deletionReceipt"]
        parent = (
            await session.execute(
                text("SELECT user_id, status FROM jobs WHERE id = :job"), {"job": remaining.job_id}
            )
        ).one()
        assert parent.user_id is None and parent.status == "succeeded"
        assert (
            await session.scalar(
                text("SELECT count(*) FROM operations WHERE id = :operation"),
                {"operation": UUID(old_operation)},
            )
            == 0
        )
    recreated = await harness.client.get("/v1/users/me", headers=ALICE)
    assert recreated.status_code == 200
    assert UUID(recreated.json()["data"]["userId"]) != alice_id
    assert (await harness.operation(accepted))["status"] == "succeeded"
    user_delete_events = [
        event
        for event in harness.audit_archiver.events
        if event.operation_id == UUID(accepted["operationId"])
        and event.operation_type == JOB_TYPE_USER_DELETE
    ]
    assert {event.event_type for event in user_delete_events} >= {
        "operation_accepted",
        "operation_transitioned",
    }
    assert any(
        event.event_type == "operation_transitioned" and event.status == "succeeded"
        for event in user_delete_events
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_waits_for_live_memory_write_and_clears_its_late_remote_record(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    pending = asyncio.create_task(
        harness.client.post(
            "/v1/memories", headers=ALICE, json={"content": "gated", "scope": "user"}
        )
    )
    await asyncio.wait_for(harness.memory.started.wait(), timeout=5)
    try:
        accepted = await accept_deletion(harness)
        worker = harness.worker()
        now = datetime.now(UTC)
        for step in range(4):
            await harness.run(worker, now=now + timedelta(seconds=2 * (step + 1)))
        operation = await harness.operation(accepted)
        assert operation["status"] == "reconciling"
        assert operation["progress"]["remote_complete"] is False
        assert harness.memory.clear_calls == []
        counts = await resource_counts(harness, user_id)
        assert counts["users"] == counts["identities"] == 1
    finally:
        harness.memory.release.set()
    response = await asyncio.wait_for(pending, timeout=5)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == "user_deleting"
    assert any(record.user_id == user_id for record in harness.memory.records.values())
    assert (await harness.finish(accepted))["status"] == "succeeded"
    assert not any(record.user_id == user_id for record in harness.memory.records.values())
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_waits_for_live_generation_and_rejects_its_final_commit(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    agent_id = await create_agent(harness)
    turn = await chat(harness, agent_id, "Before user deletion")
    started, release, _ = harness.model.gate("Generation crossing user deletion")
    pending = asyncio.create_task(
        harness.client.post(
            "/v1/chat",
            headers={**ALICE, "Idempotency-Key": "Generation crossing user deletion"},
            json={
                "agentId": agent_id,
                "conversationId": turn["conversationId"],
                "message": "Generation crossing user deletion",
            },
        )
    )
    await asyncio.wait_for(started.wait(), timeout=5)
    try:
        accepted = await accept_deletion(harness)
        worker = harness.worker()
        now = datetime.now(UTC)
        for step in range(5):
            await harness.run(worker, now=now + timedelta(seconds=2 * (step + 1)))
        operation = await harness.operation(accepted)
        assert operation["status"] == "reconciling"
        assert harness.memory.clear_calls == []
        async with harness.sessions() as session:
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM generations WHERE status = 'running' "
                        "AND conversation_id = :conversation AND lease_expires_at > now()"
                    ),
                    {"conversation": UUID(turn["conversationId"])},
                )
                == 1
            )
            assert await session.scalar(text("SELECT count(*) FROM user_identities")) == 1
    finally:
        release.set()
    response = await asyncio.wait_for(pending, timeout=5)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == "user_deleting"
    assert (await harness.finish(accepted))["status"] == "succeeded"


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_code", ["memory_provider_auth_error", "memory_provider_unavailable"]
)
async def test_remote_failure_retains_identity_and_resumes_after_controlled_retry(
    deletion_harness: UserDeletionHarness, error_code: str
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    agent_id = await create_agent(harness)
    await chat(harness, agent_id, "Local data cleared even when remote fails")
    memory_id = await create_memory(harness, "Remote deletion must be confirmed")
    snapshots = MemorySnapshotStore(session_factory=harness.sessions)
    await snapshots.create_first_page(
        scope_key=MemoryListScopeKey(user_id=user_id, provider_id=harness.memory.provider_id),
        records=[record for record in harness.memory.records.values() if record.user_id == user_id],
        limit=1,
    )
    harness.memory.clear_error_code = error_code
    accepted = await accept_deletion(harness)
    worker = harness.worker()
    now = datetime.now(UTC)
    failed_job = None
    for step in range(20):
        await harness.run(worker, now=now + timedelta(minutes=step + 1))
        async with harness.sessions() as session:
            failed_job = (
                await session.execute(
                    text(
                        "SELECT id, metadata_json AS metadata, attempt_count, max_attempts "
                        "FROM jobs "
                        "WHERE user_id = :owner AND job_type = 'memory_clear' AND status = 'failed'"
                    ),
                    {"owner": user_id},
                )
            ).one_or_none()
        if failed_job is not None:
            break
    assert failed_job is not None
    stopped_calls = len(harness.memory.clear_calls)
    assert stopped_calls >= 1
    if error_code == "memory_provider_unavailable":
        assert failed_job.attempt_count == failed_job.max_attempts
    assert failed_job.metadata["clear_phase"] == "execute_final_clear"
    async with harness.sessions() as session:
        child = (
            await session.execute(
                text("SELECT status, progress FROM operations WHERE id = :operation"),
                {"operation": UUID(failed_job.metadata["operation_id"])},
            )
        ).one()
        assert child.status == "reconciling"
        assert child.progress["phase"] == "awaiting_retry"
    restarted = harness.worker()
    stalled_check_at = now + timedelta(hours=1)
    await harness.run(restarted, now=stalled_check_at)
    await harness.run(restarted, now=stalled_check_at + timedelta(seconds=2))
    assert len(harness.memory.clear_calls) == stopped_calls
    assert memory_id in harness.memory.records
    retained = await resource_counts(harness, user_id)
    assert retained["users"] == retained["identities"] == 1
    assert retained["conversations"] == 0 and retained["agents"] == 0
    assert retained["snapshots"] == 0
    assert retained["scopes"] >= 1 and retained["operations"] >= 2
    stalled_op = await harness.operation(accepted)
    assert stalled_op["status"] == "reconciling"
    assert stalled_op["progress"]["local_complete"] is True
    assert stalled_op["progress"]["remote_complete"] is False
    assert stalled_op["progress"]["phase"] == "awaiting_retry"
    assert stalled_op["progress"]["retry_required"] is True
    assert stalled_op["error"] is not None
    assert "provider_operations" not in stalled_op["progress"]
    async with harness.sessions() as session:
        parent_next_run = await session.scalar(
            text(
                "SELECT next_run_at FROM jobs "
                "WHERE id = (SELECT job_id FROM operations WHERE id = :op)"
            ),
            {"op": UUID(accepted["operationId"])},
        )
    assert parent_next_run is not None
    assert (parent_next_run - stalled_check_at).total_seconds() >= 59.0
    assert (await harness.client.get("/v1/users/me", headers=ALICE)).status_code == 403

    harness.memory.clear_error_code = None
    await jobs_service.requeue_failed_job(
        harness.sessions,
        job_id=failed_job.id,
        reason="Provider credentials and availability restored",
    )
    assert (await harness.finish(accepted, worker=harness.worker(), now=now + timedelta(hours=1)))[
        "status"
    ] == "succeeded"
    assert user_id not in [record.user_id for record in harness.memory.records.values()]
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_partial_remote_clear_and_batched_finalization_survive_worker_restarts(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    memories = [await create_memory(harness, f"Partial clear record {index}") for index in range(3)]
    harness.memory.defer_delete = True
    for memory_id in memories:
        response = await harness.client.delete(f"/v1/memories/{memory_id}", headers=ALICE)
        assert response.status_code == 202, response.text
    harness.memory.defer_delete = False
    harness.memory.defer_clear = True
    harness.memory.partial_clear = True
    accepted = await accept_deletion(harness)
    now = datetime.now(UTC)
    for step in range(9):
        await harness.run(harness.worker(batch_size=1), now=now + timedelta(minutes=step + 1))
    pending = await harness.operation(accepted)
    assert pending["status"] == "reconciling"
    assert pending["progress"]["remote_complete"] is False
    assert len(harness.memory.clear_calls) == 1
    assert any(record.user_id == user_id for record in harness.memory.records.values())
    assert (await resource_counts(harness, user_id))["identities"] == 1
    async with harness.sessions() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM operations WHERE owner_id = :owner "
                    "AND operation_type = 'memory_clear'"
                ),
                {"owner": user_id},
            )
            == 1
        )

    harness.memory.allow_clear_verification = True
    saw_batched_finalization = False
    for step in range(30):
        await harness.run(harness.worker(batch_size=1), now=now + timedelta(minutes=step + 10))
        operation = await harness.operation(accepted)
        if operation["progress"].get("phase") == "finalizing_identity":
            saw_batched_finalization = True
            counts = await resource_counts(harness, user_id)
            assert counts["users"] == counts["identities"] == 1
            assert counts["operations"] > 1
        if operation["status"] == "succeeded":
            break
    assert operation["status"] == "succeeded"
    assert saw_batched_finalization
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())
    assert len(harness.memory.clear_calls) == 1
    assert not any(record.user_id == user_id for record in harness.memory.records.values())
    await harness.run(harness.worker(batch_size=1), now=now + timedelta(hours=2))
    assert (await harness.operation(accepted))["status"] == "succeeded"


@pytest.mark.integration
@pytest.mark.anyio
async def test_completed_ordinary_clear_is_not_reused_for_user_deletion(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    await create_memory(harness, "Before ordinary clear")
    cleared = await harness.client.delete("/v1/memories?confirm=true", headers=ALICE)
    assert cleared.status_code == 204, cleared.text
    async with harness.sessions() as session:
        previous_clear = await session.scalar(
            text(
                "SELECT id FROM operations WHERE owner_id = :owner "
                "AND operation_type = 'memory_clear' AND status = 'succeeded'"
            ),
            {"owner": user_id},
        )
    assert previous_clear is not None
    fresh_memory = await create_memory(harness, "Written after ordinary clear")
    accepted = await accept_deletion(harness)
    await harness.run(harness.worker(), now=datetime.now(UTC) + timedelta(minutes=1))
    initial = await harness.operation(accepted)
    assert "provider_operations" not in initial["progress"]
    async with harness.sessions() as session:
        parent_meta = await session.scalar(
            text(
                "SELECT metadata_json FROM jobs "
                "WHERE id = (SELECT job_id FROM operations WHERE id = :op)"
            ),
            {"op": UUID(accepted["operationId"])},
        )
    assert isinstance(parent_meta, dict)
    assert parent_meta["provider_operations"][harness.memory.provider_id] != str(previous_clear)
    assert (await harness.finish(accepted))["status"] == "succeeded"
    assert harness.memory.clear_calls == [user_id, user_id]
    assert fresh_memory not in harness.memory.records
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_unknown_write_pages_retain_timeout_and_require_delayed_compensation(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    harness.memory.supports_write_status_verification = False
    harness.memory.unknown_writes = True
    harness.memory.late_record_after_first_clear = True
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    for index in range(51):
        response = await harness.client.post(
            "/v1/memories",
            headers=ALICE,
            json={"content": f"Unknown write {index}", "scope": "user"},
        )
        assert response.status_code == 202, response.text
    async with harness.sessions() as session:
        old_writes = list(
            (
                await session.scalars(
                    text(
                        "SELECT id FROM jobs WHERE user_id = :owner "
                        "AND job_type = 'memory_explicit_create' "
                        "ORDER BY id"
                    ),
                    {"owner": user_id},
                )
            ).all()
        )
    assert len(old_writes) == 51
    accepted = await accept_deletion(harness)
    selected = (JOB_TYPE_USER_DELETE, "memory_clear")
    now = datetime.now(UTC)
    await harness.run(harness.worker(job_types=selected), now=now + timedelta(seconds=1))

    async def clear_metadata() -> dict[str, Any]:
        async with harness.sessions() as session:
            metadata = await session.scalar(
                text(
                    "SELECT metadata_json FROM jobs WHERE user_id = :owner "
                    "AND job_type = 'memory_clear'"
                ),
                {"owner": user_id},
            )
        assert isinstance(metadata, dict)
        return metadata

    await harness.run(harness.worker(job_types=selected), now=now + timedelta(minutes=2))
    first_page = await clear_metadata()
    assert first_page["inflight_scan_after_id"] == str(old_writes[49])
    assert first_page["settle_timeout_exceeded"] is True
    assert harness.memory.clear_calls == []
    await harness.run(harness.worker(job_types=selected), now=now + timedelta(seconds=121))
    last_page = await clear_metadata()
    assert last_page["inflight_scan_after_id"] == str(old_writes[-1])
    assert last_page["settle_timeout_exceeded"] is True
    assert set(last_page["unsettled_audit_job_ids"]) == {str(job_id) for job_id in old_writes}
    assert harness.memory.clear_calls == []
    await harness.run(harness.worker(job_types=selected), now=now + timedelta(seconds=122))
    after_clear = await clear_metadata()
    assert after_clear["clear_phase"] == "compensation_clear"
    assert after_clear["settle_timeout_exceeded"] is True
    assert set(after_clear["unsettled_audit_job_ids"]) == {str(job_id) for job_id in old_writes}
    assert harness.memory.clear_calls == [user_id]
    assert any(record.user_id == user_id for record in harness.memory.records.values())
    assert (await harness.operation(accepted))["progress"]["remote_complete"] is False
    await harness.run(harness.worker(job_types=selected), now=now + timedelta(seconds=123))
    assert harness.memory.clear_calls == [user_id]
    assert (await resource_counts(harness, user_id))["identities"] == 1
    assert (
        await harness.finish(
            accepted, worker=harness.worker(job_types=selected), now=now + timedelta(minutes=3)
        )
    )["status"] == "succeeded"
    assert harness.memory.clear_calls == [user_id, user_id]
    assert not any(record.user_id == user_id for record in harness.memory.records.values())
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_expired_generation_is_recovered_before_user_cleanup_finishes(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    agent_id = await create_agent(harness)
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    prepared = await conversations_service.prepare_chat_turn(
        harness.sessions,
        owner_id=user_id,
        agent_id=UUID(agent_id),
        conversation_id=None,
        user_message="Abandoned generation",
        idempotency_key="expired-before-deletion",
        memory_provider=harness.memory,
        options=ChatOptions(lease_ttl_seconds=30, lease_renew_interval_seconds=None),
        now=datetime.now(UTC) - timedelta(minutes=2),
    )
    assert isinstance(prepared, PreparedGeneration)
    async with harness.sessions() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM generations WHERE id = :generation "
                    "AND status = 'running' AND lease_expires_at < now()"
                ),
                {"generation": prepared.generation_id},
            )
            == 1
        )
    accepted = await accept_deletion(harness)
    await harness.run(harness.worker(), now=datetime.now(UTC) + timedelta(minutes=1))
    async with harness.sessions() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM generations WHERE id = :generation AND status = 'running'"
                ),
                {"generation": prepared.generation_id},
            )
            == 0
        )
    assert (await harness.finish(accepted))["status"] == "succeeded"
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_explicit_write_confirmed_mid_deletion_does_not_trigger_false_timeout(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    harness.memory.supports_write_status_verification = False
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    pending_write = asyncio.create_task(
        harness.client.post(
            "/v1/memories",
            headers=ALICE,
            json={"content": "gated", "scope": "user"},
        )
    )
    await asyncio.wait_for(harness.memory.started.wait(), timeout=5)
    try:
        accepted = await accept_deletion(harness)
    finally:
        harness.memory.release.set()
    write_response = await asyncio.wait_for(pending_write, timeout=5)
    assert write_response.status_code == 403, write_response.text
    assert write_response.json()["code"] == "user_deleting"

    async with harness.sessions() as session:
        write_meta = await session.scalar(
            text(
                "SELECT metadata_json FROM jobs "
                "WHERE user_id = :owner AND job_type = 'memory_explicit_create'"
            ),
            {"owner": user_id},
        )
    assert isinstance(write_meta, dict)
    assert write_meta["dispatch_state"] == "confirmed_during_clear"

    now = datetime.now(UTC)
    selected = (JOB_TYPE_USER_DELETE, "memory_clear")
    for step in range(8):
        await harness.run(harness.worker(job_types=selected), now=now + timedelta(seconds=step * 2))
        if (await harness.operation(accepted))["status"] == "succeeded":
            break
    assert (await harness.operation(accepted))["status"] == "succeeded"
    assert harness.memory.clear_calls == [user_id]
    assert not any(record.user_id == user_id for record in harness.memory.records.values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_superseded_conversation_delete_waits_for_remote_confirmation_and_purges_snapshots(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    agent_id = await create_agent(harness)
    turn = await chat(harness, agent_id, "Conversation joining user deletion")
    conversation_id = turn["conversationId"]
    conv_memory = await create_memory(
        harness, "Conversation-scoped memory", conversation_id=conversation_id
    )
    snapshots = MemorySnapshotStore(session_factory=harness.sessions)
    await snapshots.create_first_page(
        scope_key=MemoryListScopeKey(user_id=user_id, provider_id=harness.memory.provider_id),
        records=[record for record in harness.memory.records.values() if record.user_id == user_id],
        limit=1,
    )
    conv_delete = await harness.client.delete(f"/v1/conversations/{conversation_id}", headers=ALICE)
    assert conv_delete.status_code == 202, conv_delete.text
    conv_op_id = UUID(conv_delete.json()["data"]["operationId"])

    harness.memory.clear_error_code = "memory_provider_unavailable"
    accepted = await accept_deletion(harness)
    worker = harness.worker()
    now = datetime.now(UTC)
    failed_clear_job_id: UUID | None = None
    for step in range(20):
        await harness.run(worker, now=now + timedelta(minutes=step + 1))
        async with harness.sessions() as session:
            failed_clear_job_id = await session.scalar(
                text(
                    "SELECT id FROM jobs "
                    "WHERE user_id = :owner AND job_type = 'memory_clear' AND status = 'failed'"
                ),
                {"owner": user_id},
            )
        if failed_clear_job_id is not None:
            break
    assert failed_clear_job_id is not None

    # Local cleanup (conversations, agents, cached list snapshots) completes before remote settles.
    await harness.run(worker, now=now + timedelta(hours=1))
    retained = await resource_counts(harness, user_id)
    assert retained["conversations"] == 0
    assert retained["agents"] == 0
    assert retained["snapshots"] == 0
    assert retained["users"] == retained["identities"] == 1
    assert retained["scopes"] >= 1
    assert conv_memory in harness.memory.records

    # Superseded conversation_delete must stay reconciling until remote deletion is confirmed.
    async with harness.sessions() as session:
        conv_op = (
            await session.execute(
                text("SELECT status, progress FROM operations WHERE id = :op"),
                {"op": conv_op_id},
            )
        ).one()
    assert conv_op.status == "reconciling"
    assert conv_op.progress["local_complete"] is True
    assert conv_op.progress["remote_complete"] is False

    user_op = await harness.operation(accepted)
    assert user_op["status"] == "reconciling"
    assert user_op["progress"]["local_complete"] is True
    assert user_op["progress"]["remote_complete"] is False

    # Restore provider and requeue the failed clear job; both operations converge cleanly.
    harness.memory.clear_error_code = None
    await jobs_service.requeue_failed_job(
        harness.sessions,
        job_id=failed_clear_job_id,
        reason="Provider recovered during conversation_delete + user_delete race",
    )
    final_op = await harness.finish(accepted, worker=harness.worker(), now=now + timedelta(hours=1))
    assert final_op["status"] == "succeeded"
    assert conv_memory not in harness.memory.records
    assert all(count == 0 for count in (await resource_counts(harness, user_id)).values())
    async with harness.sessions() as session:
        assert (
            await session.scalar(
                text("SELECT count(*) FROM operations WHERE id = :op"),
                {"op": conv_op_id},
            )
            == 0
        )
