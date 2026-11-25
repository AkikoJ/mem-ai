"""Application-owned background job runner lifecycle behavior."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select, update

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import MemoryDeleteResult, MemoryOperationStatus
from mem_ai.contracts.model_provider import ModelResponse
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.conversations.models import Generation, IdempotencyRecord
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.workers.runner import WorkerRunner
from tests.conftest import FakeMemoryProvider, FakeModelProvider


async def _wait_until(predicate: Callable[[], bool], *, attempts: int = 500) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.01)
    assert predicate()


class _BlockingVerifyMemoryProvider(FakeMemoryProvider):
    def __init__(self, *, resist_cancellation: bool = False) -> None:
        super().__init__()
        self.resist_cancellation = resist_cancellation
        self.verify_started = asyncio.Event()
        self.verify_cancelled = asyncio.Event()
        self.release_verify = asyncio.Event()
        self.release_cancellation = asyncio.Event()

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        _ = user_id
        return MemoryDeleteResult(
            status="reconciling",
            external_ref=f"delete_memory:{memory_id}",
        )

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        _ = user_id
        self.verify_started.set()
        try:
            await self.release_verify.wait()
        except asyncio.CancelledError:
            self.verify_cancelled.set()
            if self.resist_cancellation:
                await self.release_cancellation.wait()
            raise
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


class _CountingReconcileMemoryProvider(FakeMemoryProvider):
    def __init__(self) -> None:
        super().__init__()
        self.delete_calls = 0
        self.verify_calls = 0

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        _ = user_id
        self.delete_calls += 1
        return MemoryDeleteResult(
            status="reconciling",
            external_ref=f"delete_memory:{memory_id}",
        )

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        _ = user_id
        self.verify_calls += 1
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


class _ReconcilingClearMemoryProvider(FakeMemoryProvider):
    def __init__(self) -> None:
        super().__init__()
        self.clear_calls_by_user: dict[UUID, int] = {}
        self.verify_calls_by_user: dict[UUID, int] = {}

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        self.clear_calls_by_user[user_id] = self.clear_calls_by_user.get(user_id, 0) + 1
        return MemoryDeleteResult(
            status="reconciling",
            external_ref=f"clear_memories:{user_id}",
        )

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        self.verify_calls_by_user[user_id] = self.verify_calls_by_user.get(user_id, 0) + 1
        for memory_id, memory in tuple(self.records.items()):
            if memory.user_id == user_id:
                del self.records[memory_id]
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


class _BlockingGenerationModelProvider(FakeModelProvider):
    def __init__(self) -> None:
        super().__init__(prefix="assistant:")
        self.block_started = asyncio.Event()
        self.release_block = asyncio.Event()

    async def generate(self, request) -> ModelResponse:
        if request.messages[-1].content == "Hold for lease recovery":
            self.block_started.set()
            await self.release_block.wait()
        return await super().generate(request)


async def _wait_for_successful_operation(
    client: httpx.AsyncClient,
    *,
    status_url: str,
    headers: dict[str, str],
    attempts: int = 1500,
) -> dict[str, object]:
    for _ in range(attempts):
        response = await client.get(status_url, headers=headers)
        if response.status_code == 200 and response.json()["data"]["status"] == "succeeded":
            return response.json()["data"]
        await asyncio.sleep(0.01)
    pytest.fail(f"operation at {status_url} did not succeed")


@pytest.mark.integration
@pytest.mark.anyio
async def test_application_worker_persists_turn_memory_after_chat(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_shutdown_grace_seconds": 0.5,
        }
    )
    memory_provider = FakeMemoryProvider()
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=FakeModelProvider(prefix="assistant:"),
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="worker-lifecycle-chat",
                    provider_id="openai_compatible",
                    upstream_model="worker-lifecycle-chat",
                    display_name="Worker Lifecycle Chat",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            agent_response = await client.post(
                "/v1/agents",
                headers=headers,
                json={
                    "name": "Lifecycle Memory Agent",
                    "systemPrompt": "Remember useful details from the conversation.",
                    "defaultModelId": "worker-lifecycle-chat",
                    "memoryPolicy": {
                        "enabled": True,
                        "writeEnabled": True,
                        "writeScope": "user",
                        "recallLimit": 5,
                    },
                },
            )
            assert agent_response.status_code == 201
            agent_id = agent_response.json()["data"]["agentId"]
            published = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
            assert published.status_code == 200

            chat_response = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": "embedded-worker-memory-write"},
                json={
                    "agentId": agent_id,
                    "message": "The launch code is Silver Birch.",
                },
            )
            assert chat_response.status_code == 200

            await _wait_until(lambda: bool(memory_provider.records))

            assert len(memory_provider.records) == 1
            memory = next(iter(memory_provider.records.values()))
            assert memory.content and "The launch code is Silver Birch." in memory.content
            listed = await client.get("/v1/memories", headers=headers)
            assert listed.status_code == 200
            assert len(listed.json()["data"]["items"]) == 1


@pytest.mark.integration
@pytest.mark.anyio
async def test_disabled_worker_loop_keeps_runner_available_for_manual_step(
    migrated_pg_settings,
    caplog,
    request,
) -> None:
    settings = migrated_pg_settings.model_copy(update={"worker_loop_enabled": False})
    app = create_app(settings)
    worker_logger = logging.getLogger("mem_ai.workers.runner")
    worker_logger.addHandler(caplog.handler)
    request.addfinalizer(lambda: worker_logger.removeHandler(caplog.handler))

    async with app.router.lifespan_context(app):
        runner = app.state.worker_runner

        assert runner.config.concurrency == settings.worker_concurrency
        assert runner.telemetry is app.state.telemetry
        assert app.state.worker_task is None
        assert await runner.run_once(max_jobs=1) == 1
        assert runner.health_snapshot().job_backlog_count == 1
        assert runner.health_snapshot().pending_reconciliation_count == 0
        heartbeat_fields = [
            getattr(record, "extra_fields", None)
            for record in caplog.records
            if record.getMessage() == "Worker heartbeat"
        ]
        assert any(
            fields is not None
            and fields.get("job_backlog_count") == 1
            and fields.get("pending_reconciliation_count") == 0
            for fields in heartbeat_fields
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            metrics = await client.get("/metrics")
        assert metrics.status_code == 200
        assert "mem_ai_worker_last_heartbeat_timestamp_seconds" in metrics.text
        assert "mem_ai_jobs_backlog 1" in metrics.text
        assert "mem_ai_jobs_reconciling 0" in metrics.text
        assert "mem_ai_worker_job_backlog_count" not in metrics.text
        assert "mem_ai_worker_pending_reconciliation_count" not in metrics.text

    assert app.state.worker_runner.health_snapshot().shutting_down is True


@pytest.mark.integration
@pytest.mark.anyio
async def test_enabled_worker_loop_starts_and_stops_with_application_lifespan(
    migrated_pg_settings,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_shutdown_grace_seconds": 0.5,
        }
    )
    app = create_app(settings)

    async with app.router.lifespan_context(app):
        runner = app.state.worker_runner
        worker_task = app.state.worker_task

        assert worker_task is not None
        assert runner.telemetry is app.state.telemetry
        assert app.state.ready is True

        await _wait_until(lambda: runner.health_snapshot().last_poll_at is not None, attempts=100)
        assert runner.health_snapshot().last_poll_at is not None

        await _wait_until(lambda: runner.health_snapshot().succeeded_count > 0)
        assert runner.health_snapshot().succeeded_count > 0

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            ready = await client.get("/health/ready")
            metrics = await client.get("/metrics")
        assert ready.status_code == 200
        assert metrics.status_code == 200
        assert "mem_ai_worker_jobs_processed_total" in metrics.text

    assert worker_task.done()
    assert runner.health_snapshot().shutting_down is True
    assert app.state.ready is False

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        not_ready = await client.get("/health/ready")
    assert not_ready.status_code == 503


@pytest.mark.integration
@pytest.mark.anyio
async def test_two_application_lifespans_share_periodic_job_deduplication(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_reconcile_delay_seconds": 0.1,
        }
    )
    memory_provider = _CountingReconcileMemoryProvider()
    providers = ProviderRegistry(memory_provider=memory_provider)
    apps = (
        create_app(settings, providers=providers, credential_verifier=test_auth_adapter),
        create_app(settings, providers=providers, credential_verifier=test_auth_adapter),
    )

    async with (
        apps[0].router.lifespan_context(apps[0]),
        apps[1].router.lifespan_context(apps[1]),
    ):
        await _wait_until(
            lambda: (
                sum(app.state.worker_runner.health_snapshot().succeeded_count for app in apps) >= 2
            )
        )
        processed_count = sum(
            app.state.worker_runner.health_snapshot().succeeded_count for app in apps
        )

        assert processed_count == 2
        assert all(app.state.ready for app in apps)

        transport = httpx.ASGITransport(app=apps[0])
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            created = await client.post(
                "/v1/memories",
                headers=headers,
                json={"content": "Delete this memory once"},
            )
            assert created.status_code == 201
            memory_id = created.json()["data"]["memoryId"]
            deleted = await client.delete(f"/v1/memories/{memory_id}", headers=headers)
            assert deleted.status_code == 202

        await _wait_until(
            lambda: (
                memory_provider.verify_calls == 1
                and sum(app.state.worker_runner.health_snapshot().succeeded_count for app in apps)
                >= 3
            )
        )
        processed_count = sum(
            app.state.worker_runner.health_snapshot().succeeded_count for app in apps
        )

        assert processed_count == 3
        assert memory_provider.delete_calls == 1
        assert memory_provider.verify_calls == 1

    assert all(app.state.worker_task.done() for app in apps)


@pytest.mark.integration
@pytest.mark.anyio
async def test_application_worker_clears_memory_and_completes_conversation_and_user_deletion(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_reconcile_delay_seconds": 0.01,
            "deletion_receipt_secret": SecretStr("lifespan-deletion-test-key-at-least-32-chars"),
        }
    )
    memory_provider = _ReconcilingClearMemoryProvider()
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=FakeModelProvider(prefix="assistant:"),
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="worker-lifecycle-deletion",
                    provider_id="openai_compatible",
                    upstream_model="worker-lifecycle-deletion",
                    display_name="Worker Lifecycle Deletion",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice = {"Authorization": "Bearer token-alice"}
            bob = {"Authorization": "Bearer token-bob"}

            alice_memory = await client.post(
                "/v1/memories", headers=alice, json={"content": "Clear this memory"}
            )
            assert alice_memory.status_code == 201
            alice_memory_id = alice_memory.json()["data"]["memoryId"]
            alice_profile = await client.get("/v1/users/me", headers=alice)
            alice_id = UUID(alice_profile.json()["data"]["userId"])

            clear = await client.delete("/v1/memories?confirm=true", headers=alice)
            assert clear.status_code == 202
            await _wait_for_successful_operation(
                client,
                status_url=clear.json()["data"]["statusUrl"],
                headers=alice,
            )
            assert alice_memory_id not in memory_provider.records

            agent = await client.post(
                "/v1/agents",
                headers=alice,
                json={
                    "name": "Lifecycle Cleanup Agent",
                    "systemPrompt": "Be helpful.",
                    "defaultModelId": "worker-lifecycle-deletion",
                    "memoryPolicy": {"enabled": False, "writeEnabled": False},
                },
            )
            assert agent.status_code == 201
            agent_id = agent.json()["data"]["agentId"]
            published = await client.post(f"/v1/agents/{agent_id}/publish", headers=alice)
            assert published.status_code == 200
            chat = await client.post(
                "/v1/chat",
                headers={**alice, "Idempotency-Key": "lifecycle-conversation-delete"},
                json={"agentId": agent_id, "message": "Create a conversation to delete."},
            )
            assert chat.status_code == 200
            conversation_id = chat.json()["data"]["conversationId"]

            conversation_memory = await client.post(
                "/v1/memories",
                headers=alice,
                json={
                    "content": "Conversation memory to remove",
                    "scope": "conversation",
                    "conversationId": conversation_id,
                },
            )
            assert conversation_memory.status_code == 201
            conversation_memory_id = conversation_memory.json()["data"]["memoryId"]
            conversation_delete = await client.delete(
                f"/v1/conversations/{conversation_id}", headers=alice
            )
            assert conversation_delete.status_code == 202
            await _wait_for_successful_operation(
                client,
                status_url=conversation_delete.json()["data"]["statusUrl"],
                headers=alice,
            )
            assert conversation_memory_id not in memory_provider.records

            bob_memory = await client.post(
                "/v1/memories", headers=bob, json={"content": "Bob's memory to remove"}
            )
            assert bob_memory.status_code == 201
            bob_memory_id = bob_memory.json()["data"]["memoryId"]
            bob_profile = await client.get("/v1/users/me", headers=bob)
            bob_id = UUID(bob_profile.json()["data"]["userId"])
            user_delete = await client.delete("/v1/users/me", headers=bob)
            assert user_delete.status_code == 202
            receipt_headers = {
                "Authorization": f"DeletionReceipt {user_delete.json()['data']['deletionReceipt']}"
            }
            await _wait_for_successful_operation(
                client,
                status_url=user_delete.json()["data"]["statusUrl"],
                headers=receipt_headers,
                attempts=2500,
            )

            assert bob_memory_id not in memory_provider.records
            assert memory_provider.clear_calls_by_user[alice_id] == 1
            assert memory_provider.clear_calls_by_user[bob_id] == 1
            assert memory_provider.verify_calls_by_user[alice_id] >= 1
            assert memory_provider.verify_calls_by_user[bob_id] >= 1


@pytest.mark.integration
@pytest.mark.anyio
async def test_application_runner_recovers_expired_generation_and_runs_retention_periodic_job(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(update={"worker_loop_enabled": False})
    model_provider = _BlockingGenerationModelProvider()
    app = create_app(
        settings,
        providers=ProviderRegistry(model_provider=model_provider),
        credential_verifier=test_auth_adapter,
    )

    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="worker-lifecycle-recovery",
                    provider_id="openai_compatible",
                    upstream_model="worker-lifecycle-recovery",
                    display_name="Worker Lifecycle Recovery",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            agent = await client.post(
                "/v1/agents",
                headers=headers,
                json={
                    "name": "Lifecycle Recovery Agent",
                    "systemPrompt": "Be helpful.",
                    "defaultModelId": "worker-lifecycle-recovery",
                    "memoryPolicy": {"enabled": False, "writeEnabled": False},
                },
            )
            assert agent.status_code == 201
            agent_id = agent.json()["data"]["agentId"]
            published = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
            assert published.status_code == 200

            completed_chat = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": "lifecycle-expired-retention-record"},
                json={"agentId": agent_id, "message": "Keep this only until retention."},
            )
            assert completed_chat.status_code == 200
            async with app.state.session_factory() as session:
                expired_idempotency = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.idempotency_key == "lifecycle-expired-retention-record"
                    )
                )
            assert expired_idempotency is not None

            blocked_chat = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers={**headers, "Idempotency-Key": "lifecycle-expired-generation"},
                    json={"agentId": agent_id, "message": "Hold for lease recovery"},
                )
            )
            try:
                await asyncio.wait_for(model_provider.block_started.wait(), timeout=3.0)
                async with app.state.session_factory() as session:
                    running_generation = await session.scalar(
                        select(Generation).where(Generation.status == "running")
                    )
                assert running_generation is not None
                recovery_generation_id = running_generation.id

                current_time = datetime.now(UTC)
                async with app.state.session_factory.begin() as session:
                    await session.execute(
                        update(Generation)
                        .where(Generation.id == recovery_generation_id)
                        .values(lease_expires_at=current_time - timedelta(seconds=1))
                    )
                    await session.execute(
                        update(IdempotencyRecord)
                        .where(IdempotencyRecord.id == expired_idempotency.id)
                        .values(
                            terminal_at=current_time - timedelta(seconds=2),
                            expires_at=current_time - timedelta(seconds=1),
                        )
                    )

                runner = app.state.worker_runner
                assert app.state.worker_task is None
                assert (
                    await runner.run_once(now=current_time + timedelta(seconds=60), max_jobs=2) == 2
                )

                async with app.state.session_factory() as session:
                    generation_status = await session.scalar(
                        select(Generation.status).where(Generation.id == recovery_generation_id)
                    )
                    retained_record = await session.get(IdempotencyRecord, expired_idempotency.id)
                    periodic_jobs = list(
                        (
                            await session.scalars(
                                select(Job).where(
                                    Job.job_type.in_(
                                        ("generation_lease_recovery", "retention_cleanup")
                                    )
                                )
                            )
                        ).all()
                    )
                assert generation_status == "failed"
                assert retained_record is None
                assert {job.job_type: job.status for job in periodic_jobs} == {
                    "generation_lease_recovery": "succeeded",
                    "retention_cleanup": "succeeded",
                }
                assert runner.health_snapshot().succeeded_count == 2
            finally:
                model_provider.release_block.set()
                blocked_response = await asyncio.wait_for(blocked_chat, timeout=5.0)
            assert blocked_response.status_code in {409, 504}


@pytest.mark.integration
@pytest.mark.anyio
async def test_lifespan_shutdown_drains_manual_worker_step_before_closing_resources(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": False,
            "worker_shutdown_grace_seconds": 0.1,
        }
    )
    memory_provider = _BlockingVerifyMemoryProvider(resist_cancellation=True)
    app = create_app(
        settings,
        providers=ProviderRegistry(memory_provider=memory_provider),
        credential_verifier=test_auth_adapter,
    )
    lifespan_started = asyncio.Event()
    finish_lifespan = asyncio.Event()

    async def run_application_lifespan() -> None:
        async with app.router.lifespan_context(app):
            lifespan_started.set()
            await finish_lifespan.wait()

    app_task = asyncio.create_task(run_application_lifespan())
    step_task: asyncio.Task[int] | None = None
    try:
        await asyncio.wait_for(lifespan_started.wait(), timeout=3.0)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            created = await client.post(
                "/v1/memories",
                headers=headers,
                json={"content": "Delete this memory"},
            )
            assert created.status_code == 201
            memory_id = created.json()["data"]["memoryId"]
            deleted = await client.delete(f"/v1/memories/{memory_id}", headers=headers)
            assert deleted.status_code == 202

        runner = app.state.worker_runner
        step_task = asyncio.create_task(
            runner.run_once(now=datetime.now(UTC) + timedelta(seconds=60), max_jobs=1)
        )
        await asyncio.wait_for(memory_provider.verify_started.wait(), timeout=3.0)
        assert runner.health_snapshot().active_tasks == 1

        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            metrics = await client.get("/metrics")
        assert "mem_ai_worker_active_tasks 1" in metrics.text

        finish_lifespan.set()
        await asyncio.wait_for(app_task, timeout=3.0)
    finally:
        finish_lifespan.set()
        memory_provider.release_cancellation.set()
        if not app_task.done():
            await asyncio.wait_for(app_task, timeout=3.0)
        if step_task is not None and not step_task.done():
            await asyncio.wait_for(
                asyncio.gather(step_task, return_exceptions=True),
                timeout=3.0,
            )

    assert memory_provider.verify_cancelled.is_set()
    assert step_task is not None and step_task.cancelled()
    assert app.state.http_client.is_closed
    assert app.state.ready is False


@pytest.mark.integration
@pytest.mark.anyio
async def test_lifespan_shutdown_cancels_worker_poll_before_closing_resources(
    migrated_pg_settings,
    monkeypatch,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={"worker_loop_enabled": True, "worker_shutdown_grace_seconds": 0.1}
    )
    poll_started = asyncio.Event()
    poll_cancelled = asyncio.Event()

    async def block_periodic_schedule(
        _runner: WorkerRunner,
        *,
        now: datetime | None = None,
    ) -> int:
        _ = now
        poll_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            poll_cancelled.set()
            raise
        return 0

    monkeypatch.setattr(WorkerRunner, "schedule_periodic_jobs", block_periodic_schedule)
    app = create_app(settings)
    lifespan_started = asyncio.Event()
    finish_lifespan = asyncio.Event()

    async def run_application_lifespan() -> None:
        async with app.router.lifespan_context(app):
            lifespan_started.set()
            await finish_lifespan.wait()

    app_task = asyncio.create_task(run_application_lifespan())
    worker_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(lifespan_started.wait(), timeout=3.0)
        await asyncio.wait_for(poll_started.wait(), timeout=3.0)
        worker_task = app.state.worker_task
        assert worker_task is not None and not worker_task.done()

        finish_lifespan.set()
        await asyncio.wait_for(app_task, timeout=3.0)
    finally:
        finish_lifespan.set()
        if not app_task.done():
            await asyncio.wait_for(app_task, timeout=3.0)

    assert poll_cancelled.is_set()
    assert worker_task is not None and worker_task.done() and not worker_task.cancelled()
    assert app.state.http_client.is_closed
    assert app.state.ready is False


@pytest.mark.integration
@pytest.mark.anyio
async def test_saturated_worker_concurrency_does_not_starve_online_requests_or_overclaim(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_concurrency": 1,
            "db_pool_size": 1,
            "db_max_overflow": 1,
            "worker_poll_interval_seconds": 0.01,
            "worker_reconcile_delay_seconds": 0.01,
            "worker_shutdown_grace_seconds": 0.5,
        }
    )
    memory_provider = _BlockingVerifyMemoryProvider()
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=FakeModelProvider(prefix="assistant:"),
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )

    try:
        async with app.router.lifespan_context(app):
            await models_service.sync_model_configs(
                app.state.session_factory,
                seeds=[
                    ModelConfigSeed(
                        model_id="worker-isolation-chat",
                        provider_id="openai_compatible",
                        upstream_model="worker-isolation-chat",
                        display_name="Worker Isolation Chat",
                        enabled=True,
                        supports_streaming=True,
                    )
                ],
            )
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                headers = {"Authorization": "Bearer token-alice"}
                agent = await client.post(
                    "/v1/agents",
                    headers=headers,
                    json={
                        "name": "Isolation Agent",
                        "systemPrompt": "Be concise.",
                        "defaultModelId": "worker-isolation-chat",
                        "memoryPolicy": {"enabled": False, "writeEnabled": False},
                    },
                )
                assert agent.status_code == 201
                agent_id = agent.json()["data"]["agentId"]
                assert (
                    await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
                ).status_code == 200

                # Enqueue a burst of 2 background jobs against worker_concurrency=1.
                for idx in range(2):
                    created = await client.post(
                        "/v1/memories",
                        headers=headers,
                        json={"content": f"Burst delete memory {idx}"},
                    )
                    assert created.status_code == 201
                    memory_id = created.json()["data"]["memoryId"]
                    deleted = await client.delete(f"/v1/memories/{memory_id}", headers=headers)
                    assert deleted.status_code == 202

                await asyncio.wait_for(memory_provider.verify_started.wait(), timeout=3.0)
                runner = app.state.worker_runner
                assert runner.health_snapshot().active_tasks == 1
                # Overlapping run_once respects the same concurrency cap and does not over-claim.
                assert await runner.run_once(max_jobs=2) == 0
                assert runner.health_snapshot().active_tasks == 1

                ready = await client.get("/health/ready")
                chat = await client.post(
                    "/v1/chat",
                    headers={**headers, "Idempotency-Key": "online-chat-under-worker-burst"},
                    json={"agentId": agent_id, "message": "Still responsive under load."},
                )
                assert ready.status_code == 200
                assert chat.status_code == 200

                memory_provider.release_verify.set()
                await _wait_until(lambda: runner.health_snapshot().active_tasks == 0)
    finally:
        memory_provider.release_verify.set()


@pytest.mark.integration
@pytest.mark.anyio
async def test_lifespan_shutdown_drains_sync_generation_and_worker_loop_in_parallel(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_reconcile_delay_seconds": 0.01,
            "worker_shutdown_grace_seconds": 2.0,
        }
    )
    model_provider = _BlockingGenerationModelProvider()
    memory_provider = _BlockingVerifyMemoryProvider()
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
    )
    lifespan_started = asyncio.Event()
    finish_lifespan = asyncio.Event()

    async def run_application_lifespan() -> None:
        async with app.router.lifespan_context(app):
            lifespan_started.set()
            await finish_lifespan.wait()

    app_task = asyncio.create_task(run_application_lifespan())
    chat_task: asyncio.Task[httpx.Response] | None = None
    try:
        await asyncio.wait_for(lifespan_started.wait(), timeout=3.0)
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="worker-parallel-drain",
                    provider_id="openai_compatible",
                    upstream_model="worker-parallel-drain",
                    display_name="Worker Parallel Drain",
                    enabled=True,
                    supports_streaming=True,
                )
            ],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            agent = await client.post(
                "/v1/agents",
                headers=headers,
                json={
                    "name": "Parallel Drain Agent",
                    "systemPrompt": "Be helpful.",
                    "defaultModelId": "worker-parallel-drain",
                    "memoryPolicy": {"enabled": False, "writeEnabled": False},
                },
            )
            assert agent.status_code == 201
            agent_id = agent.json()["data"]["agentId"]
            assert (
                await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
            ).status_code == 200

            created = await client.post(
                "/v1/memories",
                headers=headers,
                json={"content": "Drain me during shutdown"},
            )
            assert created.status_code == 201
            memory_id = created.json()["data"]["memoryId"]
            deleted = await client.delete(f"/v1/memories/{memory_id}", headers=headers)
            assert deleted.status_code == 202

            chat_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers={**headers, "Idempotency-Key": "parallel-drain-sync-chat"},
                    json={"agentId": agent_id, "message": "Hold for lease recovery"},
                )
            )
            await asyncio.wait_for(model_provider.block_started.wait(), timeout=3.0)
            await asyncio.wait_for(memory_provider.verify_started.wait(), timeout=3.0)

            finish_lifespan.set()
            await _wait_until(lambda: app.state.ready is False)
            not_ready = await client.get("/health/ready")
            assert not_ready.status_code == 503

            # Both sync generation and embedded worker job should still be draining gracefully.
            assert not app_task.done()
            assert not memory_provider.verify_cancelled.is_set()

            model_provider.release_block.set()
            memory_provider.release_verify.set()
            await asyncio.wait_for(app_task, timeout=3.0)
            chat_response = await asyncio.wait_for(chat_task, timeout=3.0)
            assert chat_response.status_code == 200
    finally:
        finish_lifespan.set()
        model_provider.release_block.set()
        memory_provider.release_verify.set()
        if not app_task.done():
            await asyncio.wait_for(app_task, timeout=3.0)
        if chat_task is not None and not chat_task.done():
            await asyncio.wait_for(
                asyncio.gather(chat_task, return_exceptions=True),
                timeout=3.0,
            )

    assert not memory_provider.verify_cancelled.is_set()
    assert app.state.worker_runner.health_snapshot().succeeded_count >= 1
    assert app.state.worker_task is not None and app.state.worker_task.done()
    assert not app.state.worker_task.cancelled()

    check_engine = create_async_engine_from_settings(settings)
    try:
        check_session_factory = create_session_factory(check_engine)
        async with check_session_factory() as session:
            delete_job = await session.scalar(select(Job).where(Job.job_type == "memory_delete"))
        assert delete_job is not None
        assert delete_job.status == "succeeded"
    finally:
        await check_engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_lifespan_shutdown_timeout_keeps_running_job_lease_recoverable_and_stops_heartbeat(
    migrated_pg_settings,
    test_auth_adapter,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "worker_loop_enabled": True,
            "worker_poll_interval_seconds": 0.01,
            "worker_reconcile_delay_seconds": 0.01,
            "worker_lease_ttl_seconds": 5,
            "worker_lease_renew_interval_seconds": 0.05,
            "worker_shutdown_grace_seconds": 0.15,
        }
    )
    memory_provider = _BlockingVerifyMemoryProvider(resist_cancellation=True)
    app = create_app(
        settings,
        providers=ProviderRegistry(memory_provider=memory_provider),
        credential_verifier=test_auth_adapter,
    )
    lifespan_started = asyncio.Event()
    finish_lifespan = asyncio.Event()

    async def run_application_lifespan() -> None:
        async with app.router.lifespan_context(app):
            lifespan_started.set()
            await finish_lifespan.wait()

    app_task = asyncio.create_task(run_application_lifespan())
    try:
        await asyncio.wait_for(lifespan_started.wait(), timeout=3.0)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"Authorization": "Bearer token-alice"}
            created = await client.post(
                "/v1/memories",
                headers=headers,
                json={"content": "Keep lease recoverable on timeout"},
            )
            assert created.status_code == 201
            memory_id = created.json()["data"]["memoryId"]
            deleted = await client.delete(f"/v1/memories/{memory_id}", headers=headers)
            assert deleted.status_code == 202

        await asyncio.wait_for(memory_provider.verify_started.wait(), timeout=3.0)
        shutdown_started = time.monotonic()
        finish_lifespan.set()
        await asyncio.wait_for(app_task, timeout=2.0)
        elapsed = time.monotonic() - shutdown_started

        # Shutdown stays bounded within grace (not doubled) even for cancellation-resistant work.
        assert elapsed < 0.6
        assert memory_provider.verify_cancelled.is_set()
        assert app.state.ready is False
        assert app.state.http_client.is_closed

        # Verify persisted DB row stays `running` with recoverable lease and stops renewing.
        check_engine = create_async_engine_from_settings(settings)
        try:
            check_session_factory = create_session_factory(check_engine)
            async with check_session_factory() as session:
                job_before = await session.scalar(
                    select(Job).where(Job.job_type == "memory_delete")
                )
            assert job_before is not None
            assert job_before.status == "running"
            assert job_before.lease_token is not None
            assert job_before.lease_expires_at is not None

            await asyncio.sleep(0.12)
            async with check_session_factory() as session:
                job_after = await session.scalar(select(Job).where(Job.job_type == "memory_delete"))
            assert job_after is not None
            assert job_after.status == "running"
            assert job_after.lease_expires_at == job_before.lease_expires_at
        finally:
            await check_engine.dispose()
    finally:
        finish_lifespan.set()
        memory_provider.release_cancellation.set()
        if not app_task.done():
            await asyncio.wait_for(app_task, timeout=3.0)
