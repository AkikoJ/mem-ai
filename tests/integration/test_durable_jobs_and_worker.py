"""PostgreSQL integration tests for atomic turn job enqueue, `memory_jobs` view, and Worker."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

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
)
from mem_ai.contracts.persona_provider import MemoryPolicy
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    BadGatewayError,
    GatewayTimeoutError,
    ServiceUnavailableError,
)
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.queries import DatabaseTurnContentLoader
from mem_ai.modules.conversations.schemas import GenerationRecord, MessageRecord
from mem_ai.modules.conversations.service import SyncChatOptions
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    JobBackoffPolicy,
    JobExecutionContext,
    JobHandlerOutcome,
)
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.jobs import (
    JOB_TYPE_MEMORY_EXPLICIT_CREATE,
    JOB_TYPE_MEMORY_TURN_WRITE,
    ExplicitMemoryReconcileJobHandler,
)
from mem_ai.modules.memories.schemas import AcceptedMemoryOutcome
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.users import service as users_service
from mem_ai.workers.main import (
    JOB_TYPE_GENERATION_LEASE_RECOVERY,
    GenerationLeaseRecoveryJobHandler,
    build_worker_registry,
)
from mem_ai.workers.runner import (
    JobHandlerRegistry,
    PeriodicJobSpec,
    WorkerConfig,
    WorkerRunner,
)
from tests.conftest import FakeModelProvider


class _ScriptedMemoryProvider:
    """Controlled MemoryProvider scripting write & verify outcomes for Worker tests."""

    provider_id = "scripted-memory"

    def __init__(
        self,
        *,
        write_outcomes: list[MemoryWriteResult | Exception] | None = None,
        verify_outcomes: list[MemoryOperationStatus | Exception] | None = None,
        write_delay_seconds: float = 0.0,
    ) -> None:
        self._write_outcomes = list(write_outcomes or [])
        self._verify_outcomes = list(verify_outcomes or [])
        self._write_delay_seconds = write_delay_seconds
        self.writes: list[MemoryWriteInput] = []
        self.verifies: list[tuple[UUID, str]] = []
        self.write_started = asyncio.Event()
        self.release_write = asyncio.Event()
        self.gate_writes = False

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        _ = query
        return []

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self.write_turn_memory(item)

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        self.writes.append(item)
        self.write_started.set()
        if self.gate_writes:
            await self.release_write.wait()
        if self._write_delay_seconds > 0:
            await asyncio.sleep(self._write_delay_seconds)
        if self._write_outcomes:
            nxt = self._write_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt
        return MemoryWriteResult(
            status="succeeded",
            provider_id=self.provider_id,
            memory_id=f"mem_{len(self.writes)}",
            external_ref=item.write_id,
        )

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        _ = query
        return MemoryListPage(items=[], next_cursor=None, has_more=False)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        _ = user_id
        return MemoryDeleteResult(status="succeeded", external_ref=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        return MemoryDeleteResult(status="succeeded", external_ref=str(user_id))

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        self.verifies.append((user_id, external_ref))
        if self._verify_outcomes:
            nxt = self._verify_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt
        return MemoryOperationStatus(
            status="succeeded",
            external_ref=external_ref,
            memory_id=f"mem_verified_{len(self.verifies)}",
        )


class _FailingModelProvider:
    async def generate(self, request: ModelRequest) -> ModelResponse:
        _ = request
        raise BadGatewayError(code="model_upstream_error", message="upstream model failed")

    async def stream_generate(self, request: ModelRequest) -> Any:
        _ = request
        yield ModelStreamChunk(sequence=1, text="", is_final=True)


async def _seed_user_model_and_agent(
    session_factory: Any,
    *,
    subject: str,
    memory_policy: MemoryPolicy | None = None,
) -> tuple[UUID, UUID]:
    user = await users_service.resolve_active_user(
        session_factory,
        issuer="https://test.mem-ai.local",
        subject=subject,
    )
    await models_service.sync_model_configs(
        session_factory,
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
    policy = memory_policy if memory_policy is not None else MemoryPolicy()
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest.model_validate(
            {
                "name": f"Agent-{subject}",
                "systemPrompt": "You are a helpful assistant.",
                "defaultModelId": "test-model",
                "memoryPolicy": {
                    "enabled": policy.enabled,
                    "writeEnabled": policy.write_enabled,
                    "writeScope": policy.write_scope,
                    "recallLimit": policy.recall_limit,
                },
            }
        ),
    )
    await agents_service.publish_agent(
        session_factory,
        owner_id=user.id,
        agent_id=agent.agent_id,
    )
    return user.id, agent.agent_id


@pytest.mark.integration
@pytest.mark.anyio
async def test_atomic_tx2_commit_write_scopes_disabled_policy_and_rollback(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="reply:")
        memory_provider = _ScriptedMemoryProvider()

        # 1. Default writeScope='user' commits reply, generation, and turn job atomically.
        user_id, user_scope_agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="tx2-user-scope",
            memory_policy=MemoryPolicy(enabled=True, write_enabled=True, write_scope="user"),
        )
        res_user = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=memory_provider,
            owner_id=user_id,
            agent_id=user_scope_agent_id,
            conversation_id=None,
            user_message="Secret user fact: my token is tok_super_secret",
        )
        jobs_user = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(jobs_user) == 1
        job = jobs_user[0]
        assert job.status == "pending"
        assert job.business_id == str(res_user.generation_id)
        assert job.dedupe_key == f"{JOB_TYPE_MEMORY_TURN_WRITE}:{res_user.generation_id}"
        assert job.metadata_json["scope"] == "user"
        assert job.metadata_json["conversation_id"] == str(res_user.conversation_id)
        assert "Secret user fact" not in json.dumps(job.metadata_json)
        assert "tok_super_secret" not in json.dumps(job.metadata_json)

        # 2. writeScope='conversation' propagates through jobs and memory_jobs view.
        _, conv_scope_agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="tx2-user-scope",
            memory_policy=MemoryPolicy(
                enabled=True, write_enabled=True, write_scope="conversation"
            ),
        )
        res_conv = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=memory_provider,
            owner_id=user_id,
            agent_id=conv_scope_agent_id,
            conversation_id=None,
            user_message="Conversation-only scratch note",
        )
        view_rows = await jobs_service.list_memory_jobs(
            session_factory,
            user_id=user_id,
            conversation_id=res_conv.conversation_id,
        )
        assert len(view_rows) == 1
        assert view_rows[0].scope == "conversation"
        assert view_rows[0].generation_id == str(res_conv.generation_id)

        # 3. Disabled memory write commits reply and generation WITHOUT creating a job.
        _, disabled_agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="tx2-user-scope",
            memory_policy=MemoryPolicy(enabled=True, write_enabled=False, write_scope="user"),
        )
        res_disabled = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=memory_provider,
            owner_id=user_id,
            agent_id=disabled_agent_id,
            conversation_id=None,
            user_message="Do not memorize this turn",
        )
        gen_disabled = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=user_id,
            conversation_id=res_disabled.conversation_id,
            generation_id=res_disabled.generation_id,
        )
        assert gen_disabled.status == "succeeded"
        all_jobs_now = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(all_jobs_now) == 2

        # 4. Enqueue failure inside Tx 2 rolls back assistant message, succeeded status, and job.
        async def _boom_hook(
            session: AsyncSession,
            generation: GenerationRecord,
            assistant_msg: MessageRecord,
        ) -> None:
            _ = (session, generation, assistant_msg)
            raise RuntimeError("simulated enqueue failure inside Tx 2")

        prepared = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user_id,
            agent_id=user_scope_agent_id,
            conversation_id=None,
            user_message="This Tx 2 will roll back",
        )
        with pytest.raises(ServiceUnavailableError) as exc_info:
            await conversations_service.complete_prepared_sync_generation(
                session_factory,
                model_provider=model_provider,
                memory_provider=memory_provider,
                prepared=prepared,
                before_commit_hook=_boom_hook,
            )
        assert exc_info.value.code == "generation_commit_failed"

        rolled_back_gen = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=user_id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
        )
        assert rolled_back_gen.status == "failed"
        assert rolled_back_gen.assistant_message_id is None
        msgs = await generation_store.list_conversation_messages_for_owner(
            session_factory,
            owner_id=user_id,
            conversation_id=prepared.conversation_id,
        )
        assert [m.role for m in msgs] == ["user"]
        jobs_after_rollback = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(jobs_after_rollback) == 2

        # 5. Failed and cancelled turns never create turn memory jobs.
        with pytest.raises(BadGatewayError):
            await conversations_service.run_sync_chat(
                session_factory,
                persona_provider=persona_provider,
                model_provider=_FailingModelProvider(),
                memory_provider=memory_provider,
                owner_id=user_id,
                agent_id=user_scope_agent_id,
                conversation_id=None,
                user_message="Model will fail",
            )
        prepared_cancel = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user_id,
            agent_id=user_scope_agent_id,
            conversation_id=None,
            user_message="Will cancel",
        )
        await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=user_id,
            conversation_id=prepared_cancel.conversation_id,
            generation_id=prepared_cancel.generation_id,
            lease_token=prepared_cancel.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="cancelled by client",
        )
        jobs_final = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(jobs_final) == 2
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memory_jobs_view_is_read_only_and_filters_non_memory_jobs(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="view-ro-user",
        )
        async with session_factory.begin() as session:
            mem_job, created_first = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-1",
                user_id=user.id,
                dedupe_key="memory_turn_write:gen-1",
                metadata_json={"write_id": "mtw_1", "provider_id": "memos", "scope": "user"},
            )
            # Duplicate dedupe_key returns existing job idempotently.
            dup_job, created_dup = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-1",
                user_id=user.id,
                dedupe_key="memory_turn_write:gen-1",
                metadata_json={"write_id": "mtw_1", "provider_id": "memos", "scope": "user"},
            )
            await jobs_service.enqueue_job_in_session(
                session,
                job_type="retention_cleanup",
                business_id="window-1",
                user_id=None,
                dedupe_key="retention_cleanup:window-1",
                metadata_json={"batch": 1},
            )
        assert created_first is True
        assert created_dup is False
        assert dup_job.id == mem_job.id

        view_rows = await jobs_service.list_memory_jobs(session_factory)
        assert [row.id for row in view_rows] == [mem_job.id]
        assert view_rows[0].write_id == "mtw_1"
        assert view_rows[0].provider_id == "memos"

        # Verify DB enforces read-only view against INSERT, UPDATE, and DELETE.
        with pytest.raises(DBAPIError, match="memory_jobs is a read-only view"):
            async with session_factory.begin() as session:
                await session.execute(
                    text("UPDATE memory_jobs SET status = 'cancelled' WHERE id = :id"),
                    {"id": mem_job.id},
                )
        with pytest.raises(DBAPIError, match="memory_jobs is a read-only view"):
            async with session_factory.begin() as session:
                await session.execute(
                    text("DELETE FROM memory_jobs WHERE id = :id"),
                    {"id": mem_job.id},
                )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_worker_retries_exhaustion_reconciling_verification_and_log_redaction(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    mem_ai_logger = logging.getLogger("mem_ai")
    mem_ai_logger.addHandler(log_handler)
    mem_ai_logger.setLevel(logging.INFO)
    try:
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="answer:")
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="worker-retry-user",
            memory_policy=MemoryPolicy(enabled=True, write_enabled=True, write_scope="user"),
        )

        # Case A: Transient failures retry with same write_id, then exhaust into `failed`;
        # committed chat reply remains `succeeded`.
        always_fail_provider = _ScriptedMemoryProvider(
            write_outcomes=[
                MemoryWriteResult(
                    status="failed",
                    provider_id="scripted-memory",
                    error_code="memory_provider_unavailable",
                ),
                MemoryWriteResult(
                    status="failed",
                    provider_id="scripted-memory",
                    error_code="memory_provider_unavailable",
                ),
                MemoryWriteResult(
                    status="failed",
                    provider_id="scripted-memory",
                    error_code="memory_provider_unavailable",
                ),
            ]
        )
        chat_fail = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=always_fail_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="CONFIDENTIAL_USER_BODY_XYZ api_key=sk-secret-999",
            options=SyncChatOptions(memory_write_max_attempts=3),
        )
        registry_fail = build_worker_registry(
            session_factory,
            memory_provider=always_fail_provider,
        )
        base_time = datetime.now(UTC) + timedelta(seconds=1)
        runner_fail = WorkerRunner(
            session_factory=session_factory,
            registry=registry_fail,
            config=WorkerConfig(
                worker_id="worker-retry-test",
                concurrency=2,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(
                    base_delay_seconds=5.0,
                    max_delay_seconds=60.0,
                    jitter_ratio=0.0,
                ),
                attempt_retention_days=30,
            ),
        )
        # Attempt 1 -> retry_wait (+5s)
        assert await runner_fail.run_once(now=base_time) == 1
        jobs_list = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        fail_job_id = jobs_list[0].id
        j1 = await jobs_service.get_job(session_factory, job_id=fail_job_id)
        assert j1 is not None and j1.status == "retry_wait"
        assert j1.attempt_count == 1
        assert j1.next_run_at == base_time + timedelta(seconds=5.0)
        # Not due yet at +4s
        assert await runner_fail.run_once(now=base_time + timedelta(seconds=4.0)) == 0
        # Attempt 2 at +5s -> retry_wait (+10s)
        t2 = base_time + timedelta(seconds=5.0)
        assert await runner_fail.run_once(now=t2) == 1
        j2 = await jobs_service.get_job(session_factory, job_id=fail_job_id)
        assert j2 is not None and j2.status == "retry_wait"
        assert j2.attempt_count == 2
        assert j2.next_run_at == t2 + timedelta(seconds=10.0)
        # Attempt 3 at +15s -> exhausted -> terminal `failed`
        t3 = t2 + timedelta(seconds=10.0)
        assert await runner_fail.run_once(now=t3) == 1
        j3 = await jobs_service.get_job(session_factory, job_id=fail_job_id)
        assert j3 is not None and j3.status == "failed"
        assert j3.attempt_count == 3
        assert j3.finished_at == t3
        assert j3.last_error_code == "memory_provider_unavailable"
        # All 3 retries used identical stable write_id!
        assert len({w.write_id for w in always_fail_provider.writes}) == 1

        # Attempts have independent 30-day retention expiry timestamps.
        attempts = await jobs_service.list_job_attempts(session_factory, job_id=fail_job_id)
        assert len(attempts) == 3
        assert [a.status for a in attempts] == ["retry_wait", "retry_wait", "failed"]
        for att in attempts:
            delta_days = (att.expires_at - att.started_at).total_seconds() / 86400.0
            assert delta_days == pytest.approx(30.0, abs=0.01)

        # Committed reply remains `succeeded` despite upstream memory exhaustion.
        gen_check = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=user_id,
            conversation_id=chat_fail.conversation_id,
            generation_id=chat_fail.generation_id,
        )
        assert gen_check.status == "succeeded"

        # Case B: Response lost (timeout) -> reconciling with external_ref + alert ->
        # unknown verify keeps reconciling -> verified succeeded converges.
        reconcile_provider = _ScriptedMemoryProvider(
            write_outcomes=[
                GatewayTimeoutError(code="memory_provider_timeout", message="dropped response"),
            ],
            verify_outcomes=[
                MemoryOperationStatus(status="reconciling", external_ref="mtw_ext"),
                MemoryOperationStatus(
                    status="succeeded",
                    external_ref="mtw_ext",
                    memory_id="mem_recovered_77",
                ),
            ],
        )
        chat_rec = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=reconcile_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="PRIVATE_RECONCILE_SECRET_MESSAGE",
        )
        rec_runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=reconcile_provider),
            config=WorkerConfig(
                worker_id="worker-reconcile",
                concurrency=2,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(reconcile_delay_seconds=10.0),
            ),
        )
        t_rec = t3 + timedelta(seconds=10.0)
        assert await rec_runner.run_once(now=t_rec) == 1
        rec_jobs = [
            j
            for j in await jobs_service.list_jobs_for_user(
                session_factory,
                user_id=user_id,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
            )
            if j.business_id == str(chat_rec.generation_id)
        ]
        assert len(rec_jobs) == 1
        rec_job = rec_jobs[0]
        assert rec_job.status == "reconciling"
        assert rec_job.external_ref == f"mtw_{chat_rec.generation_id.hex}"

        # Observability reflects failed + reconciling backlog accurately.
        obs = await jobs_service.get_job_observability(
            session_factory,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert obs.failed_count >= 1
        assert obs.reconciling_count == 1
        assert obs.oldest_due_at is not None

        # Next poll: still unknown -> stays reconciling (not silently failed).
        t_v1 = t_rec + timedelta(seconds=10.0)
        assert await rec_runner.run_once(now=t_v1) == 1
        rec_after_v1 = await jobs_service.get_job(session_factory, job_id=rec_job.id)
        assert rec_after_v1 is not None and rec_after_v1.status == "reconciling"
        assert len(reconcile_provider.writes) == 1  # No duplicate blind write!
        assert len(reconcile_provider.verifies) == 1

        # Next poll: provider confirms succeeded -> converges to succeeded.
        t_v2 = t_v1 + timedelta(seconds=10.0)
        assert await rec_runner.run_once(now=t_v2) == 1
        rec_after_v2 = await jobs_service.get_job(session_factory, job_id=rec_job.id)
        assert rec_after_v2 is not None and rec_after_v2.status == "succeeded"
        assert rec_after_v2.metadata_json.get("memory_id") == "mem_recovered_77"
        assert len(reconcile_provider.writes) == 1
        assert len(reconcile_provider.verifies) == 2

        # Logs contain alert signals & correlation IDs, and NEVER leak user body or secret keys.
        raw_logs = log_buffer.getvalue()
        assert "CONFIDENTIAL_USER_BODY_XYZ" not in raw_logs
        assert "sk-secret-999" not in raw_logs
        assert "PRIVATE_RECONCILE_SECRET_MESSAGE" not in raw_logs
        parsed_logs = [json.loads(line) for line in raw_logs.splitlines() if line.strip()]
        assert any(entry.get("alert") == "job_reconciling" for entry in parsed_logs)
        assert any(entry.get("alert") == "job_failed" for entry in parsed_logs)
    finally:
        mem_ai_logger.removeHandler(log_handler)
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_skip_locked_lease_reclaim_stale_executor_isolation_and_graceful_shutdown(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="concurrency-worker-user",
        )

        # 1. SKIP LOCKED: Two concurrent workers claiming 6 jobs never double-claim any job.
        async with session_factory.begin() as session:
            for idx in range(6):
                await jobs_service.enqueue_job_in_session(
                    session,
                    job_type="custom_cleanup",
                    business_id=f"item-{idx}",
                    user_id=user.id,
                    dedupe_key=f"custom_cleanup:item-{idx}",
                    metadata_json={"idx": idx},
                )

        claim_a, claim_b = await asyncio.gather(
            jobs_service.claim_due_jobs(
                session_factory,
                worker_id="worker-a",
                job_types=("custom_cleanup",),
                limit=4,
                lease_ttl_seconds=30.0,
            ),
            jobs_service.claim_due_jobs(
                session_factory,
                worker_id="worker-b",
                job_types=("custom_cleanup",),
                limit=4,
                lease_ttl_seconds=30.0,
            ),
        )
        ids_a = {c.job.id for c in claim_a}
        ids_b = {c.job.id for c in claim_b}
        assert ids_a.isdisjoint(ids_b)
        assert len(ids_a | ids_b) == 6

        # 2. Expired lease reclaim & stale executor isolation:
        # Worker A's lease expires -> Worker B reclaims & succeeds -> Worker A cannot overwrite!
        target_claim = claim_a[0] if claim_a else claim_b[0]
        expired_time = (target_claim.job.lease_expires_at or datetime.now(UTC)) + timedelta(
            seconds=5
        )
        reclaimed = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="worker-reclaimer",
            job_types=("custom_cleanup",),
            limit=10,
            lease_ttl_seconds=60.0,
            now=expired_time,
        )
        reclaimed_target = next(c for c in reclaimed if c.job.id == target_claim.job.id)
        assert reclaimed_target.lease_token != target_claim.lease_token
        assert reclaimed_target.attempt_number == 2

        # Reclaimer succeeds.
        committed = await jobs_service.record_job_outcome(
            session_factory,
            job_id=reclaimed_target.job.id,
            attempt_id=reclaimed_target.attempt_id,
            lease_token=reclaimed_target.lease_token,
            outcome=JobHandlerOutcome(outcome="succeeded", external_ref="new-worker-ref"),
            now=expired_time + timedelta(seconds=1),
        )
        assert committed is not None and committed.status == "succeeded"

        # Stale worker attempts to overwrite with `failed` -> rejected (returns None)!
        stale_result = await jobs_service.record_job_outcome(
            session_factory,
            job_id=target_claim.job.id,
            attempt_id=target_claim.attempt_id,
            lease_token=target_claim.lease_token,
            outcome=JobHandlerOutcome(
                outcome="failed",
                error_code="stale_error",
                error_summary="stale executor tried to overwrite",
            ),
            now=expired_time + timedelta(seconds=2),
        )
        assert stale_result is None
        final_job = await jobs_service.get_job(session_factory, job_id=target_claim.job.id)
        assert final_job is not None
        assert final_job.status == "succeeded"
        assert final_job.external_ref == "new-worker-ref"

        attempts = await jobs_service.list_job_attempts(session_factory, job_id=target_claim.job.id)
        assert [a.status for a in attempts] == ["lease_lost", "succeeded"]

        # 3. Graceful shutdown cancels unfinished slow job leaving it `running`;
        # restarted Worker reclaims after lease expiry and completes it.
        started_event = asyncio.Event()
        executions = 0

        class _SlowThenFastHandler:
            async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
                nonlocal executions
                executions += 1
                if executions == 1:
                    started_event.set()
                    await asyncio.sleep(30.0)
                return JobHandlerOutcome(
                    outcome="succeeded",
                    result_summary={"attempt": ctx.attempt_number},
                )

        async with session_factory.begin() as session:
            shutdown_job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="shutdown_probe",
                business_id="shutdown-1",
                user_id=user.id,
                dedupe_key="shutdown_probe:1",
                metadata_json={},
            )

        reg1 = JobHandlerRegistry()
        reg1.register("shutdown_probe", _SlowThenFastHandler())
        stop_event = asyncio.Event()
        runner1 = WorkerRunner(
            session_factory=session_factory,
            registry=reg1,
            config=WorkerConfig(
                worker_id="worker-shutting-down",
                concurrency=1,
                poll_interval_seconds=0.05,
                lease_ttl_seconds=10.0,
                lease_renew_interval_seconds=None,
                shutdown_grace_seconds=0.1,
            ),
        )
        run_task = asyncio.create_task(runner1.run_forever(stop_event=stop_event))
        await asyncio.wait_for(started_event.wait(), timeout=5.0)
        stop_event.set()
        await asyncio.wait_for(run_task, timeout=5.0)

        after_shutdown = await jobs_service.get_job(session_factory, job_id=shutdown_job.id)
        assert after_shutdown is not None and after_shutdown.status == "running"

        # Restarted worker reclaims after lease expiry and finishes it cleanly.
        reg2 = JobHandlerRegistry()
        reg2.register("shutdown_probe", _SlowThenFastHandler())
        runner2 = WorkerRunner(
            session_factory=session_factory,
            registry=reg2,
            config=WorkerConfig(
                worker_id="worker-restarted",
                concurrency=1,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
            ),
        )
        reclaim_time = (after_shutdown.lease_expires_at or datetime.now(UTC)) + timedelta(seconds=5)
        assert await runner2.run_once(now=reclaim_time) == 1
        recovered_job = await jobs_service.get_job(session_factory, job_id=shutdown_job.id)
        assert recovered_job is not None and recovered_job.status == "succeeded"
        assert recovered_job.attempt_count == 2
        assert runner2.health_snapshot().succeeded_count == 1
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_worker_reconciles_explicit_memory_operation_and_runs_periodic_recovery(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="explicit-and-periodic-user",
        )
        provider = _ScriptedMemoryProvider(
            write_outcomes=[
                MemoryWriteResult(
                    status="accepted",
                    provider_id="scripted-memory",
                    external_ref="task_async_explicit_1",
                )
            ],
            verify_outcomes=[
                MemoryOperationStatus(
                    status="succeeded",
                    external_ref="task_async_explicit_1",
                    memory_id="mem_explicit_confirmed_1",
                )
            ],
        )

        outcome = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=provider,
            user_id=user.id,
            content="Remember that I prefer dark mode.",
            scope="user",
        )
        assert isinstance(outcome, AcceptedMemoryOutcome)
        op_id = outcome.operation.id

        # Build Worker with explicit memory reconciler and periodic generation lease recovery.
        registry = JobHandlerRegistry()
        registry.register(
            JOB_TYPE_MEMORY_EXPLICIT_CREATE,
            ExplicitMemoryReconcileJobHandler(
                session_factory=session_factory,
                memory_provider=provider,
            ),
        )
        registry.register(
            JOB_TYPE_GENERATION_LEASE_RECOVERY,
            GenerationLeaseRecoveryJobHandler(session_factory=session_factory),
        )
        periodic_spec = PeriodicJobSpec(
            job_type=JOB_TYPE_GENERATION_LEASE_RECOVERY,
            interval_seconds=60.0,
        )
        runner_a = WorkerRunner(
            session_factory=session_factory,
            registry=registry,
            config=WorkerConfig(worker_id="w-periodic-a", lease_renew_interval_seconds=None),
            periodic_jobs=(periodic_spec,),
        )
        runner_b = WorkerRunner(
            session_factory=session_factory,
            registry=registry,
            config=WorkerConfig(worker_id="w-periodic-b", lease_renew_interval_seconds=None),
            periodic_jobs=(periodic_spec,),
        )

        # Multi-worker periodic scheduling in same window creates exactly ONE periodic job.
        window_time = datetime.now(UTC) + timedelta(seconds=5)
        c_a = await runner_a.schedule_periodic_jobs(now=window_time)
        c_b = await runner_b.schedule_periodic_jobs(now=window_time)
        assert c_a + c_b == 1

        # Running worker processes both the explicit memory reconcile job and periodic recovery job.
        processed = await runner_a.run_once(now=window_time)
        assert processed == 2

        op_after = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=user.id,
            operation_id=op_id,
        )
        assert op_after.status == "succeeded"
        assert op_after.result_summary.get("memory_id") == "mem_explicit_confirmed_1"

        # Verify `DatabaseTurnContentLoader` returns None cleanly for unknown generation.
        loader = DatabaseTurnContentLoader(session_factory=session_factory)
        assert (
            await loader.load_turn_for_memory_write(
                owner_id=user.id,
                conversation_id=uuid4(),
                generation_id=uuid4(),
            )
            is None
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_worker_crash_mid_dispatch_verifies_instead_of_re_sending_and_barrier_suppresses(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="ok:")
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="crash-mid-dispatch-user",
            memory_policy=MemoryPolicy(enabled=True, write_enabled=True, write_scope="user"),
        )

        # 1. Crash mid-HTTP call: Worker 1 marks `in_flight` before calling provider, then
        # gets cancelled before committing outcome; Worker 2 reclaims after lease expiry and
        # calls `verify_operation` instead of blindly calling `write_turn_memory` again.
        gated_provider = _ScriptedMemoryProvider(
            verify_outcomes=[
                MemoryOperationStatus(
                    status="succeeded",
                    external_ref="verified_after_crash",
                    memory_id="mem_crash_verified_1",
                )
            ]
        )
        gated_provider.gate_writes = True

        res = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=gated_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Crash recovery turn",
        )
        stop_event = asyncio.Event()
        runner1 = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=gated_provider),
            config=WorkerConfig(
                worker_id="w-crash-1",
                concurrency=1,
                poll_interval_seconds=0.05,
                lease_ttl_seconds=10.0,
                lease_renew_interval_seconds=None,
                shutdown_grace_seconds=0.1,
            ),
        )
        t_run = asyncio.create_task(runner1.run_forever(stop_event=stop_event))
        await asyncio.wait_for(gated_provider.write_started.wait(), timeout=5.0)
        stop_event.set()
        await asyncio.wait_for(t_run, timeout=5.0)
        assert len(gated_provider.writes) == 1

        jobs_mid = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(jobs_mid) == 1
        assert jobs_mid[0].status == "running"
        assert jobs_mid[0].metadata_json.get("dispatch_state") == "in_flight"
        assert jobs_mid[0].external_ref == f"mtw_{res.generation_id.hex}"

        # Worker 2 reclaims after lease expiry -> verifies instead of re-writing.
        gated_provider.gate_writes = False
        runner2 = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=gated_provider),
            config=WorkerConfig(
                worker_id="w-crash-2",
                concurrency=1,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
            ),
        )
        reclaim_now = (jobs_mid[0].lease_expires_at or datetime.now(UTC)) + timedelta(seconds=5)
        assert await runner2.run_once(now=reclaim_now) == 1
        assert len(gated_provider.writes) == 1  # Zero duplicate blind write!
        assert len(gated_provider.verifies) == 1

        final_job = await jobs_service.get_job(session_factory, job_id=jobs_mid[0].id)
        assert final_job is not None and final_job.status == "succeeded"
        assert final_job.metadata_json.get("memory_id") == "mem_crash_verified_1"

        # 2. Epoch advancement between Tx 1 and Tx 2 suppresses turn job enqueue
        # while still committing the assistant reply as `succeeded`.
        prepared = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=gated_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Turn whose epoch is superseded",
        )
        async with session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE memory_scopes SET epoch = epoch + 1, barrier_state = 'clearing' "
                    "WHERE user_id = :uid AND provider_id = :pid"
                ),
                {"uid": user_id, "pid": gated_provider.provider_id},
            )
        committed = await conversations_service.complete_prepared_sync_generation(
            session_factory,
            model_provider=model_provider,
            memory_provider=gated_provider,
            prepared=prepared,
        )
        assert committed.assistant_message.content == "ok:Turn whose epoch is superseded"
        jobs_after_barrier = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        assert len(jobs_after_barrier) == 1  # Suppressed for the stale-epoch generation!
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_review_regressions_reconcile_budget_confirmed_clear_requeue_and_atomic_op(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    mem_ai_logger = logging.getLogger("mem_ai")
    mem_ai_logger.addHandler(log_handler)
    mem_ai_logger.setLevel(logging.INFO)
    try:
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="ok:")
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="review-regression-user",
            memory_policy=MemoryPolicy(enabled=True, write_enabled=True, write_scope="user"),
        )

        # 1. Reconciling polls do NOT burn attempt_count retry budget:
        # Attempt 1 -> accepted -> 2 reconciling polls -> verify reports failed -> still retries!
        async_provider = _ScriptedMemoryProvider(
            write_outcomes=[
                MemoryWriteResult(
                    status="accepted",
                    provider_id="scripted-memory",
                    external_ref="task_async_budget_1",
                ),
                MemoryWriteResult(
                    status="succeeded",
                    provider_id="scripted-memory",
                    memory_id="mem_after_reconcile_retry",
                    external_ref="mtw_retry_2",
                ),
            ],
            verify_outcomes=[
                MemoryOperationStatus(status="reconciling", external_ref="task_async_budget_1"),
                MemoryOperationStatus(status="reconciling", external_ref="task_async_budget_1"),
                MemoryOperationStatus(
                    status="failed",
                    external_ref="task_async_budget_1",
                    error_code="memory_provider_error",
                ),
            ],
        )
        chat_res = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=async_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Async extraction budget test",
            options=SyncChatOptions(memory_write_max_attempts=2),
        )
        runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=async_provider),
            config=WorkerConfig(
                worker_id="w-review-reg",
                concurrency=1,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(
                    base_delay_seconds=5.0,
                    max_delay_seconds=30.0,
                    jitter_ratio=0.0,
                    reconcile_delay_seconds=5.0,
                ),
            ),
        )
        t0 = datetime.now(UTC) + timedelta(seconds=1)
        assert await runner.run_once(now=t0) == 1  # attempt_count=1 -> accepted -> reconciling
        assert await runner.run_once(now=t0 + timedelta(seconds=5)) == 1  # poll 1 -> reconciling
        assert await runner.run_once(now=t0 + timedelta(seconds=10)) == 1  # poll 2 -> reconciling
        assert await runner.run_once(now=t0 + timedelta(seconds=15)) == 1  # verify failed -> retry
        jobs_list = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=user_id,
            job_type=JOB_TYPE_MEMORY_TURN_WRITE,
        )
        budget_job = next(j for j in jobs_list if j.business_id == str(chat_res.generation_id))
        assert budget_job.status == "retry_wait"
        assert budget_job.attempt_count == 1  # Reconciling polls did NOT burn retry budget!
        assert await runner.run_once(now=t0 + timedelta(seconds=25)) == 1  # retry 2 -> succeeded
        budget_done = await jobs_service.get_job(session_factory, job_id=budget_job.id)
        assert budget_done is not None and budget_done.status == "succeeded"
        assert budget_done.attempt_count == 2
        attempts = await jobs_service.list_job_attempts(session_factory, job_id=budget_job.id)
        assert [a.attempt_number for a in attempts] == [1, 2, 3, 4, 5]

        # 2. `confirmed_during_clear` stays `reconciling` on subsequent polls (never self-cancels).
        clear_provider = _ScriptedMemoryProvider()
        chat_clear = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=clear_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Confirm during clear",
        )
        clear_job = next(
            j
            for j in await jobs_service.list_jobs_for_user(
                session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_TURN_WRITE
            )
            if j.business_id == str(chat_clear.generation_id)
        )
        # Simulate clearing barrier starting while provider call confirmed.
        clear_provider.gate_writes = True
        clear_runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=clear_provider),
            config=WorkerConfig(
                worker_id="w-clear-race",
                concurrency=1,
                lease_ttl_seconds=30.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(reconcile_delay_seconds=5.0),
            ),
        )
        run_clear_task = asyncio.create_task(clear_runner.run_once(now=t0 + timedelta(seconds=30)))
        await asyncio.wait_for(clear_provider.write_started.wait(), timeout=5.0)
        async with session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE memory_scopes SET epoch = epoch + 1, barrier_state = 'clearing' "
                    "WHERE user_id = :uid AND provider_id = :pid"
                ),
                {"uid": user_id, "pid": clear_provider.provider_id},
            )
        clear_provider.release_write.set()
        assert await run_clear_task == 1
        after_race = await jobs_service.get_job(session_factory, job_id=clear_job.id)
        assert after_race is not None
        assert after_race.status == "reconciling"
        assert after_race.metadata_json.get("dispatch_state") == "confirmed_during_clear"

        # Subsequent poll MUST stay `reconciling`, never self-cancel!
        assert await clear_runner.run_once(now=t0 + timedelta(seconds=40)) == 1
        after_next_poll = await jobs_service.get_job(session_factory, job_id=clear_job.id)
        assert after_next_poll is not None
        assert after_next_poll.status == "reconciling"

        # 3. Repeated lease expiry on non-dispatched job bounds at `max_attempts` -> `failed`,
        # and controlled `requeue_failed_job` lets an operator retry it cleanly.
        async with session_factory.begin() as session:
            crash_job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="bounded_crash_probe",
                business_id="crash-1",
                user_id=user_id,
                dedupe_key="bounded_crash_probe:1",
                metadata_json={},
                max_attempts=1,
            )
        t_claim = t0 + timedelta(seconds=50)
        c1 = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="w-c1",
            job_types=("bounded_crash_probe",),
            lease_ttl_seconds=5.0,
            now=t_claim,
        )
        assert len(c1) == 1
        # Lease expires at max_attempts=1 with external_ref=None -> converges to failed!
        c2 = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="w-c2",
            job_types=("bounded_crash_probe",),
            lease_ttl_seconds=5.0,
            now=t_claim + timedelta(seconds=10),
        )
        assert c2 == []
        failed_crash = await jobs_service.get_job(session_factory, job_id=crash_job.id)
        assert failed_crash is not None and failed_crash.status == "failed"

        # Controlled operational requeue transitions failed -> pending with extra attempt.
        requeued = await jobs_service.requeue_failed_job(
            session_factory,
            job_id=crash_job.id,
            extra_attempts=1,
            reason="operator verified safe retry",
            now=t_claim + timedelta(seconds=15),
        )
        assert requeued.status == "pending"
        assert requeued.max_attempts == 2

        # 4. ExplicitMemoryReconcileJobHandler stale executor cannot overwrite `operations`.
        op_provider = _ScriptedMemoryProvider(
            write_outcomes=[
                MemoryWriteResult(
                    status="accepted",
                    provider_id="scripted-memory",
                    external_ref="task_op_atomic_1",
                )
            ],
            verify_outcomes=[
                MemoryOperationStatus(
                    status="succeeded",
                    external_ref="task_op_atomic_1",
                    memory_id="mem_stale_op",
                )
            ],
        )
        # Clear the barrier we set above on scripted-memory so explicit create succeeds.
        async with session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE memory_scopes SET barrier_state = 'idle' "
                    "WHERE user_id = :uid AND provider_id = :pid"
                ),
                {"uid": user_id, "pid": op_provider.provider_id},
            )
        explicit_out = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=op_provider,
            user_id=user_id,
            content="Atomic op check",
            scope="user",
        )
        assert isinstance(explicit_out, AcceptedMemoryOutcome)
        explicit_claims = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="w-stale-op",
            job_types=(JOB_TYPE_MEMORY_EXPLICIT_CREATE,),
            lease_ttl_seconds=5.0,
            now=t_claim + timedelta(seconds=20),
        )
        assert len(explicit_claims) == 1
        handler = ExplicitMemoryReconcileJobHandler(
            session_factory=session_factory,
            memory_provider=op_provider,
        )
        stale_outcome = await handler.handle(
            JobExecutionContext(
                job=explicit_claims[0].job,
                attempt_id=explicit_claims[0].attempt_id,
                attempt_number=explicit_claims[0].attempt_number,
                worker_id="w-stale-op",
                lease_token=explicit_claims[0].lease_token,
            )
        )
        # Operation MUST NOT have been transitioned inside `handle()` before lease-checked commit!
        op_before = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=user_id,
            operation_id=explicit_out.operation.id,
        )
        assert op_before.status == "running"

        # Now commit with an expired timestamp -> discarded -> operation STILL `running`!
        discarded = await jobs_service.record_job_outcome(
            session_factory,
            job_id=explicit_claims[0].job.id,
            attempt_id=explicit_claims[0].attempt_id,
            lease_token=explicit_claims[0].lease_token,
            outcome=stale_outcome,
            now=t_claim + timedelta(seconds=60),
        )
        assert discarded is None
        op_still_running = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=user_id,
            operation_id=explicit_out.operation.id,
        )
        assert op_still_running.status == "running"

        # Structured Worker logs include `duration_ms`.
        parsed_logs = [
            json.loads(line) for line in log_buffer.getvalue().splitlines() if line.strip()
        ]
        processed_logs = [
            entry
            for entry in parsed_logs
            if entry.get("message") == "Processed durable job attempt"
        ]
        assert processed_logs and all("duration_ms" in entry for entry in processed_logs)
    finally:
        mem_ai_logger.removeHandler(log_handler)
        await engine.dispose()
