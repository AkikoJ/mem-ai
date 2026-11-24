"""PostgreSQL integration tests for bounded retention sweeps, expiry boundaries, and protection."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryRecord
from mem_ai.contracts.model_provider import ModelUsage
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError, UnauthorizedError
from mem_ai.core.logging import REDACTED_VALUE, JsonLogFormatter
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import cleanup as conversations_cleanup
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import idempotency as conversations_idempotency
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.models import (
    Conversation,
    Generation,
    IdempotencyRecord,
    Message,
    MessageRecalledMemory,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.jobs.schemas import JobBackoffPolicy, JobHandlerOutcome
from mem_ai.modules.memories import repository as memories_repository
from mem_ai.modules.memories.jobs import JOB_TYPE_MEMORY_TURN_WRITE
from mem_ai.modules.memories.models import MemoryListSnapshot, MemoryScopeState, MemoryTombstone
from mem_ai.modules.memories.snapshots import MemoryListScopeKey, MemorySnapshotStore
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.models import Operation
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.retention import service as retention_service
from mem_ai.modules.retention.schemas import RetentionPolicy
from mem_ai.modules.retention.service import (
    JOB_TYPE_RETENTION_CLEANUP,
    run_retention_sweep,
)
from mem_ai.modules.users import cleanup as users_cleanup
from mem_ai.modules.users import service as users_service
from mem_ai.workers.main import (
    JOB_TYPE_GENERATION_LEASE_RECOVERY,
    build_worker_registry,
    default_periodic_jobs,
)
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.conftest import FakeMemoryProvider, FakeModelProvider


async def _seed_user_model_and_agent(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    subject: str,
) -> tuple[UUID, UUID]:
    user = await users_service.resolve_or_create_user(
        session_factory,
        issuer="https://idp.retention.test",
        subject=subject,
    )
    await models_service.sync_model_configs(
        session_factory,
        seeds=[
            ModelConfigSeed(
                model_id="retention-model",
                provider_id="fake-model",
                upstream_model="retention-model",
                display_name="Retention Model",
                enabled=True,
                supports_streaming=True,
            )
        ],
    )
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest(
            name=f"Agent {subject}",
            systemPrompt="Stay concise.",
            defaultModelId="retention-model",
        ),
    )
    await agents_service.publish_agent(
        session_factory,
        owner_id=user.id,
        agent_id=agent.agent_id,
    )
    return user.id, agent.agent_id


async def _count_rows(session_factory: async_sessionmaker[AsyncSession], model: type[Any]) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count(model.id))) or 0)


@pytest.mark.integration
@pytest.mark.anyio
async def test_idempotency_24h_boundary_late_terminal_running_and_key_reuse(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="idem-retention-user",
        )
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="reply:")
        t0 = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)

        # 1. Terminal succeeded generation committed at t0 (expires at t0 + 24h)
        spec_succ = conversations_idempotency.build_chat_idempotency_spec(
            raw_key="idem-succeeded-key",
            agent_id=agent_id,
            conversation_id=None,
            message="hello retention",
            requested_model_id=None,
            retention_seconds=24 * 3600,
        )
        persona = await persona_provider.get_latest_published(
            owner_id=user_id,
            agent_id=agent_id,
        )
        prep_succ = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=user_id,
            agent_id=agent_id,
            persona=persona,
            conversation_id=None,
            user_message_content="hello retention",
            model_id="retention-model",
            idempotency_spec=spec_succ,
            now=t0,
        )
        # Attach a deleted recalled memory ref to verify it stays with the parent conversation.
        async with session_factory.begin() as session:
            session.add(
                MessageRecalledMemory(
                    user_message_id=prep_succ.user_message_id,
                    provider_id="fake-memory",
                    memory_id="mem-deleted-ref",
                    scope="user",
                    relevance=0.9,
                    prompt_position=1,
                    memory_epoch=1,
                    recalled_at=t0,
                    deleted_at=t0 + timedelta(minutes=5),
                )
            )
        await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=prep_succ,
            assistant_content="reply:hello retention",
            usage=ModelUsage(input_tokens=4, output_tokens=3, total_tokens=7),
            idempotency_retention_seconds=24 * 3600,
            now=t0,
        )

        # 2. Terminal failed generation committed at t0 (expires at t0 + 24h)
        spec_fail = conversations_idempotency.build_chat_idempotency_spec(
            raw_key="idem-failed-key",
            agent_id=agent_id,
            conversation_id=None,
            message="fail me",
            requested_model_id=None,
            retention_seconds=24 * 3600,
        )
        prep_fail = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=user_id,
            agent_id=agent_id,
            persona=persona,
            conversation_id=None,
            user_message_content="fail me",
            model_id="retention-model",
            idempotency_spec=spec_fail,
            now=t0,
        )
        await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=user_id,
            conversation_id=prep_fail.conversation_id,
            generation_id=prep_fail.generation_id,
            lease_token=prep_fail.lease_token,
            error_code="model_upstream_error",
            error_message="upstream failed",
            idempotency_retention_seconds=24 * 3600,
            now=t0,
        )

        # 2b. Terminal cancelled generation committed at t0 (expires at t0 + 24h)
        spec_cancel = conversations_idempotency.build_chat_idempotency_spec(
            raw_key="idem-cancelled-key",
            agent_id=agent_id,
            conversation_id=None,
            message="cancel me",
            requested_model_id=None,
            retention_seconds=24 * 3600,
        )
        prep_cancel = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=user_id,
            agent_id=agent_id,
            persona=persona,
            conversation_id=None,
            user_message_content="cancel me",
            model_id="retention-model",
            idempotency_spec=spec_cancel,
            now=t0,
        )
        await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=user_id,
            conversation_id=prep_cancel.conversation_id,
            generation_id=prep_cancel.generation_id,
            lease_token=prep_cancel.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="client disconnected",
            idempotency_retention_seconds=24 * 3600,
            now=t0,
        )

        # 3. Late-terminal generation: started t0 - 30h, committed t0 + 2h (expires t0 + 26h)
        spec_late = conversations_idempotency.build_chat_idempotency_spec(
            raw_key="idem-late-terminal-key",
            agent_id=agent_id,
            conversation_id=None,
            message="late finisher",
            requested_model_id=None,
            retention_seconds=24 * 3600,
        )
        prep_late = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=user_id,
            agent_id=agent_id,
            persona=persona,
            conversation_id=None,
            user_message_content="late finisher",
            model_id="retention-model",
            lease_ttl_seconds=3600 * 40,
            idempotency_spec=spec_late,
            now=t0 - timedelta(hours=30),
        )
        await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=prep_late,
            assistant_content="reply:late finisher",
            usage=ModelUsage(input_tokens=4, output_tokens=3, total_tokens=7),
            idempotency_retention_seconds=24 * 3600,
            now=t0 + timedelta(hours=2),
        )

        # 4. Active running generation created 30h before t0 (must NEVER expire)
        spec_running = conversations_idempotency.build_chat_idempotency_spec(
            raw_key="idem-running-key",
            agent_id=agent_id,
            conversation_id=None,
            message="still running",
            requested_model_id=None,
            retention_seconds=24 * 3600,
        )
        prep_running = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=user_id,
            agent_id=agent_id,
            persona=persona,
            conversation_id=None,
            user_message_content="still running",
            model_id="retention-model",
            lease_ttl_seconds=3600 * 100,
            idempotency_spec=spec_running,
            now=t0 - timedelta(hours=30),
        )

        # Exact boundary check: 1 second before 24h -> NOTHING deleted.
        policy = RetentionPolicy(batch_size=10, max_batches_per_run=20)
        summary_before, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(hours=24) - timedelta(seconds=1),
        )
        assert summary_before.idempotency_records_deleted == 0
        assert await _count_rows(session_factory, IdempotencyRecord) == 5

        # At exact 24h mark: succeeded/failed/cancelled at t0 deleted; running & late stay.
        summary_at_24h, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(hours=24),
        )
        assert summary_at_24h.idempotency_records_deleted == 3
        assert await _count_rows(session_factory, IdempotencyRecord) == 2

        assert (
            await conversations_idempotency.get_idempotency_record_for_generation(
                session_factory, generation_id=prep_succ.generation_id
            )
            is None
        )
        assert (
            await conversations_idempotency.get_idempotency_record_for_generation(
                session_factory, generation_id=prep_fail.generation_id
            )
            is None
        )
        assert (
            await conversations_idempotency.get_idempotency_record_for_generation(
                session_factory, generation_id=prep_cancel.generation_id
            )
            is None
        )
        late_rec = await conversations_idempotency.get_idempotency_record_for_generation(
            session_factory, generation_id=prep_late.generation_id
        )
        running_rec = await conversations_idempotency.get_idempotency_record_for_generation(
            session_factory, generation_id=prep_running.generation_id
        )
        assert late_rec is not None and late_rec.status == "succeeded"
        assert running_rec is not None and running_rec.status == "running"

        # Conversations, generations, messages, and deleted recalled memory refs are all preserved!
        assert await _count_rows(session_factory, Conversation) == 5
        assert await _count_rows(session_factory, Generation) == 5
        assert await _count_rows(session_factory, Message) == 7
        assert await _count_rows(session_factory, MessageRecalledMemory) == 1

        # Reusing the expired key behaves as a brand-new chat request creating a new generation.
        reused = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="completely new message after expiry",
            idempotency_key="idem-succeeded-key",
        )
        assert reused.generation_id != prep_succ.generation_id
        assert reused.conversation_id != prep_succ.conversation_id

        # At t0 + 26h, late-terminal record expires too; running record STILL survives!
        summary_at_26h, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(hours=26),
        )
        assert summary_at_26h.idempotency_records_deleted == 1
        assert (
            await conversations_idempotency.get_idempotency_record_for_generation(
                session_factory, generation_id=prep_late.generation_id
            )
            is None
        )
        assert (
            await conversations_idempotency.get_idempotency_record_for_generation(
                session_factory, generation_id=prep_running.generation_id
            )
            is not None
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_job_attempts_30d_boundary_late_finish_active_and_provider_dedupe_protection(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, _ = await _seed_user_model_and_agent(
            session_factory,
            subject="jobs-retention-user",
        )
        t0 = datetime(2026, 10, 1, 0, 0, 0, tzinfo=UTC)

        # Seed jobs covering every protection rule:
        # A. Settled memory_turn_write with external_ref + dedupe_key (finished t0 -> exp t0+30d)
        # B. Late-finishing terminal job (claimed at t0, finished at t0+2d -> expires t0+32d)
        # C. Reconciling memory_turn_write job with old attempt (parent non-terminal -> PROTECTED)
        # D. Active running job with running attempt (PROTECTED)
        # E. Succeeded job linked to non-terminal reconciling operation (PROTECTED)
        # F. Old succeeded periodic job (finished at t0 -> pruned at t0+30d)
        async with session_factory.begin() as session:
            job_a, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-a",
                user_id=user_id,
                dedupe_key="memory_turn_write:gen-a:fake-memory",
                metadata_json={"provider_id": "fake-memory", "write_id": "w-a"},
                now=t0,
            )
            job_b, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-b",
                user_id=user_id,
                dedupe_key="memory_turn_write:gen-b:fake-memory",
                metadata_json={"provider_id": "fake-memory", "write_id": "w-b"},
                now=t0,
            )
            job_c, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-c",
                user_id=user_id,
                dedupe_key="memory_turn_write:gen-c:fake-memory",
                metadata_json={"provider_id": "fake-memory", "write_id": "w-c"},
                now=t0,
            )
            job_d, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_MEMORY_TURN_WRITE,
                business_id="gen-d",
                user_id=user_id,
                dedupe_key="memory_turn_write:gen-d:fake-memory",
                metadata_json={"provider_id": "fake-memory", "write_id": "w-d"},
                now=t0,
            )
            reconciling_op = await operations_service.create_operation_in_session(
                session,
                owner_id=user_id,
                operation_type="memory_clear",
                status="reconciling",
                target_type="memory_scope",
                target_id="fake-memory",
            )
            job_e, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="memory_clear",
                business_id=str(reconciling_op.id),
                user_id=user_id,
                dedupe_key=f"memory_clear:{reconciling_op.id}",
                metadata_json={"operation_id": str(reconciling_op.id)},
                now=t0,
            )
            await operations_service.attach_job_in_session(
                session,
                operation_id=reconciling_op.id,
                job_id=job_e.id,
                now=t0,
            )
            job_f, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_GENERATION_LEASE_RECOVERY,
                business_id="window:1",
                user_id=None,
                dedupe_key=f"periodic:{JOB_TYPE_GENERATION_LEASE_RECOVERY}:1",
                metadata_json={"window_bucket": 1},
                now=t0,
            )

        claimed = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="test-worker",
            limit=10,
            lease_ttl_seconds=3600 * 24 * 40,
            attempt_retention_days=30,
            now=t0,
        )
        by_id = {item.job.id: item for item in claimed}

        # Complete A at t0 with external_ref
        await jobs_service.record_job_outcome(
            session_factory,
            job_id=job_a.id,
            attempt_id=by_id[job_a.id].attempt_id,
            lease_token=by_id[job_a.id].lease_token,
            outcome=JobHandlerOutcome(outcome="succeeded", external_ref="ext-ref-a"),
            attempt_retention_days=30,
            now=t0,
        )
        # Complete B late at t0 + 2d (extends attempt expires_at to t0 + 32d!)
        await jobs_service.record_job_outcome(
            session_factory,
            job_id=job_b.id,
            attempt_id=by_id[job_b.id].attempt_id,
            lease_token=by_id[job_b.id].lease_token,
            outcome=JobHandlerOutcome(outcome="succeeded", external_ref="ext-ref-b"),
            attempt_retention_days=30,
            now=t0 + timedelta(days=2),
        )
        # Transition C to reconciling at t0 (external outcome unconfirmed!)
        await jobs_service.record_job_outcome(
            session_factory,
            job_id=job_c.id,
            attempt_id=by_id[job_c.id].attempt_id,
            lease_token=by_id[job_c.id].lease_token,
            outcome=JobHandlerOutcome(outcome="reconciling", external_ref="ext-ref-c"),
            attempt_retention_days=30,
            now=t0,
        )
        # Leave D running!
        # Complete E at t0 (but its Operation is still reconciling!)
        await jobs_service.record_job_outcome(
            session_factory,
            job_id=job_e.id,
            attempt_id=by_id[job_e.id].attempt_id,
            lease_token=by_id[job_e.id].lease_token,
            outcome=JobHandlerOutcome(outcome="succeeded"),
            attempt_retention_days=30,
            now=t0,
        )
        # Complete periodic job F at t0
        await jobs_service.record_job_outcome(
            session_factory,
            job_id=job_f.id,
            attempt_id=by_id[job_f.id].attempt_id,
            lease_token=by_id[job_f.id].lease_token,
            outcome=JobHandlerOutcome(outcome="succeeded"),
            attempt_retention_days=30,
            now=t0,
        )

        policy = RetentionPolicy(job_attempt_retention_days=30, batch_size=10)
        periodic_types = (JOB_TYPE_GENERATION_LEASE_RECOVERY, JOB_TYPE_RETENTION_CLEANUP)

        # 1 second before 30d boundary: NOTHING deleted.
        before_30d, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(days=30) - timedelta(seconds=1),
            prunable_periodic_job_types=periodic_types,
        )
        assert before_30d.job_attempts_deleted == 0
        assert before_30d.periodic_jobs_deleted == 0

        # At exact 30d boundary:
        # - job_a attempt deleted, job_a row (with external_ref & dedupe_key) RETAINED
        # - job_f attempt & periodic job_f deleted
        # - job_b late-finishing attempt RETAINED
        # - job_c reconciling job & attempt RETAINED
        # - job_d running job & attempt RETAINED
        # - job_e attempt RETAINED because reconciling_op is non-terminal (protected_skipped)
        at_30d, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(days=30),
            prunable_periodic_job_types=periodic_types,
        )
        assert at_30d.job_attempts_deleted == 2  # job_a + job_f attempts
        assert at_30d.periodic_jobs_deleted == 1  # job_f
        assert at_30d.protected_skipped == 1  # job_e attempt protected by reconciling_op

        # Verify provider dedupe job A still exists with external_ref & dedupe_key!
        reloaded_a = await jobs_service.get_job(session_factory, job_id=job_a.id)
        assert reloaded_a is not None
        assert reloaded_a.external_ref == "ext-ref-a"
        assert reloaded_a.dedupe_key == "memory_turn_write:gen-a:fake-memory"
        assert await jobs_service.list_job_attempts(session_factory, job_id=job_a.id) == []

        # Verify late-finishing B, reconciling C, running D, and op-linked E attempts all survived!
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_b.id)) == 1
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_c.id)) == 1
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_d.id)) == 1
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_e.id)) == 1
        assert await jobs_service.get_job(session_factory, job_id=job_f.id) is None

        # At t0 + 32d, late-finishing B's attempt expires, while C/D/E remain protected!
        at_32d, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(days=32),
        )
        assert at_32d.job_attempts_deleted == 1
        assert await jobs_service.list_job_attempts(session_factory, job_id=job_b.id) == []
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_c.id)) == 1
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_d.id)) == 1
        assert len(await jobs_service.list_job_attempts(session_factory, job_id=job_e.id)) == 1
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_snapshots_receipts_and_operation_summaries_respect_barriers_and_tombstones(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, _ = await _seed_user_model_and_agent(
            session_factory,
            subject="ops-snap-retention-user",
        )
        user_deleted_id, _ = await _seed_user_model_and_agent(
            session_factory,
            subject="ops-receipt-deleted-user",
        )
        t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=UTC)

        # 1. Pagination snapshots: one expired at t0 + 600s, one still live at t0 + 1200s.
        store = MemorySnapshotStore(session_factory=session_factory, ttl_seconds=600)
        scope_key = MemoryListScopeKey(user_id=user_id, provider_id="fake-memory", scope="user")
        records = [
            MemoryRecord(
                memory_id=f"mem-{idx}",
                user_id=user_id,
                provider_id="fake-memory",
                scope="user",
                content=f"fact {idx}",
                created_at=t0 - timedelta(minutes=idx),
            )
            for idx in range(3)
        ]
        await store.create_first_page(scope_key=scope_key, records=records, limit=1, now=t0)
        await store.create_first_page(
            scope_key=scope_key,
            records=records,
            limit=1,
            now=t0 + timedelta(seconds=600),
        )
        assert await _count_rows(session_factory, MemoryListSnapshot) == 2

        # 2. Finalized user_delete receipt (owner_id=None, status=succeeded, expires at t0 + 30d)
        receipt_settings = migrated_pg_settings.model_copy(
            update={"deletion_receipt_ttl_seconds": 30 * 86400}
        )
        async with session_factory.begin() as session:
            finalized_op, _ = await operations_service.create_user_deletion_operation_in_session(
                session,
                owner_id=user_deleted_id,
                settings=receipt_settings,
                now=t0,
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=finalized_op.id,
                status="running",
                now=t0,
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=finalized_op.id,
                status="succeeded",
                progress={"phase": "succeeded", "local_complete": True, "remote_complete": True},
                result_summary={"deleted": True},
                now=t0,
            )
            await session.execute(
                update(Operation)
                .where(Operation.id == finalized_op.id)
                .values(owner_id=None, target_id=None)
            )

            # Incomplete user_delete operation whose receipt TTL already elapsed -> MUST STAY!
            (
                incomplete_user_op,
                _,
            ) = await operations_service.create_user_deletion_operation_in_session(
                session,
                owner_id=user_id,
                settings=receipt_settings,
                now=t0,
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=incomplete_user_op.id,
                status="running",
                now=t0,
            )
            await operations_service.transition_operation_in_session(
                session,
                operation_id=incomplete_user_op.id,
                status="reconciling",
                progress={"local_complete": True, "remote_complete": False},
                now=t0,
            )

            # 3. Settled memory_delete operation with verified tombstone (eligible at t0 + 30d;
            #    tombstone itself MUST remain!).
            settled_del_op = await operations_service.create_operation_in_session(
                session,
                owner_id=user_id,
                operation_type="memory_delete",
                status="succeeded",
                target_type="memory",
                target_id="mem-verified",
                completed_at=t0,
            )
            verified_tombstone = await memories_repository.create_tombstone(
                session,
                user_id=user_id,
                provider_id="fake-memory",
                target_kind="memory",
                memory_id="mem-verified",
                operation_id=settled_del_op.id,
                status="verified",
            )

            # 4. Failed memory_delete operation with UNVERIFIED tombstone -> PROTECTED!
            unverified_del_op = await operations_service.create_operation_in_session(
                session,
                owner_id=user_id,
                operation_type="memory_delete",
                status="failed",
                target_type="memory",
                target_id="mem-unverified",
                completed_at=t0,
            )
            await memories_repository.create_tombstone(
                session,
                user_id=user_id,
                provider_id="fake-memory",
                target_kind="memory",
                memory_id="mem-unverified",
                operation_id=unverified_del_op.id,
                status="reconciling",
            )

            # 5. Failed memory_clear operation still referenced by active barrier -> PROTECTED!
            barrier_op = await operations_service.create_operation_in_session(
                session,
                owner_id=user_id,
                operation_type="memory_clear",
                status="failed",
                target_type="memory_scope",
                target_id="fake-memory",
                completed_at=t0,
            )
            scope = await memories_repository.get_or_create_scope_for_update(
                session,
                user_id=user_id,
                provider_id="fake-memory",
            )
            scope.epoch = 2
            scope.barrier_state = "reconciling"
            scope.barrier_operation_id = barrier_op.id

        # At t0 + 600s: only the first pagination snapshot expires.
        policy = RetentionPolicy(operation_summary_retention_days=30, batch_size=10)
        at_600s, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(seconds=600),
        )
        assert at_600s.memory_list_snapshots_deleted == 1
        assert at_600s.user_delete_operations_deleted == 0
        assert at_600s.operation_summaries_deleted == 0
        assert await _count_rows(session_factory, MemoryListSnapshot) == 1

        # At t0 + 30d - 1s: receipts and summaries still preserved; second snapshot expires.
        before_30d, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(days=30) - timedelta(seconds=1),
        )
        assert before_30d.memory_list_snapshots_deleted == 1
        assert before_30d.user_delete_operations_deleted == 0
        assert before_30d.operation_summaries_deleted == 0

        # At exact t0 + 30d:
        # - finalized_op (expired user_delete receipt) deleted
        # - settled_del_op deleted, verified tombstone preserved with operation_id=NULL
        # - incomplete_user_op, unverified_del_op, barrier_op ALL preserved
        # - memory_scopes & memory_tombstones ALL preserved
        at_30d, _ = await run_retention_sweep(
            session_factory,
            policy=policy,
            clock=lambda: t0 + timedelta(days=30),
        )
        assert at_30d.user_delete_operations_deleted == 1
        assert at_30d.operation_summaries_deleted == 1
        assert at_30d.protected_skipped == 2  # unverified_del_op + barrier_op

        async with session_factory() as session:
            assert (
                await operations_service.get_operation_in_session(
                    session, operation_id=finalized_op.id
                )
                is None
            )
            assert (
                await operations_service.get_operation_in_session(
                    session, operation_id=settled_del_op.id
                )
                is None
            )
            assert (
                await operations_service.get_operation_in_session(
                    session, operation_id=incomplete_user_op.id
                )
                is not None
            )
            assert (
                await operations_service.get_operation_in_session(
                    session, operation_id=unverified_del_op.id
                )
                is not None
            )
            assert (
                await operations_service.get_operation_in_session(
                    session, operation_id=barrier_op.id
                )
                is not None
            )
            tombstone_after = await memories_repository.get_tombstone_by_id(
                session, tombstone_id=verified_tombstone.id
            )
            assert tombstone_after is not None
            assert tombstone_after.operation_id is None
            assert await _count_rows(session_factory, MemoryScopeState) == 1
            assert await _count_rows(session_factory, MemoryTombstone) == 2
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_worker_periodic_dedupe_batch_interruption_resume_idempotency_and_sanitized_logs(
    migrated_pg_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    retention_logger = logging.getLogger("mem_ai.retention")
    retention_logger.addHandler(log_handler)
    retention_logger.setLevel(logging.INFO)
    try:
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="worker-retention-user",
        )
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        persona = await persona_provider.get_latest_published(
            owner_id=user_id,
            agent_id=agent_id,
        )
        t0 = datetime(2026, 11, 1, 0, 0, 0, tzinfo=UTC)

        # Seed 5 expired idempotency records to force multi-batch scanning (batch_size=2).
        for idx in range(5):
            spec = conversations_idempotency.build_chat_idempotency_spec(
                raw_key=f"SECRET_IDEM_KEY_TOKEN_{idx}",
                agent_id=agent_id,
                conversation_id=None,
                message=f"CONFIDENTIAL_MESSAGE_BODY_{idx}",
                requested_model_id=None,
                retention_seconds=24 * 3600,
            )
            prep = await generation_store.prepare_generation_phase(
                session_factory,
                owner_id=user_id,
                agent_id=agent_id,
                persona=persona,
                conversation_id=None,
                user_message_content=f"CONFIDENTIAL_MESSAGE_BODY_{idx}",
                model_id="retention-model",
                idempotency_spec=spec,
                now=t0 + timedelta(seconds=idx),
            )
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prep,
                assistant_content="done",
                usage=ModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
                idempotency_retention_seconds=24 * 3600,
                now=t0 + timedelta(seconds=idx),
            )

        sweep_time = t0 + timedelta(hours=25)
        policy = RetentionPolicy(batch_size=2, max_batches_per_run=50, cleanup_interval_seconds=900)
        periodic_specs = [
            spec
            for spec in default_periodic_jobs(
                migrated_pg_settings.model_copy(update={"retention_cleanup_interval_seconds": 900})
            )
            if spec.job_type == JOB_TYPE_RETENTION_CLEANUP
        ]
        assert len(periodic_specs) == 1

        # Multi-Worker same-window deduplication: two runners schedule concurrently -> only 1 job.
        runner_a = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(
                session_factory,
                retention_policy=policy,
                clock=lambda: sweep_time,
            ),
            config=WorkerConfig(
                worker_id="retention-worker-a",
                concurrency=2,
                lease_ttl_seconds=60.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(
                    base_delay_seconds=5.0, max_delay_seconds=30.0, jitter_ratio=0.0
                ),
            ),
            periodic_jobs=periodic_specs,
        )
        runner_b = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(
                session_factory,
                retention_policy=policy,
                clock=lambda: sweep_time,
            ),
            config=WorkerConfig(
                worker_id="retention-worker-b",
                concurrency=2,
                lease_ttl_seconds=60.0,
                lease_renew_interval_seconds=None,
                backoff_policy=JobBackoffPolicy(
                    base_delay_seconds=5.0, max_delay_seconds=30.0, jitter_ratio=0.0
                ),
            ),
            periodic_jobs=periodic_specs,
        )
        created_a, created_b = await asyncio.gather(
            runner_a.schedule_periodic_jobs(now=sweep_time),
            runner_b.schedule_periodic_jobs(now=sweep_time),
        )
        assert created_a + created_b == 1

        # Inject a failure on batch #2 to simulate mid-sweep interruption after batch #1 commits.
        original_batch_fn = retention_service.run_retention_batch_in_session
        call_count = 0

        async def _flaky_batch_fn(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("simulated transient db error with SECRET_TOKEN=abc")
            return await original_batch_fn(*args, **kwargs)

        monkeypatch.setattr(retention_service, "run_retention_batch_in_session", _flaky_batch_fn)

        # Attempt 1: commits batch 1 (2 rows + cursor checkpoint), fails on batch 2 -> retry_wait.
        assert await runner_a.run_once(now=sweep_time) == 1
        assert await _count_rows(session_factory, IdempotencyRecord) == 3

        async with session_factory() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(Job).where(Job.job_type == JOB_TYPE_RETENTION_CLEANUP)
                    )
                ).all()
            )
        assert len(jobs) == 1
        interrupted_job = jobs[0]
        assert interrupted_job.status == "retry_wait"
        assert interrupted_job.metadata_json["batches_completed"] == 1
        assert interrupted_job.metadata_json["cursor"] is not None
        assert interrupted_job.metadata_json["totals"]["idempotency_records_deleted"] == 2
        assert "SECRET_TOKEN" not in (interrupted_job.last_error_summary or "")

        # Attempt 2 (from Worker B after backoff): resumes from checkpointed cursor and completes!
        retry_time = sweep_time + timedelta(seconds=10)
        assert await runner_b.run_once(now=retry_time) == 1
        assert await _count_rows(session_factory, IdempotencyRecord) == 0

        completed_job = await jobs_service.get_job(session_factory, job_id=interrupted_job.id)
        assert completed_job is not None
        assert completed_job.status == "succeeded"
        assert completed_job.metadata_json["totals"]["idempotency_records_deleted"] == 5

        # Repeat execution in the next periodic window is completely idempotent (0 deletes).
        next_window = sweep_time + timedelta(seconds=900)
        assert await runner_a.run_once(now=next_window) == 1
        async with session_factory() as session:
            all_retention_jobs = list(
                (
                    await session.scalars(
                        select(Job)
                        .where(Job.job_type == JOB_TYPE_RETENTION_CLEANUP)
                        .order_by(Job.created_at.asc())
                    )
                ).all()
            )
        assert len(all_retention_jobs) == 2
        repeat_attempts = await jobs_service.list_job_attempts(
            session_factory, job_id=all_retention_jobs[-1].id
        )
        assert len(repeat_attempts) == 1
        assert repeat_attempts[0].result_summary["total_deleted"] == 0
        assert repeat_attempts[0].result_summary["completed"] is True
        assert repeat_attempts[0].result_summary["duration_ms"] >= 0.0

        # Logs contain ONLY sanitized counts, duration_ms, and safe failure_summary.
        log_entries = [
            json.loads(line) for line in log_buffer.getvalue().splitlines() if line.strip()
        ]
        assert len(log_entries) >= 3
        raw_log_text = log_buffer.getvalue()
        assert "SECRET_IDEM_KEY_TOKEN" not in raw_log_text
        assert "CONFIDENTIAL_MESSAGE_BODY" not in raw_log_text
        assert "SECRET_TOKEN" not in raw_log_text
        assert str(user_id) not in raw_log_text
        assert REDACTED_VALUE not in raw_log_text
        assert any(
            entry.get("failure_summary") == "stage=idempotency_records error=RuntimeError"
            for entry in log_entries
        )
        assert any(
            entry.get("completed") is True and entry.get("idempotency_records_deleted") == 5
            for entry in log_entries
        )
    finally:
        retention_logger.removeHandler(log_handler)
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_explicit_conversation_and_user_deletion_take_precedence_over_long_term_retention(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="explicit-precedence-user",
        )
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="answer:")
        memory_provider = FakeMemoryProvider()

        conv_a = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="long term conversation A",
            idempotency_key=f"key-a-{uuid4()}",
        )
        conv_b = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="long term conversation B",
            idempotency_key=f"key-b-{uuid4()}",
        )

        # Even 10 years later, default long-term retention keeps conversations and generations.
        far_future = datetime.now(UTC) + timedelta(days=3650)
        summary, _ = await run_retention_sweep(
            session_factory,
            policy=RetentionPolicy(conversation_retention_policy="indefinite"),
            clock=lambda: far_future,
        )
        assert summary.idempotency_records_deleted == 2
        assert await _count_rows(session_factory, Conversation) == 2
        assert await _count_rows(session_factory, Generation) == 2

        # Explicit conversation deletion removes conv_a immediately via Worker.
        conv_op = await conversations_cleanup.delete_conversation_for_owner(
            session_factory,
            owner_id=user_id,
            conversation_id=conv_a.conversation_id,
            memory_provider=memory_provider,
        )
        runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(session_factory, memory_provider=memory_provider),
            config=WorkerConfig(
                worker_id="explicit-del-worker",
                concurrency=2,
                lease_ttl_seconds=60.0,
                lease_renew_interval_seconds=None,
            ),
        )
        step_now = datetime.now(UTC) + timedelta(seconds=2)
        for _ in range(10):
            await runner.run_once(now=step_now)
            step_now += timedelta(seconds=2)
            op = await operations_service.get_operation_for_owner(
                session_factory, owner_id=user_id, operation_id=conv_op.id
            )
            if op.status == "succeeded":
                break
        assert op.status == "succeeded"
        assert await _count_rows(session_factory, Conversation) == 1

        # Explicit user deletion removes conv_b, agents, and user despite long-term policy.
        receipt = await users_cleanup.delete_user_for_owner(
            session_factory,
            owner_id=user_id,
            settings=migrated_pg_settings,
            memory_provider=memory_provider,
        )
        for _ in range(20):
            await runner.run_once(now=step_now)
            step_now += timedelta(seconds=2)
            user_op = await operations_service.get_operation_for_deletion_receipt(
                session_factory,
                operation_id=receipt.operation_id,
                receipt=receipt.deletion_receipt,
                settings=migrated_pg_settings,
                now=step_now,
            )
            if user_op.status == "succeeded":
                break
        assert user_op.status == "succeeded"
        assert await _count_rows(session_factory, Conversation) == 0
        assert await _count_rows(session_factory, Generation) == 0

        # Ensure conv_b is gone and retention cleans up the expired receipt once TTL elapses.
        assert conv_b.conversation_id != conv_a.conversation_id
        after_receipt_ttl = receipt.expires_at + timedelta(seconds=1)
        final_summary, _ = await run_retention_sweep(
            session_factory,
            policy=RetentionPolicy(),
            clock=lambda: after_receipt_ttl,
        )
        assert final_summary.user_delete_operations_deleted == 1
        with pytest.raises(UnauthorizedError):
            await operations_service.get_operation_for_deletion_receipt(
                session_factory,
                operation_id=receipt.operation_id,
                receipt=receipt.deletion_receipt,
                settings=migrated_pg_settings,
                now=after_receipt_ttl,
            )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_retention_lease_loss_fences_stale_worker_and_reclaim_resumes_from_checkpoint(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, _ = await _seed_user_model_and_agent(
            session_factory,
            subject="lease-fence-retention-user",
        )
        t0 = datetime(2026, 11, 1, 0, 0, 0, tzinfo=UTC)
        store = MemorySnapshotStore(session_factory=session_factory, ttl_seconds=60)
        scope_key = MemoryListScopeKey(user_id=user_id, provider_id="fake-memory", scope="user")
        records = [
            MemoryRecord(
                memory_id=f"lease-mem-{idx}",
                user_id=user_id,
                provider_id="fake-memory",
                scope="user",
                content=f"item {idx}",
                created_at=t0 - timedelta(minutes=idx),
            )
            for idx in range(3)
        ]
        for idx in range(4):
            await store.create_first_page(
                scope_key=scope_key,
                records=records,
                limit=1,
                now=t0 + timedelta(seconds=idx),
            )
        assert await _count_rows(session_factory, MemoryListSnapshot) == 4

        sweep_time = t0 + timedelta(minutes=5)
        async with session_factory.begin() as session:
            job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_RETENTION_CLEANUP,
                business_id="window:lease-fence",
                user_id=None,
                dedupe_key="periodic:retention_cleanup:lease-fence",
                metadata_json={},
                now=sweep_time,
            )

        # Worker 1 claims the job, commits 1 batch (2 snapshots) with checkpoint, then stalls.
        claimed_1 = await jobs_service.claim_due_jobs(
            session_factory,
            worker_id="worker-1",
            job_types=(JOB_TYPE_RETENTION_CLEANUP,),
            limit=1,
            lease_ttl_seconds=30.0,
            now=sweep_time,
        )
        assert len(claimed_1) == 1
        partial_policy = RetentionPolicy(batch_size=2, max_batches_per_run=1)
        summary_1, state_1 = await run_retention_sweep(
            session_factory,
            policy=partial_policy,
            clock=lambda: sweep_time,
            job_id=job.id,
            lease_token=claimed_1[0].lease_token,
        )
        assert summary_1.completed is False
        assert summary_1.memory_list_snapshots_deleted == 0
        # Advance through empty earlier stages until memory_list_snapshots deletes 2 rows.
        while state_1.totals["memory_list_snapshots_deleted"] == 0:
            _, state_1 = await run_retention_sweep(
                session_factory,
                policy=partial_policy,
                clock=lambda: sweep_time,
                initial_state=state_1,
                job_id=job.id,
                lease_token=claimed_1[0].lease_token,
            )
        assert state_1.totals["memory_list_snapshots_deleted"] == 2
        assert state_1.cursor is not None
        assert await _count_rows(session_factory, MemoryListSnapshot) == 2

        # Lease expires and Worker 2 reclaims the job.
        reclaim_time = sweep_time + timedelta(seconds=35)
        runner_2 = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(
                session_factory,
                retention_policy=RetentionPolicy(batch_size=2, max_batches_per_run=20),
                clock=lambda: reclaim_time,
            ),
            config=WorkerConfig(
                worker_id="worker-2",
                concurrency=1,
                lease_ttl_seconds=60.0,
                lease_renew_interval_seconds=None,
            ),
        )

        # Stale Worker 1 attempting another batch with its old lease token is fenced with 409.
        with pytest.raises(ConflictError) as exc_info:
            await run_retention_sweep(
                session_factory,
                policy=partial_policy,
                clock=lambda: reclaim_time,
                initial_state=state_1,
                job_id=job.id,
                lease_token=claimed_1[0].lease_token,
            )
        assert exc_info.value.code == "job_lease_lost"
        # Uncommitted batch rolled back cleanly -> still 2 snapshots left!
        assert await _count_rows(session_factory, MemoryListSnapshot) == 2

        # Worker 2 reclaims and resumes from Worker 1's committed cursor to finish the remaining 2!
        assert await runner_2.run_once(now=reclaim_time) == 1
        assert await _count_rows(session_factory, MemoryListSnapshot) == 0
        finished = await jobs_service.get_job(session_factory, job_id=job.id)
        assert finished is not None
        assert finished.status == "succeeded"
        assert finished.metadata_json["totals"]["memory_list_snapshots_deleted"] == 4
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_budget_capped_worker_retention_yields_reconciling_and_completes_later_stages(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="budget-cap-retention-user",
        )
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        persona = await persona_provider.get_latest_published(
            owner_id=user_id,
            agent_id=agent_id,
        )
        t0 = datetime(2026, 11, 1, 0, 0, 0, tzinfo=UTC)

        # Seed 3 expired idempotency records (stage 0) AND 2 expired snapshots (stage 2).
        for idx in range(3):
            spec = conversations_idempotency.build_chat_idempotency_spec(
                raw_key=f"budget-key-{idx}",
                agent_id=agent_id,
                conversation_id=None,
                message=f"budget msg {idx}",
                requested_model_id=None,
                retention_seconds=24 * 3600,
            )
            prep = await generation_store.prepare_generation_phase(
                session_factory,
                owner_id=user_id,
                agent_id=agent_id,
                persona=persona,
                conversation_id=None,
                user_message_content=f"budget msg {idx}",
                model_id="retention-model",
                idempotency_spec=spec,
                now=t0 + timedelta(seconds=idx),
            )
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prep,
                assistant_content="ok",
                usage=ModelUsage(input_tokens=2, output_tokens=1, total_tokens=3),
                idempotency_retention_seconds=24 * 3600,
                now=t0 + timedelta(seconds=idx),
            )

        store = MemorySnapshotStore(session_factory=session_factory, ttl_seconds=60)
        scope_key = MemoryListScopeKey(user_id=user_id, provider_id="fake-memory", scope="user")
        records = [
            MemoryRecord(
                memory_id=f"budget-mem-{idx}",
                user_id=user_id,
                provider_id="fake-memory",
                scope="user",
                content=f"fact {idx}",
                created_at=t0 - timedelta(minutes=idx),
            )
            for idx in range(3)
        ]
        for idx in range(2):
            await store.create_first_page(
                scope_key=scope_key,
                records=records,
                limit=1,
                now=t0 + timedelta(seconds=idx),
            )

        sweep_time = t0 + timedelta(hours=25)
        async with session_factory.begin() as session:
            job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type=JOB_TYPE_RETENTION_CLEANUP,
                business_id="window:budget-cap",
                user_id=None,
                dedupe_key="periodic:retention_cleanup:budget-cap",
                metadata_json={},
                now=sweep_time,
            )

        # Strict budget: max_batches_per_run=1, batch_size=2 -> must yield reconciling & resume.
        capped_policy = RetentionPolicy(batch_size=2, max_batches_per_run=1)
        current_now = sweep_time
        runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry(
                session_factory,
                retention_policy=capped_policy,
                clock=lambda: current_now,
            ),
            config=WorkerConfig(
                worker_id="budget-cap-worker",
                concurrency=1,
                lease_ttl_seconds=60.0,
                lease_renew_interval_seconds=None,
            ),
        )

        # First pass processes only 1 batch (2 of 3 idempotency rows) and yields `reconciling`.
        assert await runner.run_once(now=current_now) == 1
        mid_job = await jobs_service.get_job(session_factory, job_id=job.id)
        assert mid_job is not None
        assert mid_job.status == "reconciling"
        assert mid_job.attempt_count == 1
        assert await _count_rows(session_factory, IdempotencyRecord) == 1
        assert await _count_rows(session_factory, MemoryListSnapshot) == 2

        # Subsequent passes continue from checkpoint without consuming retry budget until succeeded.
        for _ in range(10):
            current_now += timedelta(seconds=2)
            await runner.run_once(now=current_now)
            state_job = await jobs_service.get_job(session_factory, job_id=job.id)
            assert state_job is not None
            if state_job.status == "succeeded":
                break

        final_job = await jobs_service.get_job(session_factory, job_id=job.id)
        assert final_job is not None
        assert final_job.status == "succeeded"
        assert final_job.attempt_count == 1
        assert await _count_rows(session_factory, IdempotencyRecord) == 0
        assert await _count_rows(session_factory, MemoryListSnapshot) == 0
        assert final_job.metadata_json["totals"]["idempotency_records_deleted"] == 3
        assert final_job.metadata_json["totals"]["memory_list_snapshots_deleted"] == 2
    finally:
        await engine.dispose()
