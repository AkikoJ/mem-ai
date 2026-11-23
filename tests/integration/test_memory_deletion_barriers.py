"""PostgreSQL integration tests for memory tombstones, epoch barriers, and Worker convergence."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryRecord,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError, GatewayTimeoutError, NotFoundError
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.queries import (
    DatabaseRecalledMemoryReferenceMarker,
    list_recalled_memories_for_message_for_owner,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobBackoffPolicy
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.jobs import (
    JOB_TYPE_MEMORY_CLEAR,
    JOB_TYPE_MEMORY_DELETE,
    JOB_TYPE_MEMORY_EXPLICIT_CREATE,
    JOB_TYPE_MEMORY_TURN_WRITE,
)
from mem_ai.modules.memories.schemas import (
    AcceptedMemoryDeletionOutcome,
    AcceptedMemoryOutcome,
    CompletedMemoryDeletionOutcome,
)
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.audit import JsonlDeletionAuditArchiver
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.users import service as users_service
from mem_ai.workers.main import build_worker_registry
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.conftest import FakeModelProvider


class _ControlledMemoryProvider:
    """Controlled MemoryProvider scripting writes, deletes, clears, verifies, and gates."""

    provider_id = "controlled-memory"

    def __init__(
        self,
        *,
        supports_write_status_verification: bool = True,
        write_outcomes: list[MemoryWriteResult | Exception] | None = None,
        delete_outcomes: list[MemoryDeleteResult | Exception] | None = None,
        clear_outcomes: list[MemoryDeleteResult | Exception] | None = None,
        verify_outcomes: list[MemoryOperationStatus | Exception] | None = None,
    ) -> None:
        self.supports_write_status_verification = supports_write_status_verification
        self.records: dict[str, MemoryRecord] = {}
        self._write_outcomes = list(write_outcomes or [])
        self._delete_outcomes = list(delete_outcomes or [])
        self._clear_outcomes = list(clear_outcomes or [])
        self._verify_outcomes = list(verify_outcomes or [])
        self.writes: list[MemoryWriteInput] = []
        self.deletes: list[tuple[UUID, str]] = []
        self.clears: list[UUID] = []
        self.verifies: list[tuple[UUID, str]] = []
        self.write_started = asyncio.Event()
        self.release_write = asyncio.Event()
        self.gate_writes = False
        self.clear_started = asyncio.Event()
        self.release_clear = asyncio.Event()
        self.gate_clears = False

    def seed(self, *, user_id: UUID, memory_id: str, content: str, epoch: int = 0) -> None:
        self.records[memory_id] = MemoryRecord(
            memory_id=memory_id,
            provider_id=self.provider_id,
            user_id=user_id,
            scope="user",
            content=content,
            conversation_id=None,
            memory_epoch=epoch,
            created_at=datetime.now(UTC),
        )

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        matched: list[MemoryRecallItem] = []
        for rec in self.records.values():
            if rec.user_id != query.user_id or rec.scope != query.scope:
                continue
            if query.scope == "conversation" and rec.conversation_id != query.conversation_id:
                continue
            matched.append(
                MemoryRecallItem(
                    memory_id=rec.memory_id,
                    provider_id=self.provider_id,
                    scope=rec.scope,
                    content=rec.content,
                    relevance=0.92,
                    conversation_id=rec.conversation_id,
                    memory_epoch=rec.memory_epoch,
                )
            )
        return matched[: query.limit]

    async def _do_write(self, item: MemoryWriteInput) -> MemoryWriteResult:
        self.writes.append(item)
        self.write_started.set()
        if self.gate_writes:
            await self.release_write.wait()
        if self._write_outcomes:
            nxt = self._write_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            if nxt.status == "succeeded" and nxt.memory_id is not None:
                self.seed(
                    user_id=item.user_id,
                    memory_id=nxt.memory_id,
                    content=item.content,
                    epoch=item.memory_epoch,
                )
            return nxt
        mid = f"mem_{uuid4().hex[:10]}"
        self.seed(
            user_id=item.user_id, memory_id=mid, content=item.content, epoch=item.memory_epoch
        )
        return MemoryWriteResult(
            status="succeeded",
            provider_id=self.provider_id,
            memory_id=mid,
            external_ref=item.write_id,
        )

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self._do_write(item)

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self._do_write(item)

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        items = [
            rec
            for rec in self.records.values()
            if rec.user_id == query.user_id
            and (query.scope is None or rec.scope == query.scope)
            and (query.conversation_id is None or rec.conversation_id == query.conversation_id)
        ]
        items.sort(key=lambda r: (r.created_at, r.memory_id), reverse=True)
        return MemoryListPage(items=items[: query.limit], next_cursor=None, has_more=False)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        self.deletes.append((user_id, memory_id))
        if self._delete_outcomes:
            nxt = self._delete_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            if nxt.status == "succeeded":
                rec = self.records.get(memory_id)
                if rec is not None and rec.user_id == user_id:
                    del self.records[memory_id]
            return nxt
        rec = self.records.get(memory_id)
        if rec is None or rec.user_id != user_id:
            return MemoryDeleteResult(
                status="failed",
                external_ref=f"delete_memory:{memory_id}",
                error_code="memory_not_found",
            )
        del self.records[memory_id]
        return MemoryDeleteResult(status="succeeded", external_ref=f"delete_memory:{memory_id}")

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        self.clears.append(user_id)
        self.clear_started.set()
        if self.gate_clears:
            await self.release_clear.wait()
        if self._clear_outcomes:
            nxt = self._clear_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            if nxt.status == "succeeded":
                for mid in [k for k, v in self.records.items() if v.user_id == user_id]:
                    del self.records[mid]
            return nxt
        for mid in [k for k, v in self.records.items() if v.user_id == user_id]:
            del self.records[mid]
        return MemoryDeleteResult(status="succeeded", external_ref=f"clear_memories:{user_id}")

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        self.verifies.append((user_id, external_ref))
        if self._verify_outcomes:
            nxt = self._verify_outcomes.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt
        if not self.supports_write_status_verification and not external_ref.startswith(
            ("delete_memory:", "clear_memories:")
        ):
            return MemoryOperationStatus(status="reconciling", external_ref=external_ref)
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


async def _seed_user_model_and_agent(
    session_factory: Any,
    *,
    subject: str,
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
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest.model_validate(
            {
                "name": f"Agent-{subject}",
                "systemPrompt": "You are helpful.",
                "defaultModelId": "test-model",
                "memoryPolicy": {
                    "enabled": True,
                    "writeEnabled": True,
                    "writeScope": "user",
                    "recallLimit": 5,
                },
            }
        ),
    )
    await agents_service.publish_agent(session_factory, owner_id=user.id, agent_id=agent.agent_id)
    return user.id, agent.agent_id


def _make_worker(
    session_factory: Any,
    *,
    memory_provider: _ControlledMemoryProvider,
    inflight_settle_timeout_seconds: float = 60.0,
    compensation_delay_seconds: float = 30.0,
    audit_archiver: JsonlDeletionAuditArchiver | None = None,
    clock: Any = None,
) -> WorkerRunner:
    registry = build_worker_registry(
        session_factory,
        memory_provider=memory_provider,
        memory_inflight_settle_timeout_seconds=inflight_settle_timeout_seconds,
        memory_clear_compensation_delay_seconds=compensation_delay_seconds,
        audit_archiver=audit_archiver,
        clock=clock,
    )
    return WorkerRunner(
        session_factory=session_factory,
        registry=registry,
        config=WorkerConfig(
            worker_id=f"worker-t13-{uuid4().hex[:6]}",
            concurrency=2,
            lease_ttl_seconds=30.0,
            lease_renew_interval_seconds=None,
            backoff_policy=JobBackoffPolicy(
                base_delay_seconds=2.0,
                max_delay_seconds=10.0,
                jitter_ratio=0.0,
                reconcile_delay_seconds=5.0,
            ),
        ),
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_single_delete_tombstone_filters_recall_marks_refs_and_reconciles(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    audit_path = tmp_path / "single-delete-audit.jsonl"
    archiver = JsonlDeletionAuditArchiver(audit_path)
    marker = DatabaseRecalledMemoryReferenceMarker()
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory, subject="alice-single-del"
        )
        bob_id, _ = await _seed_user_model_and_agent(session_factory, subject="bob-single-del")

        provider = _ControlledMemoryProvider()
        provider.seed(user_id=alice_id, memory_id="mem_keep", content="Alice loves green tea")
        provider.seed(user_id=alice_id, memory_id="mem_del", content="Alice old address is 123 St")

        # 1. Run a chat turn so both memories are recalled and persisted as refs.
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="reply:")
        turn1 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="What do you remember about me?",
        )
        gen1 = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            generation_id=turn1.generation_id,
        )
        assert gen1.user_message_id is not None
        refs_before = await list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            user_message_id=gen1.user_message_id,
        )
        assert {r.memory_id for r in refs_before} == {"mem_keep", "mem_del"}
        assert all(r.deleted_at is None for r in refs_before)

        # 2. Bob attempting to delete Alice's memory fails with 404 and leaves Alice untouched.
        with pytest.raises(NotFoundError) as bob_err:
            await memories_service.delete_single_memory(
                session_factory,
                memory_provider=provider,
                user_id=bob_id,
                memory_id="mem_del",
                recalled_memory_marker=marker,
                audit_archiver=archiver,
            )
        assert bob_err.value.code == "memory_not_found"
        assert "mem_del" in provider.records

        # 3. Simulate lost response on Alice's single delete -> 202 reconciling, tombstone
        #    IMMEDIATELY filters recall & list, and marks historical ref deleted_at!
        provider._delete_outcomes = [
            GatewayTimeoutError(code="memory_provider_timeout", message="dropped response")
        ]
        outcome = await memories_service.delete_single_memory(
            session_factory,
            memory_provider=provider,
            user_id=alice_id,
            memory_id="mem_del",
            recalled_memory_marker=marker,
            audit_archiver=archiver,
            reconcile_delay_seconds=5.0,
        )
        assert isinstance(outcome, AcceptedMemoryDeletionOutcome)
        assert outcome.operation.status == "reconciling"
        op_id = outcome.operation.id

        # Duplicate delete while reconciling returns the exact same active operation!
        dup_outcome = await memories_service.delete_single_memory(
            session_factory,
            memory_provider=provider,
            user_id=alice_id,
            memory_id="mem_del",
            recalled_memory_marker=marker,
        )
        assert isinstance(dup_outcome, AcceptedMemoryDeletionOutcome)
        assert dup_outcome.operation.id == op_id

        # Even though provider still holds `mem_del` (timeout before delete), tombstone filters it
        # from recall AND list_memories immediately!
        recall_after = await memories_service.recall_relevant_memories(
            session_factory,
            memory_provider=provider,
            user_id=alice_id,
            conversation_id=turn1.conversation_id,
            query="Where do I live?",
        )
        assert [item.memory_id for item in recall_after.items] == ["mem_keep"]
        listed = await memories_service.list_memories(
            session_factory, memory_provider=provider, user_id=alice_id
        )
        assert [item.memory_id for item in listed.items] == ["mem_keep"]

        # Historical reference row is preserved and marked deleted_at.
        refs_after = await list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            user_message_id=gen1.user_message_id,
        )
        by_mid = {r.memory_id: r for r in refs_after}
        assert by_mid["mem_del"].deleted_at is not None
        assert by_mid["mem_keep"].deleted_at is None

        # 4. Worker verifies delete: first verify says `failed` (still present) -> retries
        #    `delete_memory` -> succeeds -> tombstone verified & operation succeeded!
        provider._verify_outcomes = [
            MemoryOperationStatus(
                status="failed",
                external_ref="delete_memory:mem_del",
                error_code="memory_still_present",
            )
        ]
        worker = _make_worker(session_factory, memory_provider=provider, audit_archiver=archiver)
        t1 = datetime.now(UTC) + timedelta(seconds=10)
        assert await worker.run_once(now=t1) >= 1
        # Retry scheduled -> run again after backoff (+2s)
        t2 = t1 + timedelta(seconds=5)
        assert await worker.run_once(now=t2) >= 1

        final_op = await operations_service.get_operation_for_owner(
            session_factory, owner_id=alice_id, operation_id=op_id
        )
        assert final_op.status == "succeeded"
        assert "mem_del" not in provider.records

        # Idempotent repeat delete after verified returns CompletedMemoryDeletionOutcome (204).
        idempotent = await memories_service.delete_single_memory(
            session_factory,
            memory_provider=provider,
            user_id=alice_id,
            memory_id="mem_del",
            recalled_memory_marker=marker,
        )
        assert isinstance(idempotent, CompletedMemoryDeletionOutcome)

        # Audit JSONL contains created + verified + succeeded events.
        events = [json.loads(line) for line in audit_path.read_text("utf-8").splitlines() if line]
        event_types = [e["event_type"] for e in events]
        assert "memory_tombstone_created" in event_types
        assert "memory_tombstone_verified" in event_types
        assert any(
            e["event_type"] == "operation_transitioned" and e["status"] == "succeeded"
            for e in events
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_clear_barrier_cancels_queued_suppresses_old_generation_and_converges_inflight_writes(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    audit_path = tmp_path / "clear-audit.jsonl"
    archiver = JsonlDeletionAuditArchiver(audit_path)
    marker = DatabaseRecalledMemoryReferenceMarker()
    try:
        user_id, agent_id = await _seed_user_model_and_agent(
            session_factory, subject="alice-clear-race"
        )
        persona_provider = LocalPersonaProvider(session_factory=session_factory)
        model_provider = FakeModelProvider(prefix="reply:")
        provider = _ControlledMemoryProvider()
        provider.seed(user_id=user_id, memory_id="mem_initial", content="Initial epoch-0 memory")

        # 1. Completed turn #1 enqueues a queued (pending) epoch-0 turn write job.
        await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Turn 1 queued write",
        )
        # 2. Completed turn #2 will be claimed & gated mid-flight inside Worker!
        turn2 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Turn 2 in-flight write",
        )
        # 3. Old sync AND stream generations start at epoch 0 BEFORE clear, finish AFTER clear!
        old_gen = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Old sync generation started before clear",
        )
        assert old_gen.memory_epoch == 0
        old_stream_gen = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Old stream generation started before clear",
        )
        assert old_stream_gen.memory_epoch == 0

        # Claim only turn #2's job (by running one worker task gated on `release_write`).
        all_turn_jobs = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_TURN_WRITE
        )
        assert len(all_turn_jobs) == 2

        provider.gate_writes = True
        worker_writer = _make_worker(session_factory, memory_provider=provider)
        inflight_turn_task = asyncio.create_task(worker_writer.run_once(max_jobs=1))
        await asyncio.wait_for(provider.write_started.wait(), timeout=5.0)

        # Also start an explicit create concurrently gated mid-flight!
        explicit_task = asyncio.create_task(
            memories_service.create_explicit_memory(
                session_factory,
                memory_provider=provider,
                user_id=user_id,
                content="Concurrent explicit memory during clear race",
            )
        )
        # Give explicit create time to pass Tx 1 and hit the gated provider call.
        for _ in range(50):
            if len(provider.writes) >= 2:
                break
            await asyncio.sleep(0.01)
        assert len(provider.writes) == 2

        # 4. Now issue `clear_user_memories(confirm=True)` while:
        #    - 1 turn write is still queued (pending)
        #    - 1 turn write is in flight (running)
        #    - 1 explicit create is in flight (running)
        #    - 1 old generation is still generating (epoch=0)
        clear_outcome = await memories_service.clear_user_memories(
            session_factory,
            memory_provider=provider,
            user_id=user_id,
            confirm=True,
            recalled_memory_marker=marker,
            audit_archiver=archiver,
        )
        assert isinstance(clear_outcome, AcceptedMemoryDeletionOutcome)
        clear_op_id = clear_outcome.operation.id
        assert clear_outcome.operation.status == "pending"
        assert clear_outcome.operation.progress["previous_epoch"] == 0
        assert clear_outcome.operation.progress["new_epoch"] == 1
        assert clear_outcome.operation.progress["cancelled_queued_writes"] == 1
        assert clear_outcome.operation.progress["inflight_writes_remaining"] == 2

        # Duplicate clear returns the exact same active operation without bumping epoch again!
        dup_clear = await memories_service.clear_user_memories(
            session_factory,
            memory_provider=provider,
            user_id=user_id,
            confirm=True,
            recalled_memory_marker=marker,
        )
        assert isinstance(dup_clear, AcceptedMemoryDeletionOutcome)
        assert dup_clear.operation.id == clear_op_id

        # During barrier:
        # - Recall degrades cleanly to empty
        degraded_recall = await memories_service.recall_relevant_memories(
            session_factory,
            memory_provider=provider,
            user_id=user_id,
            conversation_id=turn2.conversation_id,
            query="Recall during barrier",
        )
        assert degraded_recall.items == ()
        assert degraded_recall.degraded is True
        assert degraded_recall.degradation_reason == "memory_scope_clearing"

        # - Explicit create is rejected with 409 memory_scope_clearing
        with pytest.raises(ConflictError) as clearing_err:
            await memories_service.create_explicit_memory(
                session_factory,
                memory_provider=provider,
                user_id=user_id,
                content="Should be blocked by barrier",
            )
        assert clearing_err.value.code == "memory_scope_clearing"

        # - Old sync & stream generations (epoch=0) finish: replies succeed, writes SUPPRESSED!
        old_gen_result = await conversations_service.complete_prepared_sync_generation(
            session_factory,
            model_provider=model_provider,
            memory_provider=provider,
            prepared=old_gen,
        )
        assert old_gen_result.assistant_message.content.startswith("reply:")
        stream_deltas: list[str] = []
        old_stream_result = await conversations_service.complete_prepared_stream_generation(
            session_factory,
            model_provider=model_provider,
            memory_provider=provider,
            prepared=old_stream_gen,
            on_delta=lambda delta: stream_deltas.append(delta),
        )
        assert old_stream_result.assistant_message.content.startswith("reply:")
        assert "".join(stream_deltas) == old_stream_result.assistant_message.content
        turn_jobs_after_old_gen = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_TURN_WRITE
        )
        assert all(
            j.business_id not in (str(old_gen.generation_id), str(old_stream_gen.generation_id))
            for j in turn_jobs_after_old_gen
        )

        # - New chat during barrier succeeds (degraded recall) AND suppresses new turn write!
        during_barrier_chat = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=provider,
            owner_id=user_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Chat while barrier is active",
        )
        assert during_barrier_chat.assistant_message.content.startswith("reply:")
        turn_jobs_during = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_TURN_WRITE
        )
        assert all(
            j.business_id != str(during_barrier_chat.generation_id) for j in turn_jobs_during
        )

        # 5. Release both late in-flight writes -> they complete on provider AFTER clear started!
        provider.release_write.set()
        await inflight_turn_task
        explicit_res = await explicit_task
        assert isinstance(explicit_res, AcceptedMemoryOutcome)
        assert explicit_res.operation.status == "reconciling"
        assert len(provider.records) == 3  # initial + late turn + late explicit

        # 6. Worker runs `memory_clear`: both late writes are `confirmed_during_clear` ->
        #    executes final remote `clear_memories`, wipes all 3 records, releases barrier!
        worker_clear = _make_worker(
            session_factory, memory_provider=provider, audit_archiver=archiver
        )
        assert await worker_clear.run_once(now=datetime.now(UTC) + timedelta(seconds=1)) >= 1

        clear_op_final = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_id, operation_id=clear_op_id
        )
        assert clear_op_final.status == "succeeded"
        assert provider.records == {}

        # Superseded explicit create operation converged to `failed` (`memory_scope_cleared`).
        explicit_op_final = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_id, operation_id=explicit_res.operation.id
        )
        assert explicit_op_final.status == "failed"
        assert explicit_op_final.error_code == "memory_scope_cleared"

        # After barrier release, new epoch-1 explicit memory creation succeeds!
        post_clear = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=provider,
            user_id=user_id,
            content="Fresh epoch-1 memory after clear",
        )
        assert post_clear.kind == "created"
        assert post_clear.record.memory_epoch == 1

        # Audit JSONL contains epoch advanced + barrier released.
        events = [json.loads(line) for line in audit_path.read_text("utf-8").splitlines() if line]
        assert any(
            e["event_type"] == "memory_epoch_advanced" and e["memory_epoch"] == 1 for e in events
        )
        assert any(
            e["event_type"] == "memory_barrier_released" and e["barrier_state"] == "idle"
            for e in events
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_clear_verifies_lost_response_write_and_handles_no_verify_settle_timeout_compensation(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    log_buf = StringIO()
    log_handler = logging.StreamHandler(log_buf)
    log_handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger("mem_ai")
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)
    try:
        # Scenario A: Provider WITH per-write status verification waits for lost-response write
        # to stabilize via `verify_operation`, then executes final clear and releases barrier.
        user_a, _ = await _seed_user_model_and_agent(session_factory, subject="alice-verifiable")
        provider_a = _ControlledMemoryProvider(
            supports_write_status_verification=True,
            write_outcomes=[
                GatewayTimeoutError(code="memory_provider_timeout", message="response lost")
            ],
            verify_outcomes=[
                MemoryOperationStatus(status="reconciling", external_ref="ext_a"),
                MemoryOperationStatus(
                    status="succeeded", external_ref="ext_a", memory_id="mem_late_a"
                ),
            ],
        )
        explicit_a = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=provider_a,
            user_id=user_a,
            content="Write with lost response",
        )
        assert isinstance(explicit_a, AcceptedMemoryOutcome)
        assert explicit_a.operation.status == "reconciling"
        provider_a.seed(user_id=user_a, memory_id="mem_late_a", content="Appeared upstream")

        t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
        clear_a = await memories_service.clear_user_memories(
            session_factory, memory_provider=provider_a, user_id=user_a, confirm=True, now=t0
        )
        assert isinstance(clear_a, AcceptedMemoryDeletionOutcome)

        now_ref_a = [t0 + timedelta(seconds=1)]
        worker_a = _make_worker(
            session_factory, memory_provider=provider_a, clock=lambda: now_ref_a[0]
        )
        # Poll 1: verify_operation returns reconciling -> barrier stays reconciling, clear NOT run.
        await worker_a.run_once(now=now_ref_a[0])
        op_a1 = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_a, operation_id=clear_a.operation.id
        )
        assert op_a1.status == "reconciling"
        assert provider_a.clears == []

        # Poll 2: verify_operation returns succeeded (settling the lost-response write) ->
        # subsequent clear cycle runs final clear & releases barrier!
        now_ref_a[0] = t0 + timedelta(seconds=10)
        await worker_a.run_once(now=now_ref_a[0])
        now_ref_a[0] = t0 + timedelta(seconds=25)
        await worker_a.run_once(now=now_ref_a[0])
        op_a2 = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_a, operation_id=clear_a.operation.id
        )
        assert op_a2.status == "succeeded"
        assert len(provider_a.clears) == 1
        assert provider_a.records == {}

        # Scenario B: Provider WITHOUT per-write status verification reaches
        # `memory_inflight_settle_timeout`, logs reconciling alert, runs first clear + delayed
        # secondary compensation clear, releases barrier, and preserves reconciling write job!
        user_b, _ = await _seed_user_model_and_agent(session_factory, subject="bob-no-verify")
        provider_b = _ControlledMemoryProvider(
            supports_write_status_verification=False,
            write_outcomes=[
                GatewayTimeoutError(code="memory_provider_timeout", message="response lost")
            ],
        )
        explicit_b = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=provider_b,
            user_id=user_b,
            content="Uncertain write on non-verifiable provider",
        )
        assert isinstance(explicit_b, AcceptedMemoryOutcome)
        explicit_b_jobs = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_b, job_type=JOB_TYPE_MEMORY_EXPLICIT_CREATE
        )
        assert len(explicit_b_jobs) == 1
        unsettled_job_id = explicit_b_jobs[0].id

        clear_b = await memories_service.clear_user_memories(
            session_factory, memory_provider=provider_b, user_id=user_b, confirm=True, now=t0
        )
        assert isinstance(clear_b, AcceptedMemoryDeletionOutcome)

        now_ref_b = [t0 + timedelta(seconds=5)]
        worker_b = _make_worker(
            session_factory,
            memory_provider=provider_b,
            inflight_settle_timeout_seconds=20.0,
            compensation_delay_seconds=15.0,
            clock=lambda: now_ref_b[0],
        )
        # Before settle timeout (+5s < 20s): still waiting, no clear called yet.
        await worker_b.run_once(now=now_ref_b[0])
        assert provider_b.clears == []
        op_b1 = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_b, operation_id=clear_b.operation.id
        )
        assert op_b1.status == "running"

        # Past settle timeout (+25s >= 20s): first clear runs, schedules delayed compensation clear!
        now_ref_b[0] = t0 + timedelta(seconds=25)
        await worker_b.run_once(now=now_ref_b[0])
        assert len(provider_b.clears) == 1
        op_b2 = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_b, operation_id=clear_b.operation.id
        )
        assert op_b2.status == "reconciling"
        assert op_b2.progress["compensation_scheduled"] is True

        # Late memory arrives between first clear and delayed secondary compensation clear!
        provider_b.seed(user_id=user_b, memory_id="mem_late_b", content="Late extraction")

        # Past compensation delay (+45s): secondary compensation clear runs, wipes late memory,
        # releases barrier, and preserves timed-out write job in `reconciling` for audit!
        now_ref_b[0] = t0 + timedelta(seconds=45)
        await worker_b.run_once(now=now_ref_b[0])
        assert len(provider_b.clears) == 2
        assert provider_b.records == {}
        op_b3 = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_b, operation_id=clear_b.operation.id
        )
        assert op_b3.status == "succeeded"
        assert op_b3.result_summary["compensation_executed"] is True
        assert str(unsettled_job_id) in op_b3.result_summary["unsettled_write_job_ids"]

        audited_job = await jobs_service.get_job(session_factory, job_id=unsettled_job_id)
        assert audited_job is not None and audited_job.status == "reconciling"
        assert audited_job.metadata_json.get("clear_settle_timeout_alerted") is True

        # Barrier is released so user B is NOT permanently deadlocked!
        new_b = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=provider_b,
            user_id=user_b,
            content="Fresh post-compensation memory",
        )
        assert new_b.kind == "created"
        assert new_b.record.memory_epoch == 1

        logs = [json.loads(line) for line in log_buf.getvalue().splitlines() if line.strip()]
        assert any(entry.get("alert") == "memory_clear_settle_timeout_exceeded" for entry in logs)
    finally:
        root_logger.removeHandler(log_handler)
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_unknown_final_clear_holds_barrier_reconciling_and_recovers_after_worker_restart(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    log_buf = StringIO()
    log_handler = logging.StreamHandler(log_buf)
    log_handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger("mem_ai")
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)
    try:
        user_id, _ = await _seed_user_model_and_agent(
            session_factory, subject="carol-clear-crash-recovery"
        )
        provider = _ControlledMemoryProvider(
            clear_outcomes=[
                GatewayTimeoutError(code="memory_provider_timeout", message="clear response lost"),
            ],
            verify_outcomes=[
                MemoryOperationStatus(
                    status="reconciling", external_ref=f"clear_memories:{user_id}"
                ),
                MemoryOperationStatus(
                    status="failed",
                    external_ref=f"clear_memories:{user_id}",
                    error_code="memories_still_present",
                ),
            ],
        )
        provider.seed(user_id=user_id, memory_id="mem_c1", content="Carol memory")

        t0 = datetime(2026, 11, 23, 14, 0, 0, tzinfo=UTC)
        outcome = await memories_service.clear_user_memories(
            session_factory,
            memory_provider=provider,
            user_id=user_id,
            confirm=True,
            reconcile_delay_seconds=5.0,
            now=t0,
        )
        assert isinstance(outcome, AcceptedMemoryDeletionOutcome)
        assert outcome.operation.status == "reconciling"
        op_id = outcome.operation.id

        # Explicit create remains blocked while final clear is reconciling.
        with pytest.raises(ConflictError) as blocked:
            await memories_service.create_explicit_memory(
                session_factory,
                memory_provider=provider,
                user_id=user_id,
                content="Blocked while final clear reconciling",
            )
        assert blocked.value.code == "memory_scope_clearing"

        # Worker #1 polls verify -> still unknown (`reconciling`), barrier remains held.
        worker1 = _make_worker(
            session_factory, memory_provider=provider, clock=lambda: t0 + timedelta(seconds=6)
        )
        await worker1.run_once(now=t0 + timedelta(seconds=6))
        op_mid = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_id, operation_id=op_id
        )
        assert op_mid.status == "reconciling"

        # Simulated Worker restart: fresh Worker #2 polls verify -> `failed` (still present) ->
        # retries `clear_memories` -> succeeds & releases barrier!
        worker2 = _make_worker(
            session_factory, memory_provider=provider, clock=lambda: t0 + timedelta(seconds=15)
        )
        await worker2.run_once(now=t0 + timedelta(seconds=15))
        worker3 = _make_worker(
            session_factory, memory_provider=provider, clock=lambda: t0 + timedelta(seconds=20)
        )
        await worker3.run_once(now=t0 + timedelta(seconds=20))

        op_done = await operations_service.get_operation_for_owner(
            session_factory, owner_id=user_id, operation_id=op_id
        )
        assert op_done.status == "succeeded"
        assert provider.records == {}

        # Verify crash recovery of `memory_clear` job claimed and abandoned mid-dispatch.
        clear_jobs = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_CLEAR
        )
        assert len(clear_jobs) == 1
        assert clear_jobs[0].status == "succeeded"
        delete_jobs = await jobs_service.list_jobs_for_user(
            session_factory, user_id=user_id, job_type=JOB_TYPE_MEMORY_DELETE
        )
        assert delete_jobs == []

        logs = [json.loads(line) for line in log_buf.getvalue().splitlines() if line.strip()]
        assert any(entry.get("alert") == "memory_clear_barrier_reconciling" for entry in logs)
    finally:
        root_logger.removeHandler(log_handler)
        await engine.dispose()
