"""Integration tests for jobs, operations, memory scopes, and pagination snapshot persistence."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import text

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
from mem_ai.core.exceptions import ConflictError, NotFoundError, UnprocessableEntityError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.schemas import AcceptedMemoryOutcome, CreatedMemoryOutcome
from mem_ai.modules.memories.snapshots import MemoryListScopeKey, MemorySnapshotStore
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.users import service as users_service


class _ControlledMemoryProvider:
    """Stub MemoryProvider with configurable sync or async explicit-create outcomes."""

    provider_id = "controlled-memory"

    def __init__(
        self,
        result_factory: Callable[
            [MemoryWriteInput], MemoryWriteResult | Awaitable[MemoryWriteResult]
        ],
    ) -> None:
        self._result_factory = result_factory
        self.received_inputs: list[MemoryWriteInput] = []

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        return []

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        self.received_inputs.append(item)
        result = self._result_factory(item)
        if isinstance(result, MemoryWriteResult):
            return result
        return await result

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self.create_memory(item)

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        return MemoryListPage(items=[], next_cursor=None, has_more=False)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        return MemoryDeleteResult(status="succeeded", external_ref=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        return MemoryDeleteResult(status="succeeded", external_ref=str(user_id))

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


@pytest.mark.integration
@pytest.mark.anyio
async def test_explicit_create_persists_sync_job_for_succeeded_accepted_and_reconciling(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="alice-mem-persist",
        )
        bob = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="bob-mem-persist",
        )

        # 1. Sync single-ID confirmation persists a succeeded job with business_id & metadata.
        sync_provider = _ControlledMemoryProvider(
            lambda item: MemoryWriteResult(
                status="succeeded",
                provider_id="controlled-memory",
                memory_id="mem_sync_001",
                external_ref=f"ext_{item.write_id}",
            )
        )
        outcome_sync = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=sync_provider,
            user_id=alice.id,
            content="Alice prefers oat milk lattes.",
            scope="user",
            conversation_id=None,
        )
        assert isinstance(outcome_sync, CreatedMemoryOutcome)
        assert outcome_sync.record.memory_id == "mem_sync_001"
        assert outcome_sync.record.scope == "user"
        assert outcome_sync.record.memory_epoch == 0

        sync_jobs = await jobs_service.list_jobs_for_user(
            session_factory,
            user_id=alice.id,
            job_type="memory_explicit_create",
        )
        assert len(sync_jobs) == 1
        assert sync_jobs[0].status == "succeeded"
        assert sync_jobs[0].business_id == sync_provider.received_inputs[0].write_id
        assert sync_jobs[0].metadata_json["memory_id"] == "mem_sync_001"
        assert sync_jobs[0].external_ref == f"ext_{sync_provider.received_inputs[0].write_id}"
        assert sync_jobs[0].attempt_count == 1
        assert "oat milk" not in str(sync_jobs[0].metadata_json)

        # 2. Async acceptance persists a reconciling job + queryable owner-scoped operation.
        async_provider = _ControlledMemoryProvider(
            lambda item: MemoryWriteResult(
                status="accepted",
                provider_id="controlled-memory",
                memory_id=None,
                external_ref=f"task_{item.write_id}",
            )
        )
        outcome_async = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=async_provider,
            user_id=alice.id,
            content="Alice works in Beijing.",
            scope="user",
            conversation_id=None,
        )
        assert isinstance(outcome_async, AcceptedMemoryOutcome)
        assert outcome_async.operation.status == "running"
        assert outcome_async.operation.job_id is not None

        fetched_op = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=alice.id,
            operation_id=outcome_async.operation.id,
        )
        assert fetched_op.id == outcome_async.operation.id
        expected_ext_ref = f"task_{async_provider.received_inputs[0].write_id}"
        assert fetched_op.progress["external_ref"] == expected_ext_ref

        with pytest.raises(NotFoundError):
            await operations_service.get_operation_for_owner(
                session_factory,
                owner_id=bob.id,
                operation_id=outcome_async.operation.id,
            )

        # 3. Timeout / unknown outcome retains reconciling job + operation for Worker/barrier.
        timeout_provider = _ControlledMemoryProvider(
            lambda item: MemoryWriteResult(
                status="reconciling",
                provider_id="controlled-memory",
                memory_id=None,
                external_ref=item.write_id,
                error_code="memory_provider_timeout",
            )
        )
        outcome_reconciling = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=timeout_provider,
            user_id=alice.id,
            content="Alice is allergic to peanuts.",
            scope="user",
            conversation_id=None,
        )
        assert isinstance(outcome_reconciling, AcceptedMemoryOutcome)
        assert outcome_reconciling.operation.status == "reconciling"
        assert outcome_reconciling.operation.error_code == "memory_provider_timeout"
        assert outcome_reconciling.operation.job_id is not None

        reconciling_job = await jobs_service.get_job(
            session_factory,
            job_id=outcome_reconciling.operation.job_id,
        )
        assert reconciling_job is not None
        assert reconciling_job.status == "reconciling"
        assert reconciling_job.external_ref == timeout_provider.received_inputs[0].write_id
        assert reconciling_job.last_error_code == "memory_provider_timeout"
        assert "peanuts" not in str(reconciling_job.metadata_json)
        assert "peanuts" not in (reconciling_job.last_error_summary or "")
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memory_scope_clearing_barrier_blocks_pre_call_and_reconciles_mid_flight(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="alice-barrier",
        )

        # 1. Mid-flight clear started while external provider call is running -> reconciling 202.
        async def mid_flight_clear(item: MemoryWriteInput) -> MemoryWriteResult:
            async with session_factory.begin() as session:
                await session.execute(
                    text(
                        "UPDATE memory_scopes SET epoch = epoch + 1, barrier_state = 'clearing' "
                        "WHERE user_id = :user_id AND provider_id = :provider_id"
                    ),
                    {"user_id": alice.id, "provider_id": "controlled-memory"},
                )
            return MemoryWriteResult(
                status="succeeded",
                provider_id="controlled-memory",
                memory_id="mem_mid_flight",
                external_ref=item.write_id,
            )

        mid_flight_provider = _ControlledMemoryProvider(mid_flight_clear)
        outcome = await memories_service.create_explicit_memory(
            session_factory,
            memory_provider=mid_flight_provider,
            user_id=alice.id,
            content="Write racing with clear barrier.",
            scope="user",
            conversation_id=None,
        )
        assert isinstance(outcome, AcceptedMemoryOutcome)
        assert outcome.operation.status == "reconciling"
        assert outcome.operation.error_code == "memory_scope_clearing"
        assert outcome.operation.job_id is not None

        job = await jobs_service.get_job(session_factory, job_id=outcome.operation.job_id)
        assert job is not None
        assert job.status == "reconciling"
        assert job.metadata_json["memory_id"] == "mem_mid_flight"

        # 2. Subsequent explicit write while scope is already clearing is rejected with 409.
        blocked_provider = _ControlledMemoryProvider(
            lambda item: MemoryWriteResult(
                status="succeeded",
                provider_id="controlled-memory",
                memory_id="mem_should_not_run",
            )
        )
        with pytest.raises(ConflictError) as exc_info:
            await memories_service.create_explicit_memory(
                session_factory,
                memory_provider=blocked_provider,
                user_id=alice.id,
                content="Should be blocked by clearing barrier.",
                scope="user",
                conversation_id=None,
            )
        assert exc_info.value.code == "memory_scope_clearing"
        assert blocked_provider.received_inputs == []
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_job_completion_returns_fresh_dto_and_rejects_expired_lease(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        base_time = datetime(2026, 11, 21, 15, 0, 0, tzinfo=UTC)
        async with session_factory.begin() as session:
            fresh_job = await jobs_service.register_and_claim_sync_job_in_session(
                session,
                job_type="memory_explicit_create",
                business_id="biz_1",
                user_id=None,
                dedupe_key="test:fresh-dto",
                metadata_json={"k": "v"},
                lease_duration_seconds=30,
                now=base_time,
            )
            assert fresh_job.lease_token is not None
            completed = await jobs_service.complete_job_in_session(
                session,
                job_id=fresh_job.id,
                lease_token=fresh_job.lease_token,
                status="succeeded",
                external_ref="ext_1",
                now=base_time + timedelta(seconds=5),
            )
            # Must reflect post-UPDATE values rather than stale identity-map state.
            assert completed.status == "succeeded"
            assert completed.lease_token is None
            assert completed.external_ref == "ext_1"
            assert completed.finished_at is not None

        # Attempting to complete a job after its lease expired must raise ConflictError.
        async with session_factory.begin() as session:
            expired_job = await jobs_service.register_and_claim_sync_job_in_session(
                session,
                job_type="memory_explicit_create",
                business_id="biz_2",
                user_id=None,
                dedupe_key="test:expired-lease",
                metadata_json={},
                lease_duration_seconds=10,
                now=base_time,
            )
        assert expired_job.lease_token is not None
        with pytest.raises(ConflictError) as lease_err:
            async with session_factory.begin() as session:
                await jobs_service.complete_job_in_session(
                    session,
                    job_id=expired_job.id,
                    lease_token=expired_job.lease_token,
                    status="succeeded",
                    now=base_time + timedelta(seconds=20),
                )
        assert lease_err.value.code == "job_lease_lost"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memory_list_snapshot_store_cross_replica_expiry_and_owner(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="alice-snap",
        )
        bob = await users_service.resolve_active_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="bob-snap",
        )

        base_time = datetime(2026, 11, 21, 10, 0, 0, tzinfo=UTC)
        records = [
            MemoryRecord(
                memory_id="mem_c",
                provider_id="memos",
                user_id=alice.id,
                scope="user",
                content="Third item",
                created_at=base_time,
            ),
            MemoryRecord(
                memory_id="mem_b",
                provider_id="memos",
                user_id=alice.id,
                scope="user",
                content="Second item (same timestamp as mem_c)",
                created_at=base_time,
            ),
            MemoryRecord(
                memory_id="mem_a",
                provider_id="memos",
                user_id=alice.id,
                scope="user",
                content="First item",
                created_at=base_time - timedelta(seconds=5),
            ),
        ]

        alice_key = MemoryListScopeKey(user_id=alice.id, provider_id="memos", scope="user")
        bob_key = MemoryListScopeKey(user_id=bob.id, provider_id="memos", scope="user")

        store_replica_1 = MemorySnapshotStore(
            session_factory=session_factory,
            ttl_seconds=60,
        )
        page1 = await store_replica_1.create_first_page(
            scope_key=alice_key,
            records=records,
            limit=2,
            now=base_time,
        )
        assert [item.memory_id for item in page1.items] == ["mem_c", "mem_b"]
        assert page1.has_more is True
        assert page1.next_cursor is not None

        # A second independent store instance (simulating another API replica) reads page 2 from PG.
        store_replica_2 = MemorySnapshotStore(
            session_factory=session_factory,
            ttl_seconds=60,
        )
        page2 = await store_replica_2.read_next_page(
            scope_key=alice_key,
            cursor=page1.next_cursor,
            limit=2,
            now=base_time + timedelta(seconds=10),
        )
        assert [item.memory_id for item in page2.items] == ["mem_a"]
        assert page2.has_more is False
        assert page2.next_cursor is None

        # Cross-user reuse of Alice's cursor is rejected.
        with pytest.raises(UnprocessableEntityError) as cross_err:
            await store_replica_2.read_next_page(
                scope_key=bob_key,
                cursor=page1.next_cursor,
                limit=2,
                now=base_time + timedelta(seconds=10),
            )
        assert cross_err.value.code == "invalid_cursor"

        # Expired cursor returns 422 cursor_expired.
        with pytest.raises(UnprocessableEntityError) as exp_err:
            await store_replica_2.read_next_page(
                scope_key=alice_key,
                cursor=page1.next_cursor,
                limit=2,
                now=base_time + timedelta(seconds=120),
            )
        assert exp_err.value.code == "cursor_expired"
    finally:
        await engine.dispose()
