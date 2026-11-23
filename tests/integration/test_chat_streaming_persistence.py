"""PostgreSQL integration tests for streaming generation lifecycle, races, and recovery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import idempotency as idempotency_store
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations import stream as conversations_stream
from mem_ai.modules.conversations.models import Generation
from mem_ai.modules.conversations.schemas import (
    GenerationRecord,
    MessageRecord,
    PreparedGeneration,
)
from mem_ai.modules.conversations.service import ChatOptions
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.users import service as users_service
from tests.conftest import FakeMemoryProvider, parse_sse_text


class _ScriptedStreamingModelProvider:
    def __init__(self, chunks: list[str]) -> None:
        self._chunks = chunks
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(
            model_id=request.model_id,
            content="".join(self._chunks),
            usage=ModelUsage(input_tokens=8, output_tokens=4, total_tokens=12),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        self.requests.append(request)
        for idx, text in enumerate(self._chunks, start=1):
            yield ModelStreamChunk(sequence=idx, text=text, is_final=False)
        yield ModelStreamChunk(
            sequence=len(self._chunks) + 1,
            text="",
            is_final=True,
            usage=ModelUsage(input_tokens=10, output_tokens=6, total_tokens=16),
            finish_reason="stop",
        )


async def _seed_user_agent_and_model(
    session_factory: Any,
) -> tuple[Any, Any, LocalPersonaProvider]:
    user = await users_service.resolve_or_create_user(
        session_factory,
        issuer="https://test.mem-ai.local",
        subject="stream-alice",
    )
    await models_service.sync_model_configs(
        session_factory,
        seeds=[
            ModelConfigSeed(
                model_id="gpt-4o-mini",
                provider_id="openai_compatible",
                upstream_model="gpt-4o-mini",
                display_name="GPT-4o Mini",
                enabled=True,
                supports_streaming=True,
            )
        ],
    )
    created = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest(
            name="Stream Agent",
            systemPrompt="Be concise.",
            defaultModelId="gpt-4o-mini",
        ),
    )
    published = await agents_service.publish_agent(
        session_factory, owner_id=user.id, agent_id=created.agent_id
    )
    return user, published, LocalPersonaProvider(session_factory=session_factory)


@pytest.mark.integration
@pytest.mark.anyio
async def test_stream_shared_lifecycle_atomic_commit_and_commit_failure_rollback(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    memory_provider = FakeMemoryProvider()
    model_provider = _ScriptedStreamingModelProvider(["Hello ", "world!"])

    try:
        user, agent, persona_provider = await _seed_user_agent_and_model(session_factory)

        # 1. Successful streaming turn commits reply + succeeded + idempotency + memory job.
        prepared1 = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="First streamed question",
            idempotency_key=f"stream-ok-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
        )
        assert isinstance(prepared1, PreparedGeneration)

        frames1 = [
            frame
            async for frame in conversations_stream.stream_prepared_generation(
                session_factory,
                model_provider=model_provider,
                memory_provider=memory_provider,
                prepared=prepared1,
            )
        ]
        events1 = parse_sse_text("".join(frames1))
        assert [e.event for e in events1] == [
            "conversation.started",
            "message.delta",
            "message.delta",
            "conversation.completed",
        ]
        assert [e.event_id for e in events1] == ["1", "2", "3", "4"]
        assert events1[0].data == {
            "conversation_id": str(prepared1.conversation_id),
            "generation_id": str(prepared1.generation_id),
            "message_id": str(prepared1.user_message_id),
        }
        assert [e.data["sequence"] for e in events1[1:3]] == [1, 2]
        assert events1[3].data["usage"] == {
            "input_tokens": 10,
            "output_tokens": 6,
            "total_tokens": 16,
        }

        detail1 = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared1.conversation_id,
            generation_id=prepared1.generation_id,
        )
        assert detail1.generation.status == "succeeded"
        assert detail1.assistant_message is not None
        assert detail1.assistant_message.content == "Hello world!"
        assert str(detail1.assistant_message.id) == events1[3].data["message_id"]

        async with session_factory() as session:
            jobs_after_ok = (
                (await session.execute(select(Job).where(Job.job_type == "memory_turn_write")))
                .scalars()
                .all()
            )
        assert len(jobs_after_ok) == 1
        assert jobs_after_ok[0].business_id == str(prepared1.generation_id)

        # 2. Commit failure rolls back assistant message & memory job and emits conversation.failed.
        async def _boom_hook(
            _session: AsyncSession,
            _gen: GenerationRecord,
            _assistant: MessageRecord,
        ) -> None:
            raise RuntimeError("simulated Tx 2 commit error")

        prepared2 = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=prepared1.conversation_id,
            user_message="Turn whose commit will fail",
            idempotency_key=f"stream-commit-fail-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
        )
        assert isinstance(prepared2, PreparedGeneration)

        frames2 = [
            frame
            async for frame in conversations_stream.stream_prepared_generation(
                session_factory,
                model_provider=model_provider,
                memory_provider=memory_provider,
                prepared=prepared2,
                options=ChatOptions(before_commit_hook=_boom_hook),
            )
        ]
        events2 = parse_sse_text("".join(frames2))
        assert [e.event for e in events2] == [
            "conversation.started",
            "message.delta",
            "message.delta",
            "conversation.failed",
        ]
        assert events2[-1].data["code"] == "generation_commit_failed"
        assert events2[-1].data["partial_output_persisted"] is False

        detail2 = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared1.conversation_id,
            generation_id=prepared2.generation_id,
        )
        assert detail2.generation.status == "failed"
        assert detail2.generation.error_code == "generation_commit_failed"
        assert detail2.assistant_message is None

        idem2 = await idempotency_store.get_idempotency_record_for_generation(
            session_factory, generation_id=prepared2.generation_id
        )
        assert idem2 is not None
        assert idem2.status == "failed"
        assert idem2.response_status_code == 503
        assert idem2.error_code == "generation_commit_failed"

        async with session_factory() as session:
            jobs_after_fail = (
                (await session.execute(select(Job).where(Job.job_type == "memory_turn_write")))
                .scalars()
                .all()
            )
        assert len(jobs_after_fail) == 1  # No memory job created for rolled-back turn 2.

        # 3. Subsequent sync turn shares history rules: failed streaming user input is filtered out.
        sync_turn3 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=prepared1.conversation_id,
            user_message="Third sync turn",
            idempotency_key=f"sync-turn3-{uuid4().hex}",
        )
        assert not isinstance(sync_turn3, PreparedGeneration)
        last_prompt_contents = [m.content for m in model_provider.requests[-1].messages]
        assert any("First streamed question" in c for c in last_prompt_contents)
        assert any("Hello world!" in c for c in last_prompt_contents)
        assert all("Turn whose commit will fail" not in c for c in last_prompt_contents)
        assert last_prompt_contents[-1] == "Third sync turn"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_stream_commit_vs_cancel_race_produces_single_terminal_state(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    memory_provider = FakeMemoryProvider()

    try:
        user, agent, persona_provider = await _seed_user_agent_and_model(session_factory)

        # Case A: Commit enters Tx 2 first; concurrent cancel waits on row lock and loses.
        prepared_a = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Race commit wins",
            idempotency_key=f"race-commit-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
        )
        assert isinstance(prepared_a, PreparedGeneration)

        hook_entered = asyncio.Event()
        release_commit = asyncio.Event()

        async def _blocking_hook(
            session: AsyncSession,
            gen: GenerationRecord,
            assistant: MessageRecord,
        ) -> None:
            inner = conversations_service.build_turn_memory_enqueue_hook(
                prepared=prepared_a,
                memory_provider=memory_provider,
            )
            assert inner is not None
            await inner(session, gen, assistant)
            hook_entered.set()
            await release_commit.wait()

        commit_task = asyncio.create_task(
            generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared_a,
                assistant_content="Committed reply wins",
                usage=ModelUsage(input_tokens=5, output_tokens=5, total_tokens=10),
                finish_reason="stop",
                before_commit_hook=_blocking_hook,
            )
        )
        await asyncio.wait_for(hook_entered.wait(), timeout=3.0)

        cancel_task = asyncio.create_task(
            generation_store.converge_generation_failure_phase(
                session_factory,
                owner_id=prepared_a.owner_id,
                conversation_id=prepared_a.conversation_id,
                generation_id=prepared_a.generation_id,
                lease_token=prepared_a.lease_token,
                target_status="cancelled",
                error_code="generation_cancelled",
                error_message="client disconnected",
                error_status_code=409,
            )
        )
        await asyncio.sleep(0.05)
        release_commit.set()

        committed_a = await commit_task
        cancelled_a = await cancel_task
        assert committed_a.assistant_message.content == "Committed reply wins"
        assert cancelled_a is None

        detail_a = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared_a.conversation_id,
            generation_id=prepared_a.generation_id,
        )
        assert detail_a.generation.status == "succeeded"
        assert detail_a.assistant_message is not None

        # Case B: Cancel commits first; subsequent Tx 2 commit raises ConflictError & rolls back.
        prepared_b = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Race cancel wins",
            idempotency_key=f"race-cancel-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
        )
        assert isinstance(prepared_b, PreparedGeneration)

        cancelled_b = await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=prepared_b.owner_id,
            conversation_id=prepared_b.conversation_id,
            generation_id=prepared_b.generation_id,
            lease_token=prepared_b.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="client disconnected",
            error_status_code=409,
        )
        assert cancelled_b is not None
        assert cancelled_b.status == "cancelled"

        with pytest.raises(ConflictError) as exc_info:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared_b,
                assistant_content="Late assistant output must roll back",
                usage=ModelUsage(input_tokens=5, output_tokens=5, total_tokens=10),
                finish_reason="stop",
                before_commit_hook=conversations_service.build_turn_memory_enqueue_hook(
                    prepared=prepared_b,
                    memory_provider=memory_provider,
                ),
            )
        assert exc_info.value.code == "generation_lease_lost"

        detail_b = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared_b.conversation_id,
            generation_id=prepared_b.generation_id,
        )
        assert detail_b.generation.status == "cancelled"
        assert detail_b.generation.error_code == "generation_cancelled"
        assert detail_b.assistant_message is None

        async with session_factory() as session:
            jobs_for_b = (
                (
                    await session.execute(
                        select(Job).where(Job.business_id == str(prepared_b.generation_id))
                    )
                )
                .scalars()
                .all()
            )
        assert jobs_for_b == []
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_stream_expired_lease_recovery_via_worker_and_inline_takeover(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)

    try:
        user, agent, persona_provider = await _seed_user_agent_and_model(session_factory)
        t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

        # 1. Worker fallback recovery converges orphaned streaming generation to queryable failed.
        crashed_stream = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Orphaned streaming process crash",
            idempotency_key=f"crashed-stream-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
            options=ChatOptions(lease_ttl_seconds=10.0),
            now=t0,
        )
        assert isinstance(crashed_stream, PreparedGeneration)

        recovered = await generation_store.recover_expired_generations_phase(
            session_factory,
            now=t0 + timedelta(seconds=11),
        )
        assert [r.id for r in recovered] == [crashed_stream.generation_id]
        assert recovered[0].status == "failed"
        assert recovered[0].error_code == "generation_lease_lost"

        detail = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=crashed_stream.conversation_id,
            generation_id=crashed_stream.generation_id,
        )
        assert detail.generation.status == "failed"
        assert detail.generation.error_code == "generation_lease_lost"
        assert detail.assistant_message is None

        # 2. Inline takeover when a new streaming turn arrives after an expired lease.
        stale_stream = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=crashed_stream.conversation_id,
            user_message="Stale streaming turn",
            idempotency_key=f"stale-stream-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
            options=ChatOptions(lease_ttl_seconds=10.0),
            now=t0 + timedelta(seconds=20),
        )
        assert isinstance(stale_stream, PreparedGeneration)

        takeover_stream = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=crashed_stream.conversation_id,
            user_message="Takeover streaming turn",
            idempotency_key=f"takeover-stream-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
            options=ChatOptions(lease_ttl_seconds=10.0),
            now=t0 + timedelta(seconds=31),
        )
        assert isinstance(takeover_stream, PreparedGeneration)
        assert takeover_stream.generation_id != stale_stream.generation_id

        stale_detail = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=crashed_stream.conversation_id,
            generation_id=stale_stream.generation_id,
        )
        assert stale_detail.generation.status == "failed"
        assert stale_detail.generation.error_code == "generation_lease_lost"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_stream_late_disconnect_after_tx2_commit_preserves_succeeded(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    memory_provider = FakeMemoryProvider()
    model_provider = _ScriptedStreamingModelProvider(["Done ", "reply"])

    try:
        user, agent, persona_provider = await _seed_user_agent_and_model(session_factory)
        prepared = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Disconnect right after commit",
            idempotency_key=f"late-disc-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
        )
        assert isinstance(prepared, PreparedGeneration)

        tx2_ran = asyncio.Event()

        async def _signal_tx2(
            _session: AsyncSession,
            _gen: GenerationRecord,
            _assistant: MessageRecord,
        ) -> None:
            tx2_ran.set()

        stream_gen = conversations_stream.stream_prepared_generation(
            session_factory,
            model_provider=model_provider,
            memory_provider=memory_provider,
            prepared=prepared,
            options=ChatOptions(before_commit_hook=_signal_tx2),
        )
        # Read only `conversation.started`, wait until Tx 2 commits, then close stream early!
        started_frame = await stream_gen.__anext__()
        assert parse_sse_text(started_frame)[0].event == "conversation.started"
        await asyncio.wait_for(tx2_ran.wait(), timeout=3.0)
        await asyncio.sleep(0.05)  # Allow Tx 2 commit to finalize before disconnect hits.
        await stream_gen.aclose()

        detail = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
        )
        assert detail.generation.status == "succeeded"
        assert detail.assistant_message is not None
        assert detail.assistant_message.content == "Done reply"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_stream_mid_flight_lease_loss_isolates_stale_executor_and_respects_convergence(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)

    class _SlowStreamingProvider:
        def __init__(self) -> None:
            self.first_chunk_sent = asyncio.Event()
            self.upstream_closed = asyncio.Event()

        async def generate(self, request: ModelRequest) -> ModelResponse:
            raise AssertionError("sync generate should not be called")

        async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
            _ = request
            try:
                yield ModelStreamChunk(sequence=1, text="partial-", is_final=False)
                self.first_chunk_sent.set()
                await asyncio.sleep(30.0)
            finally:
                self.upstream_closed.set()

    try:
        user, agent, persona_provider = await _seed_user_agent_and_model(session_factory)

        # Case 1: Lease expires AND Worker recovers it to `failed` -> stale stream cancels
        # upstream and emits `conversation.failed(code="generation_lease_lost")`.
        provider1 = _SlowStreamingProvider()
        prepared1 = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Stream loses lease after worker recovery",
            idempotency_key=f"lease-loss-recovered-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
            options=ChatOptions(lease_ttl_seconds=10.0),
        )
        assert isinstance(prepared1, PreparedGeneration)

        async def _expire_and_recover() -> None:
            await asyncio.wait_for(provider1.first_chunk_sent.wait(), timeout=3.0)
            async with session_factory.begin() as session:
                await session.execute(
                    update(Generation)
                    .where(Generation.id == prepared1.generation_id)
                    .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=5))
                )
            recovered = await generation_store.recover_expired_generations_phase(session_factory)
            assert [r.id for r in recovered] == [prepared1.generation_id]

        task1 = asyncio.create_task(_expire_and_recover())
        frames1 = [
            frame
            async for frame in conversations_stream.stream_prepared_generation(
                session_factory,
                model_provider=provider1,
                prepared=prepared1,
                options=ChatOptions(
                    lease_ttl_seconds=10.0,
                    lease_renew_interval_seconds=0.05,
                    stream_heartbeat_interval_seconds=1.0,
                ),
            )
        ]
        await task1

        assert provider1.upstream_closed.is_set()
        events1 = [e for e in parse_sse_text("".join(frames1)) if e.event != "heartbeat"]
        assert [e.event for e in events1] == [
            "conversation.started",
            "message.delta",
            "conversation.failed",
        ]
        assert events1[-1].data["code"] == "generation_lease_lost"
        assert events1[-1].data["partial_output_persisted"] is False

        # Case 2: Lease expires BEFORE Worker runs -> stale executor does NOT write DB and does
        # NOT emit unconfirmed `conversation.failed`; Worker recovery subsequently converges it.
        provider2 = _SlowStreamingProvider()
        prepared2 = await conversations_service.prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            owner_id=user.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Stream loses lease before worker runs",
            idempotency_key=f"lease-loss-unrecovered-{uuid4().hex}",
            stream=True,
            allow_streaming=True,
            options=ChatOptions(lease_ttl_seconds=10.0),
        )
        assert isinstance(prepared2, PreparedGeneration)

        async def _expire_only() -> None:
            await asyncio.wait_for(provider2.first_chunk_sent.wait(), timeout=3.0)
            async with session_factory.begin() as session:
                await session.execute(
                    update(Generation)
                    .where(Generation.id == prepared2.generation_id)
                    .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=5))
                )

        task2 = asyncio.create_task(_expire_only())
        frames2 = [
            frame
            async for frame in conversations_stream.stream_prepared_generation(
                session_factory,
                model_provider=provider2,
                prepared=prepared2,
                options=ChatOptions(
                    lease_ttl_seconds=10.0,
                    lease_renew_interval_seconds=0.05,
                    stream_heartbeat_interval_seconds=1.0,
                ),
            )
        ]
        await task2

        assert provider2.upstream_closed.is_set()
        events2 = [e for e in parse_sse_text("".join(frames2)) if e.event != "heartbeat"]
        assert [e.event for e in events2] == ["conversation.started", "message.delta"]

        recovered2 = await generation_store.recover_expired_generations_phase(session_factory)
        assert [r.id for r in recovered2] == [prepared2.generation_id]
        detail2 = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=user.id,
            conversation_id=prepared2.conversation_id,
            generation_id=prepared2.generation_id,
        )
        assert detail2.generation.status == "failed"
        assert detail2.generation.error_code == "generation_lease_lost"
        assert detail2.assistant_message is None
    finally:
        await engine.dispose()
