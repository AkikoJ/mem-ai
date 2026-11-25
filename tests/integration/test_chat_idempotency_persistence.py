"""PostgreSQL integration tests for chat idempotency, concurrency, expiry, and recovery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import AppError, ConflictError, ForbiddenError, NotFoundError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import idempotency as idempotency_store
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.models import Conversation, Generation, IdempotencyRecord
from mem_ai.modules.conversations.schemas import (
    ChatAcceptedResult,
    CommittedChatResult,
    PreparedGeneration,
)
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.users import service as users_service
from mem_ai.modules.users.models import User


class _StubModelProvider:
    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(
            model_id=request.model_id,
            content=f"reply:{request.messages[-1].content}",
            usage=ModelUsage(input_tokens=11, output_tokens=7, total_tokens=18),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


async def _seed_user_and_agent(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    subject: str,
    model_id: str = "gpt-4o-mini",
) -> tuple[UUID, UUID]:
    user = await users_service.resolve_or_create_user(
        session_factory,
        issuer="https://test.mem-ai.local",
        subject=subject,
    )
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest(
            name=f"Agent {subject}",
            systemPrompt="Helpful assistant.",
            defaultModelId=model_id,
        ),
    )
    await agents_service.publish_agent(session_factory, owner_id=user.id, agent_id=agent.agent_id)
    return user.id, agent.agent_id


async def _seed_model(session_factory: async_sessionmaker[AsyncSession]) -> None:
    await models_service.sync_model_configs(
        session_factory,
        seeds=[
            ModelConfigSeed(
                model_id="gpt-4o-mini",
                provider_id="openai_compatible",
                upstream_model="gpt-4o-mini",
                display_name="GPT-4o Mini",
                enabled=True,
            )
        ],
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_same_key_concurrent_implicit_and_existing_conversation_creates_single_generation(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)

    try:
        await _seed_model(session_factory)
        alice_id, agent_id = await _seed_user_and_agent(session_factory, subject="alice-conc-idem")

        # 1. Concurrent same-key requests with omitted conversationId:
        #    exactly one PreparedGeneration and one ChatAcceptedResult pointing to the SAME IDs.
        r1, r2 = await asyncio.gather(
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=None,
                user_message="implicit race",
                idempotency_key="idem-implicit-race",
            ),
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=None,
                user_message="implicit race",
                idempotency_key="idem-implicit-race",
            ),
        )
        prepared_list = [r for r in (r1, r2) if isinstance(r, PreparedGeneration)]
        accepted_list = [r for r in (r1, r2) if isinstance(r, ChatAcceptedResult)]
        assert len(prepared_list) == 1
        assert len(accepted_list) == 1
        winner = prepared_list[0]
        replay = accepted_list[0]
        assert replay.conversation_id == winner.conversation_id
        assert replay.generation_id == winner.generation_id
        assert replay.user_message_id == winner.user_message_id
        assert replay.status == "running"

        async with session_factory() as session:
            conv_count = await session.scalar(
                select(func.count())
                .select_from(Conversation)
                .where(Conversation.owner_id == alice_id)
            )
            gen_count = await session.scalar(
                select(func.count()).select_from(Generation).where(Generation.owner_id == alice_id)
            )
            idem_count = await session.scalar(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.user_id == alice_id)
            )
        assert conv_count == 1
        assert gen_count == 1
        assert idem_count == 1

        # Complete turn 1 so we can race on the existing conversation.
        await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=winner,
            assistant_content="done-1",
            usage=ModelUsage(input_tokens=3, output_tokens=2, total_tokens=5),
            finish_reason="stop",
        )

        # 2. Concurrent same-key requests on an EXISTING conversation:
        #    loser MUST get ChatAcceptedResult (NOT 409 conversation_generation_conflict!).
        e1, e2 = await asyncio.gather(
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=winner.conversation_id,
                user_message="existing conv race",
                idempotency_key="idem-existing-race",
            ),
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=winner.conversation_id,
                user_message="existing conv race",
                idempotency_key="idem-existing-race",
            ),
        )
        e_prepared = [r for r in (e1, e2) if isinstance(r, PreparedGeneration)]
        e_accepted = [r for r in (e1, e2) if isinstance(r, ChatAcceptedResult)]
        assert len(e_prepared) == 1
        assert len(e_accepted) == 1
        assert e_accepted[0].generation_id == e_prepared[0].generation_id

        await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=e_prepared[0],
            assistant_content="done-2",
            usage=ModelUsage(input_tokens=3, output_tokens=2, total_tokens=5),
            finish_reason="stop",
        )

        # 3. Concurrent same-key requests with DIFFERENT digests:
        #    exactly one wins PreparedGeneration and the other raises 409 idempotency_key_conflict.
        results = await asyncio.gather(
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=winner.conversation_id,
                user_message="payload-alpha",
                idempotency_key="idem-digest-race",
            ),
            conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=winner.conversation_id,
                user_message="payload-beta",
                idempotency_key="idem-digest-race",
            ),
            return_exceptions=True,
        )
        winners = [r for r in results if isinstance(r, PreparedGeneration)]
        conflicts = [
            r
            for r in results
            if isinstance(r, ConflictError) and r.code == "idempotency_key_conflict"
        ]
        assert len(winners) == 1
        assert len(conflicts) == 1
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_idempotency_finalization_across_success_failure_cancel_and_lease_recovery(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)

    try:
        await _seed_model(session_factory)
        alice_id, agent_id = await _seed_user_and_agent(session_factory, subject="alice-term")

        # 1. Active running record has NULL terminal_at & expires_at and never expires at t0 + 48h.
        p1 = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="first turn",
            idempotency_key="k-running-to-success",
            now=t0,
        )
        assert isinstance(p1, PreparedGeneration)
        rec_running = await idempotency_store.get_idempotency_record_for_generation(
            session_factory, generation_id=p1.generation_id
        )
        assert rec_running is not None
        assert rec_running.status == "running"
        assert rec_running.terminal_at is None
        assert rec_running.expires_at is None

        still_running = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="first turn",
            idempotency_key="k-running-to-success",
            now=t0 + timedelta(hours=48),
        )
        assert isinstance(still_running, ChatAcceptedResult)
        assert still_running.generation_id == p1.generation_id

        # Commit success at t1 -> terminal_at = t1, expires_at = t1 + 24h.
        t1 = t0 + timedelta(seconds=10)
        committed = await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=p1,
            assistant_content="hello back",
            usage=ModelUsage(input_tokens=4, output_tokens=5, total_tokens=9),
            finish_reason="stop",
            now=t1,
        )
        rec_succ = await idempotency_store.get_idempotency_record_for_generation(
            session_factory, generation_id=p1.generation_id
        )
        assert rec_succ is not None
        assert rec_succ.status == "succeeded"
        assert rec_succ.response_status_code == 200
        assert rec_succ.terminal_at == t1
        assert rec_succ.expires_at == t1 + timedelta(hours=24)

        # 2. Exact 24h boundary:
        #    - t1 + 24h - 1s -> returns saved CommittedChatResult
        #    - t1 + 24h -> treated as new request, replaces idempotency row, keeps old Generation!
        before_expiry = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="first turn",
            idempotency_key="k-running-to-success",
            now=t1 + timedelta(hours=24) - timedelta(seconds=1),
        )
        assert isinstance(before_expiry, CommittedChatResult)
        assert before_expiry.generation_id == committed.generation_id
        assert before_expiry.assistant_message.content == "hello back"

        at_expiry = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="brand new after 24h",
            idempotency_key="k-running-to-success",
            now=t1 + timedelta(hours=24),
        )
        assert isinstance(at_expiry, PreparedGeneration)
        assert at_expiry.generation_id != p1.generation_id
        assert at_expiry.conversation_id != p1.conversation_id

        # Original generation G1 MUST still be preserved and queryable!
        old_detail = await conversation_queries.get_generation_detail_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=p1.conversation_id,
            generation_id=p1.generation_id,
        )
        assert old_detail.generation.status == "succeeded"
        assert old_detail.assistant_message is not None
        assert old_detail.assistant_message.content == "hello back"

        # 3. Cancellation syncs idempotency_records -> replay returns 409 generation_cancelled.
        t2 = t1 + timedelta(hours=24, seconds=5)
        await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=alice_id,
            conversation_id=at_expiry.conversation_id,
            generation_id=at_expiry.generation_id,
            lease_token=at_expiry.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="client cancelled stream",
            now=t2,
        )
        with pytest.raises(AppError) as cancel_replay:
            await conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=None,
                user_message="brand new after 24h",
                idempotency_key="k-running-to-success",
                now=t2 + timedelta(seconds=1),
            )
        assert cancel_replay.value.status_code == 409
        assert cancel_replay.value.code == "generation_cancelled"
        assert cancel_replay.value.details["status"] == "cancelled"
        assert cancel_replay.value.details["generation_id"] == str(at_expiry.generation_id)

        # 4. Inline expired-lease takeover AND Worker recovery BOTH finalize idempotency_records.
        p_expired_inline = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=p1.conversation_id,
            user_message="orphaned inline",
            idempotency_key="k-orphaned-inline",
            now=t2,
        )
        assert isinstance(p_expired_inline, PreparedGeneration)

        # Another request with a NEW key arrives after lease expiry (t2 + 120s) and reclaims inline.
        t_reclaim = t2 + timedelta(seconds=120)
        p_taker = await conversations_service.prepare_chat_turn(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=p1.conversation_id,
            user_message="new key takeover",
            idempotency_key="k-taker",
            now=t_reclaim,
        )
        assert isinstance(p_taker, PreparedGeneration)
        with pytest.raises(AppError) as inline_lost_replay:
            await conversations_service.prepare_chat_turn(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=p1.conversation_id,
                user_message="orphaned inline",
                idempotency_key="k-orphaned-inline",
                now=t_reclaim + timedelta(seconds=1),
            )
        assert inline_lost_replay.value.status_code == 409
        assert inline_lost_replay.value.code == "generation_lease_lost"
        assert inline_lost_replay.value.details["status"] == "failed"

        # Now let `p_taker` expire and be recovered by Worker `recover_expired_generations_phase`.
        t_worker = t_reclaim + timedelta(seconds=120)
        recovered = await generation_store.recover_expired_generations_phase(
            session_factory, now=t_worker
        )
        assert [g.id for g in recovered] == [p_taker.generation_id]
        rec_worker = await idempotency_store.get_idempotency_record_for_generation(
            session_factory, generation_id=p_taker.generation_id
        )
        assert rec_worker is not None
        assert rec_worker.status == "failed"
        assert rec_worker.error_code == "generation_lease_lost"
        assert rec_worker.response_status_code == 409
        assert rec_worker.terminal_at == t_worker
        assert rec_worker.expires_at == t_worker + timedelta(hours=24)
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_idempotency_cross_user_isolation_check_constraints_and_deleted_resources(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    model_provider = _StubModelProvider()

    try:
        await _seed_model(session_factory)
        alice_id, alice_agent = await _seed_user_and_agent(session_factory, subject="alice-iso")
        bob_id, bob_agent = await _seed_user_and_agent(session_factory, subject="bob-iso")

        # Same Idempotency-Key across different users is completely independent.
        alice_res = await conversations_service.run_sync_chat(
            session_factory,
            model_provider=model_provider,
            owner_id=alice_id,
            agent_id=alice_agent,
            conversation_id=None,
            user_message="same key text",
            idempotency_key="shared-client-key",
        )
        bob_res = await conversations_service.run_sync_chat(
            session_factory,
            model_provider=model_provider,
            owner_id=bob_id,
            agent_id=bob_agent,
            conversation_id=None,
            user_message="same key text",
            idempotency_key="shared-client-key",
        )
        assert isinstance(alice_res, CommittedChatResult)
        assert isinstance(bob_res, CommittedChatResult)
        assert alice_res.conversation_id != bob_res.conversation_id
        assert alice_res.generation_id != bob_res.generation_id

        # Cross-user generation query returns 404.
        with pytest.raises(NotFoundError):
            await conversation_queries.get_generation_detail_for_owner(
                session_factory,
                owner_id=bob_id,
                conversation_id=alice_res.conversation_id,
                generation_id=alice_res.generation_id,
            )

        # DB CHECK constraint enforces active=NULL expiry and terminal=NOT NULL expiry.
        with pytest.raises(IntegrityError):
            async with session_factory.begin() as session:
                session.add(
                    IdempotencyRecord(
                        user_id=alice_id,
                        method="POST",
                        canonical_route="/v1/chat",
                        idempotency_key="bad-check",
                        request_digest="0" * 64,
                        conversation_id=alice_res.conversation_id,
                        generation_id=uuid4(),
                        status="running",
                        expires_at=datetime.now(UTC) + timedelta(hours=1),
                    )
                )

        # Conversation marked `deleting`: generation query AND idempotent replay return 404;
        # continuation with a new key returns 409 conversation_deleting.
        async with session_factory.begin() as session:
            await session.execute(
                update(Conversation)
                .where(Conversation.id == alice_res.conversation_id)
                .values(status="deleting")
            )

        with pytest.raises(NotFoundError):
            await conversation_queries.get_generation_detail_for_owner(
                session_factory,
                owner_id=alice_id,
                conversation_id=alice_res.conversation_id,
                generation_id=alice_res.generation_id,
            )

        with pytest.raises(NotFoundError):
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=model_provider,
                owner_id=alice_id,
                agent_id=alice_agent,
                conversation_id=None,
                user_message="same key text",
                idempotency_key="shared-client-key",
            )

        with pytest.raises(ConflictError) as deleting_cont:
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=model_provider,
                owner_id=alice_id,
                agent_id=alice_agent,
                conversation_id=alice_res.conversation_id,
                user_message="try continue deleting",
                idempotency_key="new-key-on-deleting",
            )
        assert deleting_cont.value.code == "conversation_deleting"

        # User marked `disabled` and `deleting`: generation query AND idempotent replay return 403.
        for user_status, expected_code in (
            ("disabled", "user_disabled"),
            ("deleting", "user_deleting"),
        ):
            async with session_factory.begin() as session:
                await session.execute(
                    update(User).where(User.id == bob_id).values(status=user_status)
                )

            with pytest.raises(ForbiddenError) as status_query:
                await conversation_queries.get_generation_detail_for_owner(
                    session_factory,
                    owner_id=bob_id,
                    conversation_id=bob_res.conversation_id,
                    generation_id=bob_res.generation_id,
                )
            assert status_query.value.code == expected_code

            with pytest.raises(ForbiddenError) as status_replay:
                await conversations_service.run_sync_chat(
                    session_factory,
                    model_provider=model_provider,
                    owner_id=bob_id,
                    agent_id=bob_agent,
                    conversation_id=None,
                    user_message="same key text",
                    idempotency_key="shared-client-key",
                )
            assert status_replay.value.code == expected_code
    finally:
        await engine.dispose()
