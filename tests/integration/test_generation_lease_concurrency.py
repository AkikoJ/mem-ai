"""PostgreSQL integration tests for generation leases, concurrency, renewal, and recovery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, update

from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError, ForbiddenError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.models import Conversation, Generation, Message
from mem_ai.modules.conversations.service import ChatOptions
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.users import service as users_service
from tests.conftest import create_builtin_user


class _GatedByMessageModelProvider:
    """ModelProvider that can pause specific prompts to exercise true DB concurrency."""

    def __init__(self, *, gated_prompts: set[str] | None = None) -> None:
        self._gated_prompts = gated_prompts or set()
        self.started_events: dict[str, asyncio.Event] = {
            prompt: asyncio.Event() for prompt in self._gated_prompts
        }
        self.release_events: dict[str, asyncio.Event] = {
            prompt: asyncio.Event() for prompt in self._gated_prompts
        }
        self.calls: list[str] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        prompt = request.messages[-1].content
        self.calls.append(prompt)
        if prompt in self._gated_prompts:
            self.started_events[prompt].set()
            await self.release_events[prompt].wait()
        return ModelResponse(
            model_id=request.model_id,
            content=f"reply:{prompt}",
            usage=ModelUsage(input_tokens=8, output_tokens=4, total_tokens=12),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


async def _seed_user_model_and_agent(
    session_factory,  # type: ignore[no-untyped-def]
    *,
    email: str,
) -> tuple[UUID, UUID]:
    user = await create_builtin_user(session_factory, email=email)
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
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest.model_validate(
            {
                "name": f"Lease Agent {email}",
                "systemPrompt": "Be concise.",
                "defaultModelId": "gpt-4o-mini",
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
async def test_concurrent_same_conversation_conflicts_while_different_conversations_run_in_parallel(
    migrated_pg_settings: Settings,
) -> None:
    engine_primary = create_async_engine_from_settings(migrated_pg_settings)
    engine_competing = create_async_engine_from_settings(migrated_pg_settings)
    sf_primary = create_session_factory(engine_primary)
    sf_competing = create_session_factory(engine_competing)

    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            sf_primary,
            email="alice-lease-conflict@example.com",
        )
        provider = _GatedByMessageModelProvider(gated_prompts={"pause-conv-a"})

        # Seed conversation A and conversation B.
        conv_a_seed = await conversations_service.run_sync_chat(
            sf_primary,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="seed-a",
        )
        conv_b_seed = await conversations_service.run_sync_chat(
            sf_primary,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="seed-b",
        )

        # Start a paused generation on conversation A.
        paused_task = asyncio.create_task(
            conversations_service.run_sync_chat(
                sf_primary,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=conv_a_seed.conversation_id,
                user_message="pause-conv-a",
            )
        )
        await asyncio.wait_for(provider.started_events["pause-conv-a"].wait(), timeout=3.0)

        # Competing request on conversation A from an independent DB session pool must fail 409
        # immediately without adding a user message or invoking the model.
        calls_before_conflict = list(provider.calls)
        with pytest.raises(ConflictError) as conflict_exc:
            await conversations_service.run_sync_chat(
                sf_competing,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=conv_a_seed.conversation_id,
                user_message="competing-turn-rejected",
            )
        assert conflict_exc.value.code == "conversation_generation_conflict"
        assert provider.calls == calls_before_conflict

        msgs_during_pause = await generation_store.list_conversation_messages_for_owner(
            sf_competing,
            owner_id=alice_id,
            conversation_id=conv_a_seed.conversation_id,
        )
        assert [m.content for m in msgs_during_pause] == [
            "seed-a",
            "reply:seed-a",
            "pause-conv-a",
        ]

        # Meanwhile, a different conversation completes in parallel without waiting for conv A.
        conv_b_turn2 = await conversations_service.run_sync_chat(
            sf_competing,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=conv_b_seed.conversation_id,
            user_message="parallel-conv-b",
        )
        assert conv_b_turn2.assistant_message.content == "reply:parallel-conv-b"

        # Release conversation A: succeeds and releases lease so next turn on A works immediately.
        provider.release_events["pause-conv-a"].set()
        conv_a_turn2 = await asyncio.wait_for(paused_task, timeout=3.0)
        assert conv_a_turn2.assistant_message.content == "reply:pause-conv-a"

        gen_a2 = await generation_store.get_generation_for_owner(
            sf_primary,
            owner_id=alice_id,
            conversation_id=conv_a_seed.conversation_id,
            generation_id=conv_a_turn2.generation_id,
        )
        assert gen_a2.status == "succeeded"
        assert gen_a2.lease_token is None
        assert gen_a2.lease_expires_at is None

        conv_a_turn3 = await conversations_service.run_sync_chat(
            sf_competing,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=conv_a_seed.conversation_id,
            user_message="after-release-a",
        )
        assert conv_a_turn3.assistant_message.content == "reply:after-release-a"
    finally:
        await engine_primary.dispose()
        await engine_competing.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_lease_renewal_commit_and_failure_enforce_token_expiry_user_and_conversation_state(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            email="alice-lease-guards@example.com",
        )
        persona = await agents_service.get_latest_published_version_for_owner(
            session_factory, owner_id=alice_id, agent_id=agent_id
        )
        assert persona is not None

        t0 = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)
        prepared = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="guarded turn",
            lease_ttl_seconds=20.0,
            now=t0,
        )
        assert prepared.lease_token.startswith("genlease_")
        assert prepared.lease_expires_at == t0 + timedelta(seconds=20)

        # 1. Valid renewal extends lease_expires_at.
        renewed = await generation_store.renew_generation_lease_phase(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
            lease_token=prepared.lease_token,
            lease_ttl_seconds=20.0,
            now=t0 + timedelta(seconds=10),
        )
        assert renewed.lease_expires_at == t0 + timedelta(seconds=30)

        # 2. Wrong lease token is rejected for renew, commit, and failure convergence.
        with pytest.raises(ConflictError) as wrong_renew:
            await generation_store.renew_generation_lease_phase(
                session_factory,
                owner_id=alice_id,
                conversation_id=prepared.conversation_id,
                generation_id=prepared.generation_id,
                lease_token="genlease_wrong_token",
                lease_ttl_seconds=20.0,
                now=t0 + timedelta(seconds=12),
            )
        assert wrong_renew.value.code == "generation_lease_lost"

        wrong_converge = await generation_store.converge_generation_failure_phase(
            session_factory,
            generation_id=prepared.generation_id,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            lease_token="genlease_wrong_token",
            error_code="model_upstream_error",
            error_message="forged failure",
            now=t0 + timedelta(seconds=12),
        )
        assert wrong_converge is None

        wrong_token_prepared = prepared.model_copy(update={"lease_token": "genlease_wrong_token"})
        with pytest.raises(ConflictError) as wrong_commit:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=wrong_token_prepared,
                assistant_content="wrong-token reply must roll back",
                usage=ModelUsage(input_tokens=5, output_tokens=5, total_tokens=10),
                now=t0 + timedelta(seconds=12),
            )
        assert wrong_commit.value.code == "generation_lease_lost"
        still_active_after_wrong_commit = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
        )
        assert still_active_after_wrong_commit.status == "running"
        assert still_active_after_wrong_commit.lease_token == prepared.lease_token

        # 3. After expiry, renew/commit/active-failure ALL fail and do NOT write assistant msg.
        after_expiry = t0 + timedelta(seconds=31)
        with pytest.raises(ConflictError) as expired_renew:
            await generation_store.renew_generation_lease_phase(
                session_factory,
                owner_id=alice_id,
                conversation_id=prepared.conversation_id,
                generation_id=prepared.generation_id,
                lease_token=prepared.lease_token,
                lease_ttl_seconds=20.0,
                now=after_expiry,
            )
        assert expired_renew.value.code == "generation_lease_lost"

        with pytest.raises(ConflictError) as expired_commit:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared,
                assistant_content="late reply must roll back",
                usage=ModelUsage(input_tokens=5, output_tokens=5, total_tokens=10),
                now=after_expiry,
            )
        assert expired_commit.value.code == "generation_lease_lost"

        expired_active_converge = await generation_store.converge_generation_failure_phase(
            session_factory,
            generation_id=prepared.generation_id,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            lease_token=prepared.lease_token,
            error_code="model_upstream_error",
            error_message="stale executor cannot alter expired lease",
            now=after_expiry,
        )
        assert expired_active_converge is None

        msgs = await generation_store.list_conversation_messages_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
        )
        assert [(m.role, m.content) for m in msgs] == [("user", "guarded turn")]

        # 4. User disabled blocks renew & commit, and failure convergence records user_disabled
        #    while releasing the still-unexpired lease.
        await users_service.set_user_status(session_factory, user_id=alice_id, status="disabled")
        with pytest.raises(ForbiddenError) as disabled_renew:
            await generation_store.renew_generation_lease_phase(
                session_factory,
                owner_id=alice_id,
                conversation_id=prepared.conversation_id,
                generation_id=prepared.generation_id,
                lease_token=prepared.lease_token,
                lease_ttl_seconds=20.0,
                now=t0 + timedelta(seconds=15),
            )
        assert disabled_renew.value.code == "user_disabled"

        with pytest.raises(ForbiddenError) as disabled_commit:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared,
                assistant_content="reply for disabled user must roll back",
                usage=ModelUsage(input_tokens=5, output_tokens=5, total_tokens=10),
                now=t0 + timedelta(seconds=15),
            )
        assert disabled_commit.value.code == "user_disabled"

        converged_disabled = await generation_store.converge_generation_failure_phase(
            session_factory,
            generation_id=prepared.generation_id,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            lease_token=prepared.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="cancelled",
            now=t0 + timedelta(seconds=15),
        )
        assert converged_disabled is not None
        assert converged_disabled.status == "failed"
        assert converged_disabled.error_code == "user_disabled"
        assert converged_disabled.lease_token is None
        assert converged_disabled.lease_expires_at is None

        # 5. Re-enable user and test conversation entering `deleting` mid-generation.
        await users_service.set_user_status(session_factory, user_id=alice_id, status="active")
        prepared_del = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=prepared.conversation_id,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="turn before conversation delete",
            lease_ttl_seconds=20.0,
            now=t0 + timedelta(seconds=40),
        )
        async with session_factory.begin() as session:
            await session.execute(
                update(Conversation)
                .where(Conversation.id == prepared.conversation_id)
                .values(status="deleting", deleted_at=t0 + timedelta(seconds=45))
            )

        with pytest.raises(ConflictError) as deleting_renew:
            await generation_store.renew_generation_lease_phase(
                session_factory,
                owner_id=alice_id,
                conversation_id=prepared.conversation_id,
                generation_id=prepared_del.generation_id,
                lease_token=prepared_del.lease_token,
                lease_ttl_seconds=20.0,
                now=t0 + timedelta(seconds=46),
            )
        assert deleting_renew.value.code == "conversation_deleting"

        with pytest.raises(ConflictError) as deleting_commit:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared_del,
                assistant_content="reply for deleting conversation must roll back",
                usage=ModelUsage(input_tokens=3, output_tokens=3, total_tokens=6),
                now=t0 + timedelta(seconds=46),
            )
        assert deleting_commit.value.code == "conversation_deleting"

        converged_deleting = await generation_store.converge_generation_failure_phase(
            session_factory,
            generation_id=prepared_del.generation_id,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            lease_token=prepared_del.lease_token,
            error_code="conversation_deleting",
            error_message="conversation is being deleted",
            now=t0 + timedelta(seconds=46),
        )
        assert converged_deleting is not None
        assert converged_deleting.status == "failed"
        assert converged_deleting.error_code == "conversation_deleting"
        assert converged_deleting.lease_token is None
        assert converged_deleting.lease_expires_at is None
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_expired_lease_inline_takeover_and_worker_fallback_recovery(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            email="alice-lease-recovery@example.com",
        )
        persona = await agents_service.get_latest_published_version_for_owner(
            session_factory, owner_id=alice_id, agent_id=agent_id
        )
        assert persona is not None

        t0 = datetime(2026, 11, 22, 13, 0, 0, tzinfo=UTC)

        # Simulate Process 1 crashing right after Tx 1 (no Worker running).
        stale_gen = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="orphaned crashed turn",
            lease_ttl_seconds=10.0,
            now=t0,
        )

        # Before expiry (t0 + 9s), competing request gets 409.
        with pytest.raises(ConflictError) as still_active:
            await generation_store.prepare_generation_phase(
                session_factory,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=stale_gen.conversation_id,
                persona=persona,
                model_id="gpt-4o-mini",
                user_message_content="too early takeover",
                lease_ttl_seconds=10.0,
                now=t0 + timedelta(seconds=9),
            )
        assert still_active.value.code == "conversation_generation_conflict"

        # At exact expiry boundary (t0 + 10s), new request converges stale_gen to
        # `failed` (`generation_lease_lost`) in the SAME short tx and takes over!
        takeover_gen = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=stale_gen.conversation_id,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="fresh takeover turn",
            lease_ttl_seconds=20.0,
            now=t0 + timedelta(seconds=10),
        )
        assert takeover_gen.generation_id != stale_gen.generation_id
        assert takeover_gen.lease_token != stale_gen.lease_token
        assert takeover_gen.history_messages == ()

        reclaimed_record = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=stale_gen.conversation_id,
            generation_id=stale_gen.generation_id,
        )
        assert reclaimed_record.status == "failed"
        assert reclaimed_record.error_code == "generation_lease_lost"
        assert reclaimed_record.lease_token is None
        assert reclaimed_record.lease_expires_at is None
        assert reclaimed_record.completed_at == t0 + timedelta(seconds=10)

        # Stale Process 1 wakes up late and tries to renew/commit/fail: ALL isolated!
        with pytest.raises(ConflictError) as stale_commit:
            await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=stale_gen,
                assistant_content="stale output must never persist",
                usage=ModelUsage(input_tokens=4, output_tokens=4, total_tokens=8),
                now=t0 + timedelta(seconds=11),
            )
        assert stale_commit.value.code == "generation_lease_lost"

        # New Process 2 commits cleanly.
        committed_takeover = await generation_store.commit_generation_success_phase(
            session_factory,
            prepared=takeover_gen,
            assistant_content="takeover succeeded",
            usage=ModelUsage(input_tokens=6, output_tokens=5, total_tokens=11),
            now=t0 + timedelta(seconds=12),
        )
        assert committed_takeover.assistant_message.content == "takeover succeeded"

        # Worker fallback recovery: two new conversations, one expired and one still active.
        expired_orphan = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="worker orphan",
            lease_ttl_seconds=10.0,
            now=t0 + timedelta(seconds=20),
        )
        healthy_active = await generation_store.prepare_generation_phase(
            session_factory,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            persona=persona,
            model_id="gpt-4o-mini",
            user_message_content="healthy active",
            lease_ttl_seconds=60.0,
            now=t0 + timedelta(seconds=20),
        )

        recovered = await generation_store.recover_expired_generations_phase(
            session_factory,
            batch_size=20,
            now=t0 + timedelta(seconds=35),
        )
        assert [g.id for g in recovered] == [expired_orphan.generation_id]
        assert recovered[0].status == "failed"
        assert recovered[0].error_code == "generation_lease_lost"
        assert recovered[0].lease_token is None
        assert recovered[0].lease_expires_at is None

        # Healthy unexpired generation was untouched; recovery is idempotent on repeat run.
        still_running = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=healthy_active.conversation_id,
            generation_id=healthy_active.generation_id,
        )
        assert still_running.status == "running"
        assert still_running.lease_token == healthy_active.lease_token

        assert (
            await generation_store.recover_expired_generations_phase(
                session_factory,
                batch_size=20,
                now=t0 + timedelta(seconds=35),
            )
            == []
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_lease_renewal_heartbeat_keeps_slow_generation_alive_and_handles_renewal_loss(
    migrated_pg_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            email="alice-lease-heartbeat@example.com",
        )
        provider = _GatedByMessageModelProvider(
            gated_prompts={
                "slow-renewed",
                "renewal-db-error-stale",
                "midflight-lease-lost",
                "aborted-on-user-delete",
            }
        )

        # Seed conversation.
        seed = await conversations_service.run_sync_chat(
            session_factory,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="seed-heartbeat",
        )

        # 1. Slow turn with TTL=0.35s and renewal every 0.08s survives a 0.55s pause!
        renewed_options = ChatOptions(
            lease_ttl_seconds=0.35,
            lease_renew_interval_seconds=0.08,
            timeout_seconds=3.0,
        )
        slow_task = asyncio.create_task(
            conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=seed.conversation_id,
                user_message="slow-renewed",
                options=renewed_options,
            )
        )
        await asyncio.wait_for(provider.started_events["slow-renewed"].wait(), timeout=3.0)

        # Wait past initial 0.35s TTL so renewal MUST have extended the lease in PostgreSQL.
        await asyncio.sleep(0.55)

        # Competing turn must STILL get 409 because the lease was renewed!
        with pytest.raises(ConflictError) as still_locked:
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=seed.conversation_id,
                user_message="rejected-while-renewed",
            )
        assert still_locked.value.code == "conversation_generation_conflict"

        provider.release_events["slow-renewed"].set()
        slow_result = await asyncio.wait_for(slow_task, timeout=3.0)
        assert slow_result.assistant_message.content == "reply:slow-renewed"

        # 2. Active renewal loop encountering repeated DB failures lets the lease expire;
        #    new turn reclaims it inline and stale executor fails cleanly without writing output.
        real_renew = generation_store.renew_generation_lease_phase
        renew_attempts = 0

        async def _failing_renew(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal renew_attempts
            renew_attempts += 1
            _ = (args, kwargs)
            raise RuntimeError("simulated transient DB outage during lease renewal")

        monkeypatch.setattr(generation_store, "renew_generation_lease_phase", _failing_renew)
        failing_renew_options = ChatOptions(
            lease_ttl_seconds=0.2,
            lease_renew_interval_seconds=0.05,
            timeout_seconds=3.0,
        )
        stale_task = asyncio.create_task(
            conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=seed.conversation_id,
                user_message="renewal-db-error-stale",
                options=failing_renew_options,
            )
        )
        await asyncio.wait_for(
            provider.started_events["renewal-db-error-stale"].wait(),
            timeout=3.0,
        )
        await asyncio.sleep(0.32)
        assert renew_attempts >= 2
        monkeypatch.setattr(generation_store, "renew_generation_lease_phase", real_renew)

        # New turn reclaims expired lease immediately and commits.
        fresh_result = await conversations_service.run_sync_chat(
            session_factory,
            model_provider=provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=seed.conversation_id,
            user_message="fresh-after-expiry",
        )
        assert fresh_result.assistant_message.content == "reply:fresh-after-expiry"

        # Release the old executor: must fail with `generation_lease_lost` and write nothing.
        provider.release_events["renewal-db-error-stale"].set()
        with pytest.raises(ConflictError) as stale_err:
            await asyncio.wait_for(stale_task, timeout=3.0)
        assert stale_err.value.code == "generation_lease_lost"

        msgs_after_takeover = await generation_store.list_conversation_messages_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=seed.conversation_id,
        )
        assert "reply:renewal-db-error-stale" not in [m.content for m in msgs_after_takeover]
        assert msgs_after_takeover[-1].content == "reply:fresh-after-expiry"

        # 3. Mid-flight lease expiry detected inside `_run_loop` aborts model_task promptly
        #    with `generation_lease_lost` (well before the 5s model timeout).
        midflight_options = ChatOptions(
            lease_ttl_seconds=2.0,
            lease_renew_interval_seconds=0.05,
            timeout_seconds=5.0,
        )
        midflight_task = asyncio.create_task(
            conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=seed.conversation_id,
                user_message="midflight-lease-lost",
                options=midflight_options,
            )
        )
        await asyncio.wait_for(
            provider.started_events["midflight-lease-lost"].wait(),
            timeout=3.0,
        )
        # Force the running generation's lease to be expired in PostgreSQL.
        async with session_factory.begin() as session:
            await session.execute(
                update(Generation)
                .where(
                    Generation.conversation_id == seed.conversation_id,
                    Generation.status == "running",
                )
                .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=5))
            )

        with pytest.raises(ConflictError) as midflight_err:
            await asyncio.wait_for(midflight_task, timeout=2.0)
        assert midflight_err.value.code == "generation_lease_lost"

        # Worker fallback recovers the expired row cleanly.
        recovered_midflight = await generation_store.recover_expired_generations_phase(
            session_factory,
            batch_size=10,
        )
        assert len(recovered_midflight) == 1
        assert recovered_midflight[0].error_code == "generation_lease_lost"

        # 4. User entering `deleting` mid-flight is caught by lease renewal heartbeat:
        #    model execution aborts promptly and lease is released immediately.
        deleting_options = ChatOptions(
            lease_ttl_seconds=2.0,
            lease_renew_interval_seconds=0.05,
            timeout_seconds=5.0,
        )
        deleting_task = asyncio.create_task(
            conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice_id,
                agent_id=agent_id,
                conversation_id=seed.conversation_id,
                user_message="aborted-on-user-delete",
                options=deleting_options,
            )
        )
        await asyncio.wait_for(
            provider.started_events["aborted-on-user-delete"].wait(),
            timeout=3.0,
        )
        await users_service.set_user_status(session_factory, user_id=alice_id, status="deleting")

        with pytest.raises(ForbiddenError) as del_err:
            await asyncio.wait_for(deleting_task, timeout=2.0)
        assert del_err.value.code == "user_deleting"

        # Deletion is irreversible; inspect persisted lease/output without reactivating the user.
        async with session_factory() as session:
            running_id = await session.scalar(
                select(Generation.id).where(
                    Generation.conversation_id == seed.conversation_id,
                    Generation.status == "running",
                )
            )
            stale_reply = await session.scalar(
                select(Message.id).where(
                    Message.conversation_id == seed.conversation_id,
                    Message.content == "reply:aborted-on-user-delete",
                )
            )
        assert running_id is None
        assert stale_reply is None
        with pytest.raises(ConflictError) as irreversible:
            await users_service.set_user_status(session_factory, user_id=alice_id, status="active")
        assert irreversible.value.code == "user_deleting"
    finally:
        await engine.dispose()
