"""Integration tests for Agent & PersonaVersion persistence, constraints, and pagination."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError, ForbiddenError, NotFoundError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import repository as agents_repository
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.models import Agent, PersonaVersion
from mem_ai.modules.agents.schemas import (
    AgentCreateRequest,
    AgentPublishRequest,
    AgentUpdateDraftRequest,
    MemoryPolicyPatch,
    MemoryPolicyPayload,
)
from mem_ai.modules.users import service as users_service


@pytest.mark.integration
@pytest.mark.anyio
async def test_agent_draft_and_first_publish_persist_across_restart_with_unique_version(
    migrated_pg_settings: Settings,
) -> None:
    engine1 = create_async_engine_from_settings(migrated_pg_settings)
    sf1 = create_session_factory(engine1)
    try:
        alice = await users_service.resolve_or_create_user(
            sf1,
            issuer="https://issuer.test",
            subject="alice-persist",
        )
        created = await agents_service.create_agent(
            sf1,
            owner_id=alice.id,
            data=AgentCreateRequest(
                name="Research Assistant",
                systemPrompt="You are a precise researcher.",
                behaviorRules=["Cite primary sources", "Be concise"],
                tone="analytical",
                language="zh-CN",
                defaultModelId="gpt-4.1-mini",
                memoryPolicy=MemoryPolicyPayload(
                    enabled=True,
                    writeEnabled=True,
                    writeScope="conversation",
                    recallLimit=8,
                ),
            ),
        )
        assert created.owner_id == alice.id
        assert created.status == "active"
        assert created.current_published_version_id is None
        assert created.published_version is None
        assert created.latest_version == 0
        assert created.draft.default_model_id == "gpt-4.1-mini"
        assert created.draft.memory_policy.write_scope == "conversation"
        assert created.draft.memory_policy.write_enabled is True

        published = await agents_service.publish_agent(
            sf1,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert published.current_published_version_id is not None
        assert published.published_version is not None
        assert published.published_version.version == 1
        assert published.published_version.published_at is not None
        assert published.published_version.version_id == published.current_published_version_id
        assert published.published_version.system_prompt == "You are a precise researcher."
        assert published.published_version.behavior_rules == ("Cite primary sources", "Be concise")
        assert published.published_version.default_model_id == "gpt-4.1-mini"
        assert published.published_version.memory_policy.write_scope == "conversation"
        assert published.latest_version == 1
    finally:
        await engine1.dispose()

    # Simulate process restart with a fresh engine and session factory.
    engine2 = create_async_engine_from_settings(migrated_pg_settings)
    sf2 = create_session_factory(engine2)
    try:
        reloaded = await agents_service.get_agent(
            sf2,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert reloaded.agent_id == created.agent_id
        assert reloaded.current_published_version_id == published.current_published_version_id
        assert reloaded.published_version is not None
        assert reloaded.published_version.version == 1
        assert reloaded.published_version.default_model_id == "gpt-4.1-mini"
        assert reloaded.published_version.memory_policy.write_scope == "conversation"

        # Verify PostgreSQL directly enforces UNIQUE (agent_id, version) on persona_versions.
        with pytest.raises(IntegrityError):
            async with sf2.begin() as session:
                await session.execute(
                    text(
                        """
                        INSERT INTO persona_versions (
                            id, agent_id, version, system_prompt, behavior_rules, memory_policy
                        ) VALUES (
                            gen_random_uuid(), :agent_id, 1, 'dup', '[]'::jsonb, '{}'::jsonb
                        )
                        """
                    ),
                    {"agent_id": created.agent_id},
                )

        # Verify composite FK prevents pointing current_published_version_id to another Agent's row.
        other = await agents_service.create_agent(
            sf2,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Other", systemPrompt="Other prompt"),
        )
        with pytest.raises(IntegrityError):
            async with sf2.begin() as session:
                await session.execute(
                    update(Agent)
                    .where(Agent.id == other.agent_id)
                    .values(current_published_version_id=published.current_published_version_id)
                )
    finally:
        await engine2.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_draft_edit_and_version_snapshot_do_not_mutate_or_switch_published_version(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    sf = create_session_factory(engine)
    try:
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-versions",
        )
        created = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(
                name="Writer",
                systemPrompt="Prompt v1",
                behaviorRules=["Rule 1"],
                tone="formal",
                language="en",
                defaultModelId="model-a",
                memoryPolicy=MemoryPolicyPayload(writeScope="user", recallLimit=5),
            ),
        )
        v1_agent = await agents_service.publish_agent(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert v1_agent.published_version is not None
        v1_id = v1_agent.published_version.version_id

        # Update draft: published v1 must remain completely untouched.
        edited = await agents_service.update_agent_draft(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            data=AgentUpdateDraftRequest(
                systemPrompt="Prompt v2 draft",
                tone=None,
                defaultModelId="model-b",
                memoryPolicy=MemoryPolicyPatch(writeScope="conversation"),
            ),
        )
        assert edited.draft.system_prompt == "Prompt v2 draft"
        assert edited.draft.tone is None
        assert edited.draft.default_model_id == "model-b"
        assert edited.draft.memory_policy.write_scope == "conversation"
        assert edited.draft.memory_policy.recall_limit == 5  # untouched sub-field preserved
        assert edited.current_published_version_id == v1_id
        assert edited.published_version is not None
        assert edited.published_version.system_prompt == "Prompt v1"
        assert edited.published_version.tone == "formal"
        assert edited.published_version.memory_policy.write_scope == "user"

        # Snapshot draft into v2 without switching current_published_version_id.
        v2_snapshot = await agents_service.create_persona_version(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert v2_snapshot.version == 2
        assert v2_snapshot.published_at is None
        assert v2_snapshot.system_prompt == "Prompt v2 draft"
        assert v2_snapshot.default_model_id == "model-b"

        # Unpublished v2 snapshot must not be returned by PersonaProvider read-only lookup.
        unpublished_lookup = await agents_service.get_persona_version_for_owner(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            version_id=v2_snapshot.version_id,
        )
        assert unpublished_lookup is None

        still_v1 = await agents_service.get_agent(sf, owner_id=alice.id, agent_id=created.agent_id)
        assert still_v1.current_published_version_id == v1_id
        assert still_v1.current_published_version == 1
        assert still_v1.latest_version == 2

        # Explicitly publish v2: stamps published_at and preserves v1 immutable.
        published_v2 = await agents_service.publish_agent(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            data=AgentPublishRequest(versionId=v2_snapshot.version_id),
        )
        assert published_v2.current_published_version_id == v2_snapshot.version_id
        assert published_v2.current_published_version == 2
        assert published_v2.published_version is not None
        assert published_v2.published_version.published_at is not None
        assert published_v2.published_version.system_prompt == "Prompt v2 draft"

        pinned_v1 = await agents_service.get_persona_version_for_owner(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            version_id=v1_id,
        )
        assert pinned_v1 is not None
        assert pinned_v1.version == 1
        assert pinned_v1.system_prompt == "Prompt v1"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_concurrent_publish_deterministically_triggers_unique_and_conditional_update(
    migrated_pg_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    sf = create_session_factory(engine)
    try:
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-race",
        )
        agent_draft_race = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Draft Racer", systemPrompt="Race prompt"),
        )

        # Case 1: Synchronize two concurrent draft publishes right after reading the Agent row
        # so both attempt version=1 -> one commits, one hits uq_persona_versions_agent_id_version.
        original_get = agents_repository.get_agent_for_owner
        draft_barrier = asyncio.Barrier(2)

        async def _synchronized_get_for_draft(
            session: AsyncSession,
            *,
            owner_id: UUID,
            agent_id: UUID,
        ) -> Agent | None:
            result = await original_get(session, owner_id=owner_id, agent_id=agent_id)
            await draft_barrier.wait()
            return result

        monkeypatch.setattr(agents_repository, "get_agent_for_owner", _synchronized_get_for_draft)
        draft_results = await asyncio.gather(
            agents_service.publish_agent(sf, owner_id=alice.id, agent_id=agent_draft_race.agent_id),
            agents_service.publish_agent(sf, owner_id=alice.id, agent_id=agent_draft_race.agent_id),
            return_exceptions=True,
        )
        monkeypatch.setattr(agents_repository, "get_agent_for_owner", original_get)

        draft_successes = [r for r in draft_results if not isinstance(r, BaseException)]
        draft_conflicts = [r for r in draft_results if isinstance(r, ConflictError)]
        assert len(draft_successes) == 1
        assert len(draft_conflicts) == 1
        assert draft_conflicts[0].status_code == 409
        assert draft_conflicts[0].code == "agent_publish_conflict"

        async with sf() as session:
            version_count = (
                await session.execute(
                    select(func.count(PersonaVersion.id)).where(
                        PersonaVersion.agent_id == agent_draft_race.agent_id
                    )
                )
            ).scalar_one()
            agent_row = await session.scalar(
                select(Agent).where(Agent.id == agent_draft_race.agent_id)
            )
            assert agent_row is not None
            assert version_count == 1
            assert agent_row.latest_version_number == 1

        # Case 2: Synchronize two concurrent publishes of two pre-created versions (v1 vs v2)
        # so both pass read and race directly on conditional_publish_agent WHERE revision/current.
        agent_switch_race = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Switch Racer", systemPrompt="v1"),
        )
        v1 = await agents_service.create_persona_version(
            sf,
            owner_id=alice.id,
            agent_id=agent_switch_race.agent_id,
        )
        v2 = await agents_service.create_persona_version(
            sf,
            owner_id=alice.id,
            agent_id=agent_switch_race.agent_id,
        )

        switch_barrier = asyncio.Barrier(2)

        async def _synchronized_get_for_switch(
            session: AsyncSession,
            *,
            owner_id: UUID,
            agent_id: UUID,
        ) -> Agent | None:
            result = await original_get(session, owner_id=owner_id, agent_id=agent_id)
            await switch_barrier.wait()
            return result

        monkeypatch.setattr(agents_repository, "get_agent_for_owner", _synchronized_get_for_switch)
        switch_results = await asyncio.gather(
            agents_service.publish_agent(
                sf,
                owner_id=alice.id,
                agent_id=agent_switch_race.agent_id,
                data=AgentPublishRequest(versionId=v1.version_id),
            ),
            agents_service.publish_agent(
                sf,
                owner_id=alice.id,
                agent_id=agent_switch_race.agent_id,
                data=AgentPublishRequest(versionId=v2.version_id),
            ),
            return_exceptions=True,
        )
        monkeypatch.setattr(agents_repository, "get_agent_for_owner", original_get)

        switch_successes = [r for r in switch_results if not isinstance(r, BaseException)]
        switch_conflicts = [r for r in switch_results if isinstance(r, ConflictError)]
        assert len(switch_successes) == 1
        assert len(switch_conflicts) == 1
        assert switch_conflicts[0].status_code == 409
        assert switch_conflicts[0].code == "agent_publish_conflict"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_list_agents_cursor_pagination_is_stable_with_identical_timestamps(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    sf = create_session_factory(engine)
    try:
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-page",
        )
        bob = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="bob-page",
        )

        alice_ids: list[UUID] = []
        for idx in range(5):
            rec = await agents_service.create_agent(
                sf,
                owner_id=alice.id,
                data=AgentCreateRequest(name=f"Alice Agent {idx}", systemPrompt="Prompt"),
            )
            alice_ids.append(rec.agent_id)

        for idx in range(2):
            await agents_service.create_agent(
                sf,
                owner_id=bob.id,
                data=AgentCreateRequest(name=f"Bob Agent {idx}", systemPrompt="Prompt"),
            )

        # Pin all rows to the exact same created_at timestamp to verify tie-breaking on id DESC.
        same_ts = datetime(2026, 11, 21, 10, 0, 0, tzinfo=UTC)
        async with sf.begin() as session:
            await session.execute(update(Agent).values(created_at=same_ts))

        collected: list[UUID] = []
        cursor: str | None = None
        pages = 0
        while True:
            items, next_cursor, has_more = await agents_service.list_agents(
                sf,
                owner_id=alice.id,
                limit=2,
                cursor=cursor,
            )
            pages += 1
            collected.extend(item.agent_id for item in items)
            if not has_more:
                assert next_cursor is None
                break
            assert next_cursor is not None
            cursor = next_cursor

        assert pages == 3
        assert len(collected) == 5
        assert len(set(collected)) == 5
        # Expected strict (created_at DESC, id DESC) ordering across identical timestamps.
        assert collected == sorted(alice_ids, reverse=True)
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_retire_agent_persists_across_restart_is_idempotent_and_blocks_mutations(
    migrated_pg_settings: Settings,
) -> None:
    engine1 = create_async_engine_from_settings(migrated_pg_settings)
    sf1 = create_session_factory(engine1)
    try:
        alice = await users_service.resolve_or_create_user(
            sf1,
            issuer="https://issuer.test",
            subject="alice-retire-persist",
        )
        bob = await users_service.resolve_or_create_user(
            sf1,
            issuer="https://issuer.test",
            subject="bob-retire-persist",
        )
        created = await agents_service.create_agent(
            sf1,
            owner_id=alice.id,
            data=AgentCreateRequest(
                name="Retireable",
                systemPrompt="Pinned prompt v1",
                defaultModelId="gpt-4o-mini",
            ),
        )
        published = await agents_service.publish_agent(
            sf1,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert published.published_version is not None
        v1_id = published.published_version.version_id

        # Cross-user retire fails with uniform 404 agent_not_found.
        with pytest.raises(NotFoundError) as cross_exc:
            await agents_service.retire_agent(
                sf1,
                owner_id=bob.id,
                agent_id=created.agent_id,
            )
        assert cross_exc.value.status_code == 404
        assert cross_exc.value.code == "agent_not_found"

        # First retirement transitions status to retired and preserves published version.
        retired_first = await agents_service.retire_agent(
            sf1,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert retired_first.status == "retired"
        assert retired_first.retired_at is not None
        assert retired_first.current_published_version_id == v1_id
        assert retired_first.published_version is not None
        assert retired_first.published_version.system_prompt == "Pinned prompt v1"

        async with sf1() as session:
            rev_after_first = await session.scalar(
                select(Agent.revision).where(Agent.id == created.agent_id)
            )
            assert rev_after_first is not None

        # Repeated retirement is idempotent: preserves original retired_at and revision.
        retired_second = await agents_service.retire_agent(
            sf1,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert retired_second.status == "retired"
        assert retired_second.retired_at == retired_first.retired_at
        assert retired_second.current_published_version_id == v1_id
        async with sf1() as session:
            rev_after_second = await session.scalar(
                select(Agent.revision).where(Agent.id == created.agent_id)
            )
            assert rev_after_second == rev_after_first

        # Retired agent rejects draft edit, version creation, and publishing with 409 agent_retired.
        with pytest.raises(ConflictError) as patch_exc:
            await agents_service.update_agent_draft(
                sf1,
                owner_id=alice.id,
                agent_id=created.agent_id,
                data=AgentUpdateDraftRequest(systemPrompt="Cannot edit"),
            )
        assert patch_exc.value.status_code == 409
        assert patch_exc.value.code == "agent_retired"

        with pytest.raises(ConflictError) as version_exc:
            await agents_service.create_persona_version(
                sf1,
                owner_id=alice.id,
                agent_id=created.agent_id,
            )
        assert version_exc.value.status_code == 409
        assert version_exc.value.code == "agent_retired"

        with pytest.raises(ConflictError) as publish_exc:
            await agents_service.publish_agent(
                sf1,
                owner_id=alice.id,
                agent_id=created.agent_id,
            )
        assert publish_exc.value.status_code == 409
        assert publish_exc.value.code == "agent_retired"

        # Disabled user cannot retire even an already-retired owned agent.
        await users_service.set_user_status(sf1, user_id=alice.id, status="disabled")
        with pytest.raises(ForbiddenError) as disabled_exc:
            await agents_service.retire_agent(
                sf1,
                owner_id=alice.id,
                agent_id=created.agent_id,
            )
        assert disabled_exc.value.status_code == 403
        await users_service.set_user_status(sf1, user_id=alice.id, status="active")
    finally:
        await engine1.dispose()

    # Fresh engine simulates process restart: retired agent and published snapshot remain readable,
    # while latest published lookup for new chats returns None.
    engine2 = create_async_engine_from_settings(migrated_pg_settings)
    sf2 = create_session_factory(engine2)
    try:
        reloaded = await agents_service.get_agent(
            sf2,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert reloaded.status == "retired"
        assert reloaded.retired_at == retired_first.retired_at
        assert reloaded.published_version is not None
        assert reloaded.published_version.version_id == v1_id
        assert reloaded.published_version.system_prompt == "Pinned prompt v1"

        latest_for_new_chat = await agents_service.get_latest_published_version_for_owner(
            sf2,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert latest_for_new_chat is None

        pinned_for_existing_chat = await agents_service.get_persona_version_for_owner(
            sf2,
            owner_id=alice.id,
            agent_id=created.agent_id,
            version_id=v1_id,
        )
        assert pinned_for_existing_chat is not None
        assert pinned_for_existing_chat.system_prompt == "Pinned prompt v1"
    finally:
        await engine2.dispose()


async def _race_with_get_with_published_barrier(
    monkeypatch: pytest.MonkeyPatch,
    coro_a: Awaitable[Any],
    coro_b: Awaitable[Any],
) -> list[Any]:
    """Synchronize two calls right after `get_agent_with_published_version` initial read."""
    original = agents_repository.get_agent_with_published_version
    barrier = asyncio.Barrier(2)

    async def _synced(
        session: AsyncSession,
        *,
        owner_id: UUID,
        agent_id: UUID,
        populate_existing: bool = False,
    ) -> tuple[Agent, PersonaVersion | None] | None:
        res = await original(
            session,
            owner_id=owner_id,
            agent_id=agent_id,
            populate_existing=populate_existing,
        )
        if not populate_existing:
            await barrier.wait()
        return res

    monkeypatch.setattr(agents_repository, "get_agent_with_published_version", _synced)
    try:
        return list(await asyncio.gather(coro_a, coro_b, return_exceptions=True))
    finally:
        monkeypatch.setattr(agents_repository, "get_agent_with_published_version", original)


async def _race_pausing_before_repo_call(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_attr: str,
    paused_op: Callable[[], Awaitable[Any]],
    winner_op: Callable[[], Awaitable[Any]],
) -> tuple[Any, Any]:
    """Pause `paused_op` right before `agents_repository.<repo_attr>` until `winner_op` commits."""
    original = getattr(agents_repository, repo_attr)
    reached_gate = asyncio.Event()
    winner_done = asyncio.Event()

    async def _gated(*args: Any, **kwargs: Any) -> Any:
        reached_gate.set()
        await winner_done.wait()
        return await original(*args, **kwargs)

    async def _run_winner() -> Any:
        await reached_gate.wait()
        result = await winner_op()
        winner_done.set()
        return result

    monkeypatch.setattr(agents_repository, repo_attr, _gated)
    try:
        paused_res, winner_res = await asyncio.gather(
            paused_op(),
            _run_winner(),
            return_exceptions=True,
        )
        return paused_res, winner_res
    finally:
        monkeypatch.setattr(agents_repository, repo_attr, original)


@pytest.mark.integration
@pytest.mark.anyio
async def test_concurrent_edit_version_publish_and_retire_guards_enforce_409_and_idempotency(
    migrated_pg_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    sf = create_session_factory(engine)
    try:
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-concurrency-full",
        )

        # 1. Concurrent PATCH vs PATCH -> 1 succeeds, 1 raises 409 agent_revision_conflict.
        agent_patch = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Patch Racer", systemPrompt="Initial"),
        )
        patch_results = await _race_with_get_with_published_barrier(
            monkeypatch,
            agents_service.update_agent_draft(
                sf,
                owner_id=alice.id,
                agent_id=agent_patch.agent_id,
                data=AgentUpdateDraftRequest(systemPrompt="Edit A"),
            ),
            agents_service.update_agent_draft(
                sf,
                owner_id=alice.id,
                agent_id=agent_patch.agent_id,
                data=AgentUpdateDraftRequest(systemPrompt="Edit B"),
            ),
        )
        patch_ok = [r for r in patch_results if not isinstance(r, BaseException)]
        patch_err = [r for r in patch_results if isinstance(r, ConflictError)]
        assert len(patch_ok) == 1
        assert len(patch_err) == 1
        assert patch_err[0].status_code == 409
        assert patch_err[0].code == "agent_revision_conflict"

        # 2. Concurrent POST /versions vs POST /versions -> 1 succeeds, 1 raises 409 conflict.
        agent_ver = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Version Racer", systemPrompt="v1"),
        )
        original_get = agents_repository.get_agent_for_owner
        ver_barrier = asyncio.Barrier(2)

        async def _sync_get_for_ver(
            session: AsyncSession,
            *,
            owner_id: UUID,
            agent_id: UUID,
        ) -> Agent | None:
            res = await original_get(session, owner_id=owner_id, agent_id=agent_id)
            await ver_barrier.wait()
            return res

        monkeypatch.setattr(agents_repository, "get_agent_for_owner", _sync_get_for_ver)
        try:
            ver_results = await asyncio.gather(
                agents_service.create_persona_version(
                    sf, owner_id=alice.id, agent_id=agent_ver.agent_id
                ),
                agents_service.create_persona_version(
                    sf, owner_id=alice.id, agent_id=agent_ver.agent_id
                ),
                return_exceptions=True,
            )
        finally:
            monkeypatch.setattr(agents_repository, "get_agent_for_owner", original_get)
        ver_ok = [r for r in ver_results if not isinstance(r, BaseException)]
        ver_err = [r for r in ver_results if isinstance(r, ConflictError)]
        assert len(ver_ok) == 1
        assert len(ver_err) == 1
        assert ver_err[0].status_code == 409
        assert ver_err[0].code == "agent_version_conflict"

        # 3. Concurrent duplicate retire vs retire -> BOTH succeed idempotently with same time.
        agent_dup_retire = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Dup Retire", systemPrompt="Prompt"),
        )
        dup_r1, dup_r2 = await _race_with_get_with_published_barrier(
            monkeypatch,
            agents_service.retire_agent(sf, owner_id=alice.id, agent_id=agent_dup_retire.agent_id),
            agents_service.retire_agent(sf, owner_id=alice.id, agent_id=agent_dup_retire.agent_id),
        )
        assert not isinstance(dup_r1, BaseException)
        assert not isinstance(dup_r2, BaseException)
        assert dup_r1.status == "retired"
        assert dup_r2.status == "retired"
        assert dup_r1.retired_at is not None
        assert dup_r1.retired_at == dup_r2.retired_at

        # 4. Concurrent PATCH vs retire: retire wins first -> PATCH gets 409 agent_retired.
        agent_retire_wins = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Retire Wins", systemPrompt="Prompt"),
        )
        patch_res, retire_res = await _race_pausing_before_repo_call(
            monkeypatch,
            repo_attr="conditional_update_draft",
            paused_op=lambda: agents_service.update_agent_draft(
                sf,
                owner_id=alice.id,
                agent_id=agent_retire_wins.agent_id,
                data=AgentUpdateDraftRequest(systemPrompt="Late draft edit"),
            ),
            winner_op=lambda: agents_service.retire_agent(
                sf, owner_id=alice.id, agent_id=agent_retire_wins.agent_id
            ),
        )
        assert not isinstance(retire_res, BaseException)
        assert isinstance(patch_res, ConflictError)
        assert patch_res.status_code == 409
        assert patch_res.code == "agent_retired"

        # 5. Concurrent retire vs PATCH: PATCH wins first -> retire gets 409 agent_retire_conflict.
        agent_patch_wins = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Patch Wins", systemPrompt="Prompt"),
        )
        retire_conflict, patch_winner = await _race_pausing_before_repo_call(
            monkeypatch,
            repo_attr="conditional_retire_agent",
            paused_op=lambda: agents_service.retire_agent(
                sf, owner_id=alice.id, agent_id=agent_patch_wins.agent_id
            ),
            winner_op=lambda: agents_service.update_agent_draft(
                sf,
                owner_id=alice.id,
                agent_id=agent_patch_wins.agent_id,
                data=AgentUpdateDraftRequest(systemPrompt="Updated first"),
            ),
        )
        assert not isinstance(patch_winner, BaseException)
        assert isinstance(retire_conflict, ConflictError)
        assert retire_conflict.status_code == 409
        assert retire_conflict.code == "agent_retire_conflict"

        # 6. Concurrent publish (draft) vs retire: retire wins -> publish gets 409 agent_retired
        #    and rolls back its inserted persona_versions row cleanly.
        agent_pub_retire = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Publish vs Retire", systemPrompt="Prompt"),
        )
        pub_res, pub_retire_res = await _race_pausing_before_repo_call(
            monkeypatch,
            repo_attr="conditional_publish_agent",
            paused_op=lambda: agents_service.publish_agent(
                sf, owner_id=alice.id, agent_id=agent_pub_retire.agent_id
            ),
            winner_op=lambda: agents_service.retire_agent(
                sf, owner_id=alice.id, agent_id=agent_pub_retire.agent_id
            ),
        )
        assert not isinstance(pub_retire_res, BaseException)
        assert isinstance(pub_res, ConflictError)
        assert pub_res.status_code == 409
        assert pub_res.code == "agent_retired"

        async with sf() as session:
            orphan_count = (
                await session.execute(
                    select(func.count(PersonaVersion.id)).where(
                        PersonaVersion.agent_id == agent_pub_retire.agent_id
                    )
                )
            ).scalar_one()
            assert orphan_count == 0
    finally:
        await engine.dispose()
