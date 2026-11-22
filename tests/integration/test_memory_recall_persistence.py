"""PostgreSQL integration tests for `message_recalled_memories`, guards, and context phases."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import inspect, update
from sqlalchemy.exc import IntegrityError
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
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    BadGatewayError,
    ConflictError,
    NotFoundError,
    ServiceUnavailableError,
)
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.models import MessageRecalledMemory
from mem_ai.modules.conversations.service import SyncChatOptions
from mem_ai.modules.memories.models import MemoryScopeState
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.users import service as users_service


class _CapturingModelProvider:
    def __init__(self) -> None:
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(
            model_id=request.model_id,
            content="ok-reply",
            usage=ModelUsage(input_tokens=10, output_tokens=5, total_tokens=15),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest):  # type: ignore[no-untyped-def]
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


class _ScriptedRecallProvider:
    provider_id = "scripted-memory"

    def __init__(self, *, items: list[MemoryRecallItem] | None = None) -> None:
        self.items: list[MemoryRecallItem] = items if items is not None else []
        self.queries: list[MemoryRecallQuery] = []
        self.on_recall: Any = None
        self.next_error: Exception | None = None

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        self.queries.append(query)
        if self.on_recall is not None:
            await self.on_recall(query)
        if self.next_error is not None:
            raise self.next_error
        return [
            item
            for item in self.items
            if item.scope == query.scope
            and (query.scope != "conversation" or item.conversation_id == query.conversation_id)
        ]

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        _ = item
        raise NotImplementedError

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        _ = item
        raise NotImplementedError

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        _ = query
        return MemoryListPage(items=[])

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        _ = (user_id, memory_id)
        raise NotImplementedError

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        _ = user_id
        raise NotImplementedError

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        _ = (user_id, external_ref)
        raise NotImplementedError


async def _seed_user_model_and_agent(
    session_factory: Any,
    *,
    subject: str,
    recall_limit: int = 5,
    memory_enabled: bool = True,
) -> tuple[UUID, UUID]:
    user = await users_service.resolve_or_create_user(
        session_factory,
        issuer="https://test.mem-ai.local",
        subject=subject,
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
            )
        ],
    )
    agent = await agents_service.create_agent(
        session_factory,
        owner_id=user.id,
        data=AgentCreateRequest.model_validate(
            {
                "name": "Memory Recall Agent",
                "systemPrompt": "Use relevant memories carefully.",
                "defaultModelId": "gpt-4o-mini",
                "memoryPolicy": {
                    "enabled": memory_enabled,
                    "recallLimit": recall_limit,
                    "writeEnabled": True,
                    "writeScope": "user",
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
async def test_recalled_memory_table_stores_metadata_only_and_enforces_uniqueness(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    persona_provider = LocalPersonaProvider(session_factory=session_factory)
    try:
        # Verify schema has strictly metadata columns (NO content/text column).
        async with engine.connect() as conn:
            column_names = await conn.run_sync(
                lambda sync_conn: {
                    col["name"]
                    for col in inspect(sync_conn).get_columns("message_recalled_memories")
                }
            )
        assert {
            "id",
            "user_message_id",
            "provider_id",
            "memory_id",
            "scope",
            "relevance",
            "prompt_position",
            "memory_epoch",
            "recalled_at",
            "deleted_at",
            "created_at",
        } <= column_names
        assert "content" not in column_names
        assert "memory_value" not in column_names

        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="alice-recall-schema",
        )
        bob = await users_service.resolve_or_create_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="bob-recall-schema",
        )

        # Seed `memory_scopes` at epoch=3 to verify `prepare_generation` captures current epoch.
        async with session_factory.begin() as session:
            session.add(
                MemoryScopeState(
                    user_id=alice_id,
                    provider_id="scripted-memory",
                    epoch=3,
                    barrier_state="idle",
                )
            )

        recall_provider = _ScriptedRecallProvider()
        prepared = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Design a schema",
        )
        assert prepared.memory_epoch == 3

        # Duplicate `(user_message_id, provider_id, memory_id, scope)` is rejected by DB constraint.
        async with session_factory.begin() as session:
            session.add(
                MessageRecalledMemory(
                    user_message_id=prepared.user_message_id,
                    provider_id="scripted-memory",
                    memory_id="mem_dup",
                    scope="user",
                    relevance=0.9,
                    prompt_position=1,
                    memory_epoch=3,
                )
            )
        with pytest.raises(IntegrityError):
            async with session_factory.begin() as session:
                session.add(
                    MessageRecalledMemory(
                        user_message_id=prepared.user_message_id,
                        provider_id="scripted-memory",
                        memory_id="mem_dup",
                        scope="user",
                        relevance=0.8,
                        prompt_position=2,
                        memory_epoch=3,
                    )
                )

        # Owner can read references ordered by prompt_position; foreign user gets 404.
        refs = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared.conversation_id,
            user_message_id=prepared.user_message_id,
        )
        assert len(refs) == 1
        assert refs[0].memory_id == "mem_dup"
        assert refs[0].prompt_position == 1
        assert refs[0].memory_epoch == 3
        assert refs[0].deleted_at is None

        with pytest.raises(NotFoundError):
            await generation_store.list_recalled_memories_for_message_for_owner(
                session_factory,
                owner_id=bob.id,
                conversation_id=prepared.conversation_id,
                user_message_id=prepared.user_message_id,
            )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_prepare_context_persists_only_injected_refs_and_handles_degraded_and_empty_recall(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    persona_provider = LocalPersonaProvider(session_factory=session_factory)
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="alice-recall-persist",
            recall_limit=2,
        )
        model_provider = _CapturingModelProvider()
        recall_provider = _ScriptedRecallProvider()

        # First turn establishes conversation_id; empty recall writes 0 reference rows.
        turn1 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Start conversation",
        )
        gen1 = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            generation_id=turn1.generation_id,
        )
        assert gen1.user_message_id is not None
        refs1 = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            user_message_id=gen1.user_message_id,
        )
        assert refs1 == []

        # Second turn recalls 4 items: 1 low-rel, 1 other-conv, 2 valid (conv + user).
        # Only the 2 actually injected memories are saved with prompt_position 1 & 2.
        recall_provider.items = [
            MemoryRecallItem(
                memory_id="mem_user_1",
                provider_id="scripted-memory",
                scope="user",
                content="User builds Python backends",
                relevance=0.82,
                memory_epoch=0,
            ),
            MemoryRecallItem(
                memory_id="mem_conv_1",
                provider_id="scripted-memory",
                scope="conversation",
                content="Current task is ticket 08",
                relevance=0.93,
                conversation_id=turn1.conversation_id,
                memory_epoch=0,
            ),
            MemoryRecallItem(
                memory_id="mem_low_rel",
                provider_id="scripted-memory",
                scope="user",
                content="Irrelevant low score",
                relevance=0.25,
                memory_epoch=0,
            ),
            MemoryRecallItem(
                memory_id="mem_other_conv",
                provider_id="scripted-memory",
                scope="conversation",
                content="Other conversation secret",
                relevance=0.99,
                conversation_id=uuid4(),
                memory_epoch=0,
            ),
        ]

        turn2 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=turn1.conversation_id,
            user_message="How should we continue?",
        )
        gen2 = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            generation_id=turn2.generation_id,
        )
        assert gen2.user_message_id is not None
        refs2 = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            user_message_id=gen2.user_message_id,
        )
        assert [(r.provider_id, r.memory_id, r.scope, r.prompt_position) for r in refs2] == [
            ("scripted-memory", "mem_conv_1", "conversation", 1),
            ("scripted-memory", "mem_user_1", "user", 2),
        ]
        assert refs2[0].relevance == pytest.approx(0.93)
        assert refs2[1].relevance == pytest.approx(0.82)

        system_prompt_turn2 = model_provider.requests[-1].messages[0].content
        assert "- [conversation] Current task is ticket 08" in system_prompt_turn2
        assert "- [user] User builds Python backends" in system_prompt_turn2
        assert "Irrelevant low score" not in system_prompt_turn2
        assert "Other conversation secret" not in system_prompt_turn2

        # Third turn: provider raises error -> degrades to persona + history + input, 0 refs saved.
        recall_provider.next_error = BadGatewayError(
            code="memory_provider_error",
            message="upstream exploded",
        )
        turn3 = await conversations_service.run_sync_chat(
            session_factory,
            persona_provider=persona_provider,
            model_provider=model_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=turn1.conversation_id,
            user_message="Continue despite outage",
        )
        gen3 = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            generation_id=turn3.generation_id,
        )
        assert gen3.user_message_id is not None
        refs3 = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=turn1.conversation_id,
            user_message_id=gen3.user_message_id,
        )
        assert refs3 == []
        assert "Relevant Memories:" not in model_provider.requests[-1].messages[0].content
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_persist_recalled_memories_enforces_lease_midflight_barrier_and_tombstone_seam(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    persona_provider = LocalPersonaProvider(session_factory=session_factory)
    try:
        alice_id, agent_id = await _seed_user_model_and_agent(
            session_factory,
            subject="alice-recall-guards",
        )
        async with session_factory.begin() as session:
            session.add(
                MemoryScopeState(
                    user_id=alice_id,
                    provider_id="scripted-memory",
                    epoch=1,
                    barrier_state="idle",
                )
            )

        # 1. Mid-flight tombstone seam filters out a memory deleted between recall and Tx 1.5
        #    and re-indexes remaining memories cleanly in both DB and compiled prompt.
        tombstoned_now: set[str] = set()

        async def _tombstone_lookup(
            session: AsyncSession,
            user_id: UUID,
            provider_id: str,
            conversation_id: UUID | None,
        ) -> tuple[frozenset[str], bool]:
            _ = (session, user_id, provider_id, conversation_id)
            return (frozenset(tombstoned_now), False)

        recall_provider = _ScriptedRecallProvider(
            items=[
                MemoryRecallItem(
                    memory_id="mem_deleted_midflight",
                    provider_id="scripted-memory",
                    scope="user",
                    content="Must be filtered at Tx 1.5",
                    relevance=0.95,
                    memory_epoch=1,
                ),
                MemoryRecallItem(
                    memory_id="mem_survivor",
                    provider_id="scripted-memory",
                    scope="user",
                    content="Surviving memory",
                    relevance=0.85,
                    memory_epoch=1,
                ),
            ]
        )

        async def _mark_tombstone_midflight(_query: MemoryRecallQuery) -> None:
            tombstoned_now.add("mem_deleted_midflight")

        recall_provider.on_recall = _mark_tombstone_midflight

        prepared_tomb = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Check tombstone seam",
        )
        ctx_tomb = await conversations_service.prepare_context(
            session_factory,
            prepared=prepared_tomb,
            memory_provider=recall_provider,
            options=SyncChatOptions(tombstone_lookup=_tombstone_lookup),
        )
        assert [m.memory_id for m in ctx_tomb.recalled_memories] == ["mem_survivor"]
        assert "Must be filtered at Tx 1.5" not in ctx_tomb.compiled_messages[0].content
        assert "Surviving memory" in ctx_tomb.compiled_messages[0].content

        refs_tomb = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared_tomb.conversation_id,
            user_message_id=prepared_tomb.user_message_id,
        )
        assert [(r.memory_id, r.prompt_position) for r in refs_tomb] == [("mem_survivor", 1)]

        # Converge generation 1 cleanly so we can reuse the conversation.
        await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared_tomb.conversation_id,
            generation_id=prepared_tomb.generation_id,
            lease_token=prepared_tomb.lease_token,
            error_code="generation_cancelled",
            error_message="cancelled for test",
            target_status="cancelled",
        )

        # 2. Mid-flight clearing barrier committed during external recall -> Tx 1.5 drops all
        #    candidates cleanly (0 DB rows, 0 injected memories).
        tombstoned_now.clear()
        barrier_flipped = False

        async def _flip_barrier_midflight(_query: MemoryRecallQuery) -> None:
            nonlocal barrier_flipped
            if barrier_flipped:
                return
            barrier_flipped = True
            async with session_factory.begin() as barrier_session:
                await barrier_session.execute(
                    update(MemoryScopeState)
                    .where(
                        MemoryScopeState.user_id == alice_id,
                        MemoryScopeState.provider_id == "scripted-memory",
                    )
                    .values(epoch=2, barrier_state="clearing")
                )

        recall_provider.on_recall = _flip_barrier_midflight
        prepared_barrier = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=recall_provider,
            owner_id=alice_id,
            agent_id=agent_id,
            conversation_id=prepared_tomb.conversation_id,
            user_message="Check midflight clearing barrier",
        )
        ctx_barrier = await conversations_service.prepare_context(
            session_factory,
            prepared=prepared_barrier,
            memory_provider=recall_provider,
        )
        assert ctx_barrier.recalled_memories == ()
        assert "Relevant Memories:" not in ctx_barrier.compiled_messages[0].content
        refs_barrier = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared_barrier.conversation_id,
            user_message_id=prepared_barrier.user_message_id,
        )
        assert refs_barrier == []

        # 3. Expired/lost lease prevents persisting recalled refs (`409 generation_lease_lost`).
        expired_prepared = prepared_barrier.model_copy(
            update={"lease_token": "genlease_forged_wrong_token"}
        )
        with pytest.raises(ConflictError) as lease_err:
            await generation_store.persist_recalled_memories_phase(
                session_factory,
                prepared=expired_prepared,
                candidates=recall_provider.items,
                now=datetime.now(UTC) + timedelta(seconds=1),
            )
        assert lease_err.value.code == "generation_lease_lost"
        # Ensure still 0 rows were committed.
        refs_after_lease_loss = await generation_store.list_recalled_memories_for_message_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared_barrier.conversation_id,
            user_message_id=prepared_barrier.user_message_id,
        )
        assert refs_after_lease_loss == []

        # 4. Duplicate candidate refs convert unique violation into `409 recalled_memory_conflict`.
        dup_item = MemoryRecallItem(
            memory_id="mem_dup_ref",
            provider_id="scripted-memory",
            scope="user",
            content="Duplicate ref",
            relevance=0.9,
            memory_epoch=2,
        )
        with pytest.raises(ConflictError) as dup_err:
            await generation_store.persist_recalled_memories_phase(
                session_factory,
                prepared=prepared_barrier,
                candidates=[dup_item, dup_item],
            )
        assert dup_err.value.code == "recalled_memory_conflict"

        # 5. Unexpected context-preparation failure maps to `503 generation_context_failed`
        #    (never `502 model_upstream_error`) and never invokes `ModelProvider`.
        async def _crashing_tombstone_lookup(
            session: AsyncSession,
            user_id: UUID,
            provider_id: str,
            conversation_id: UUID | None,
        ) -> tuple[frozenset[str], bool]:
            _ = (session, user_id, provider_id, conversation_id)
            raise RuntimeError("simulated db error in context guard")

        model_provider = _CapturingModelProvider()

        with pytest.raises(ServiceUnavailableError) as ctx_err:
            await conversations_service.complete_prepared_sync_generation(
                session_factory,
                model_provider=model_provider,
                memory_provider=recall_provider,
                prepared=prepared_barrier,
                options=SyncChatOptions(tombstone_lookup=_crashing_tombstone_lookup),
            )
        assert ctx_err.value.code == "generation_context_failed"
        assert model_provider.requests == []
        failed_gen = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice_id,
            conversation_id=prepared_barrier.conversation_id,
            generation_id=prepared_barrier.generation_id,
        )
        assert failed_gen.status == "failed"
        assert failed_gen.error_code == "generation_context_failed"
        await asyncio.sleep(0)
    finally:
        await engine.dispose()
