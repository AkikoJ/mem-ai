"""PostgreSQL integration tests for model resolution and shared generation lifecycle phases."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

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
    GatewayTimeoutError,
    NotFoundError,
    ServiceUnavailableError,
)
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import AgentCreateRequest
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.schemas import GenerationRecord, MessageRecord
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from tests.conftest import create_builtin_user


class _InspectingModelProvider:
    """Controlled ModelProvider that verifies DB state committed before model HTTP runs."""

    def __init__(self, *, on_generate) -> None:  # type: ignore[no-untyped-def]
        self._on_generate = on_generate
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        result: ModelResponse = await self._on_generate(request)
        return result

    async def stream_generate(self, request: ModelRequest):  # type: ignore[no-untyped-def]
        _ = request
        raise NotImplementedError
        yield ModelStreamChunk(sequence=1)


@pytest.mark.integration
@pytest.mark.anyio
async def test_sync_chat_commits_user_message_before_model_and_filters_failed_turns(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await create_builtin_user(session_factory, email="alice-chat-persist@example.com")
        await models_service.sync_model_configs(
            session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="GPT-4o Mini",
                    enabled=True,
                ),
                ModelConfigSeed(
                    model_id="retired-model",
                    provider_id="openai_compatible",
                    upstream_model="retired-model",
                    display_name="Retired Model",
                    enabled=False,
                ),
            ],
        )

        # Create and publish an Agent pointing at `gpt-4o-mini`.
        agent = await agents_service.create_agent(
            session_factory,
            owner_id=alice.id,
            data=AgentCreateRequest.model_validate(
                {
                    "name": "Sync Helper",
                    "systemPrompt": "Version 1 instructions.",
                    "behaviorRules": ["Stay concise"],
                    "defaultModelId": "gpt-4o-mini",
                }
            ),
        )
        published = await agents_service.publish_agent(
            session_factory,
            owner_id=alice.id,
            agent_id=agent.agent_id,
        )
        assert published.current_published_version_id is not None

        # Turn 1: verify inside `ModelProvider.generate` that Tx 1 already committed
        # the conversation, `running` generation, and original user message (DB tx released).
        observed_mid_flight: dict[str, object] = {}

        async def turn1_model(request: ModelRequest) -> ModelResponse:
            conversations = await generation_store.list_conversation_messages_for_owner(
                session_factory,
                owner_id=alice.id,
                conversation_id=observed_mid_flight_conv_id(),
            )
            observed_mid_flight["messages_during_model_call"] = [
                (m.role, m.content, m.sequence) for m in conversations
            ]
            return ModelResponse(
                model_id=request.model_id,
                content="Turn 1 assistant reply",
                usage=ModelUsage(input_tokens=10, output_tokens=5, total_tokens=15),
                finish_reason="stop",
            )

        # Prepare Turn 1 through shared preparation entry to inspect exact IDs mid-flight.
        prepared_turn1 = await conversations_service.prepare_generation(
            session_factory,
            owner_id=alice.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Hello turn 1",
            requested_model_id=None,
        )
        assert prepared_turn1.created_conversation is True
        assert prepared_turn1.persona.version_id == published.current_published_version_id
        assert prepared_turn1.model_id == "gpt-4o-mini"

        # Verify `running` generation is visible from an independent DB session before model call.
        gen_before_model = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_turn1.conversation_id,
            generation_id=prepared_turn1.generation_id,
        )
        assert gen_before_model.status == "running"
        assert gen_before_model.user_message_id == prepared_turn1.user_message_id
        assert gen_before_model.assistant_message_id is None

        def observed_mid_flight_conv_id() -> UUID:
            return prepared_turn1.conversation_id

        provider1 = _InspectingModelProvider(on_generate=turn1_model)
        committed_turn1 = await conversations_service.complete_prepared_sync_generation(
            session_factory,
            model_provider=provider1,
            prepared=prepared_turn1,
        )

        assert observed_mid_flight["messages_during_model_call"] == [
            ("user", "Hello turn 1", 1),
        ]
        assert committed_turn1.conversation_id == prepared_turn1.conversation_id
        assert committed_turn1.assistant_message.content == "Turn 1 assistant reply"
        assert committed_turn1.assistant_message.sequence == 2
        assert committed_turn1.usage == ModelUsage(
            input_tokens=10, output_tokens=5, total_tokens=15
        )

        gen_after_turn1 = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_turn1.conversation_id,
            generation_id=prepared_turn1.generation_id,
        )
        assert gen_after_turn1.status == "succeeded"
        assert gen_after_turn1.assistant_message_id == committed_turn1.assistant_message.id
        assert gen_after_turn1.total_tokens == 15

        # Turn 2: upstream timeout -> generation converges to `failed`, user message is retained.
        async def timeout_model(request: ModelRequest) -> ModelResponse:
            _ = request
            raise GatewayTimeoutError(
                code="generation_timeout",
                message="model generation timed out",
            )

        with pytest.raises(GatewayTimeoutError) as timeout_exc:
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=_InspectingModelProvider(on_generate=timeout_model),
                owner_id=alice.id,
                agent_id=agent.agent_id,
                conversation_id=prepared_turn1.conversation_id,
                user_message="Failed turn 2 input",
            )
        assert timeout_exc.value.code == "generation_timeout"

        # Turn 3: cancelled generation -> user message retained, must also be filtered out.
        prepared_cancelled = await conversations_service.prepare_generation(
            session_factory,
            owner_id=alice.id,
            agent_id=agent.agent_id,
            conversation_id=prepared_turn1.conversation_id,
            user_message="Cancelled turn 3 input",
            requested_model_id=None,
        )
        cancelled_record = await generation_store.converge_generation_failure_phase(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_cancelled.conversation_id,
            generation_id=prepared_cancelled.generation_id,
            lease_token=prepared_cancelled.lease_token,
            target_status="cancelled",
            error_code="generation_cancelled",
            error_message="generation cancelled",
        )
        assert cancelled_record is not None
        assert cancelled_record.status == "cancelled"

        # Turn 4: succeeds and MUST only see Turn 1 succeeded messages + Turn 4 input in Prompt,
        # while Turn 2 (failed) and Turn 3 (cancelled) user messages remain persisted in DB!
        async def turn4_model(request: ModelRequest) -> ModelResponse:
            return ModelResponse(
                model_id=request.model_id,
                content="Turn 4 assistant reply",
                usage=ModelUsage(input_tokens=20, output_tokens=6, total_tokens=26),
                finish_reason="stop",
            )

        provider4 = _InspectingModelProvider(on_generate=turn4_model)
        committed_turn4 = await conversations_service.run_sync_chat(
            session_factory,
            model_provider=provider4,
            owner_id=alice.id,
            agent_id=agent.agent_id,
            conversation_id=prepared_turn1.conversation_id,
            user_message="Hello turn 4",
        )
        assert committed_turn4.assistant_message.content == "Turn 4 assistant reply"

        sent_messages = provider4.requests[0].messages
        non_system = [(m.role, m.content) for m in sent_messages[1:]]
        assert non_system == [
            ("user", "Hello turn 1"),
            ("assistant", "Turn 1 assistant reply"),
            ("user", "Hello turn 4"),
        ]

        all_messages = await generation_store.list_conversation_messages_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_turn1.conversation_id,
        )
        assert [(m.role, m.content) for m in all_messages] == [
            ("user", "Hello turn 1"),
            ("assistant", "Turn 1 assistant reply"),
            ("user", "Failed turn 2 input"),
            ("user", "Cancelled turn 3 input"),
            ("user", "Hello turn 4"),
            ("assistant", "Turn 4 assistant reply"),
        ]
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_commit_failure_rolls_back_assistant_reply_atomically_and_converges_failed(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await create_builtin_user(session_factory, email="alice-atomic-commit@example.com")
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
            owner_id=alice.id,
            data=AgentCreateRequest(
                name="Atomic Agent",
                systemPrompt="Remote persona prompt",
                defaultModelId="gpt-4o-mini",
            ),
        )
        await agents_service.publish_agent(
            session_factory, owner_id=alice.id, agent_id=agent.agent_id
        )

        prepared = await conversations_service.prepare_generation(
            session_factory,
            owner_id=alice.id,
            agent_id=agent.agent_id,
            conversation_id=None,
            user_message="Trigger commit rollback",
            requested_model_id=None,
        )

        async def ok_model(request: ModelRequest) -> ModelResponse:
            return ModelResponse(
                model_id=request.model_id,
                content="Should be rolled back",
                usage=ModelUsage(input_tokens=4, output_tokens=4, total_tokens=8),
                finish_reason="stop",
            )

        async def failing_before_commit_hook(
            session: AsyncSession,
            generation: GenerationRecord,
            assistant_message: MessageRecord,
        ) -> None:
            _ = (session, generation, assistant_message)
            raise RuntimeError("simulated database failure inside commit tx")

        with pytest.raises(ServiceUnavailableError) as exc_info:
            await conversations_service.complete_prepared_sync_generation(
                session_factory,
                model_provider=_InspectingModelProvider(on_generate=ok_model),
                prepared=prepared,
                before_commit_hook=failing_before_commit_hook,
            )
        assert exc_info.value.code == "generation_commit_failed"

        # Assistant message MUST have been rolled back; user message preserved; generation `failed`.
        messages = await generation_store.list_conversation_messages_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared.conversation_id,
        )
        assert [(m.role, m.content) for m in messages] == [("user", "Trigger commit rollback")]

        gen = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
        )
        assert gen.status == "failed"
        assert gen.assistant_message_id is None
        assert gen.error_code == "generation_commit_failed"

        # Already-failed or succeeded check: conversation ownership check enforces isolation.
        bob = await create_builtin_user(session_factory, email="bob-atomic-commit@example.com")
        await generation_store.ensure_conversation_accessible(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared.conversation_id,
        )
        with pytest.raises(NotFoundError) as bob_err:
            await generation_store.ensure_conversation_accessible(
                session_factory,
                owner_id=bob.id,
                conversation_id=prepared.conversation_id,
            )
        assert bob_err.value.code == "conversation_not_found"
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_unavailable_default_model_and_upstream_error_paths(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        alice = await create_builtin_user(session_factory, email="alice-model-err@example.com")
        await models_service.sync_model_configs(
            session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="disabled-model",
                    provider_id="openai_compatible",
                    upstream_model="disabled-model",
                    display_name="Disabled",
                    enabled=False,
                ),
                ModelConfigSeed(
                    model_id="enabled-model",
                    provider_id="openai_compatible",
                    upstream_model="enabled-model",
                    display_name="Enabled",
                    enabled=True,
                ),
            ],
        )

        # Agent with disabled default model -> rejected before external call.
        agent_disabled = await agents_service.create_agent(
            session_factory,
            owner_id=alice.id,
            data=AgentCreateRequest.model_validate(
                {
                    "name": "Disabled Model Agent",
                    "systemPrompt": "Hello",
                    "defaultModelId": "disabled-model",
                }
            ),
        )
        await agents_service.publish_agent(
            session_factory,
            owner_id=alice.id,
            agent_id=agent_disabled.agent_id,
        )

        model_calls = 0

        async def counting_model(request: ModelRequest) -> ModelResponse:
            nonlocal model_calls
            model_calls += 1
            raise BadGatewayError(
                code="model_upstream_error",
                message="upstream failed",
            )

        provider = _InspectingModelProvider(on_generate=counting_model)

        with pytest.raises(ConflictError) as model_err:
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice.id,
                agent_id=agent_disabled.agent_id,
                conversation_id=None,
                user_message="Should fail pre-call",
            )
        assert model_err.value.code == "model_unavailable"
        assert model_calls == 0

        # Agent with enabled default model hitting upstream 502 error -> converges to failed.
        agent_ok = await agents_service.create_agent(
            session_factory,
            owner_id=alice.id,
            data=AgentCreateRequest.model_validate(
                {
                    "name": "Enabled Model Agent",
                    "systemPrompt": "Hello",
                    "defaultModelId": "enabled-model",
                }
            ),
        )
        await agents_service.publish_agent(
            session_factory,
            owner_id=alice.id,
            agent_id=agent_ok.agent_id,
        )

        with pytest.raises(BadGatewayError) as upstream_err:
            await conversations_service.run_sync_chat(
                session_factory,
                model_provider=provider,
                owner_id=alice.id,
                agent_id=agent_ok.agent_id,
                conversation_id=None,
                user_message="Will fail upstream",
            )
        assert upstream_err.value.code == "model_upstream_error"
        assert model_calls == 1

        # Overall generation deadline enforces `generation_timeout` even when provider ignores
        # `request.timeout_seconds`, and converges the running generation to `failed`.
        async def slow_model(request: ModelRequest) -> ModelResponse:
            _ = request
            await asyncio.sleep(5.0)
            return ModelResponse(
                model_id="enabled-model",
                content="too late",
                usage=ModelUsage(),
            )

        prepared_slow = await conversations_service.prepare_generation(
            session_factory,
            owner_id=alice.id,
            agent_id=agent_ok.agent_id,
            conversation_id=None,
            user_message="Trigger wall-clock deadline",
        )
        with pytest.raises(GatewayTimeoutError) as deadline_err:
            await conversations_service.complete_prepared_sync_generation(
                session_factory,
                model_provider=_InspectingModelProvider(on_generate=slow_model),
                prepared=prepared_slow,
                timeout_seconds=0.05,
            )
        assert deadline_err.value.code == "generation_timeout"
        timed_out_gen = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_slow.conversation_id,
            generation_id=prepared_slow.generation_id,
        )
        assert timed_out_gen.status == "failed"
        assert timed_out_gen.error_code == "generation_timeout"
    finally:
        await engine.dispose()
