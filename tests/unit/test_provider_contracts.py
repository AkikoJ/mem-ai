"""Unit tests for project-owned provider contracts and data types."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryProvider,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryRecord,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.contracts.model_provider import (
    ModelMessage,
    ModelProvider,
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.contracts.persona_provider import MemoryPolicy, PersonaProvider, PersonaSnapshot


class _StubModelProvider:
    async def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            model_id=request.model_id,
            content=f"echo:{request.messages[-1].content}",
            usage=ModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        yield ModelStreamChunk(sequence=1, text="echo:")
        yield ModelStreamChunk(
            sequence=2,
            text=request.messages[-1].content,
            is_final=True,
            usage=ModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
        )


class _StubMemoryProvider:
    provider_id = "stub-memory"

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        return [
            MemoryRecallItem(
                memory_id="mem_1",
                provider_id=self.provider_id,
                scope=query.scope,
                content="User prefers concise answers.",
                relevance=0.95,
            )
        ]

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return MemoryWriteResult(
            status="succeeded",
            provider_id=self.provider_id,
            memory_id="mem_created",
            external_ref=item.write_id,
        )

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return MemoryWriteResult(
            status="accepted",
            provider_id=self.provider_id,
            external_ref=item.write_id,
        )

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        return MemoryListPage(
            items=[
                MemoryRecord(
                    memory_id="mem_1",
                    provider_id=self.provider_id,
                    user_id=query.user_id,
                    scope="user",
                    content="User prefers concise answers.",
                    created_at=datetime.now(UTC),
                )
            ],
            next_cursor=None,
            has_more=False,
        )

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


class _StubPersonaProvider:
    async def get_latest_published(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
    ) -> PersonaSnapshot | None:
        return PersonaSnapshot(
            agent_id=agent_id,
            version_id=uuid4(),
            version=1,
            system_prompt="You are a helpful assistant.",
            behavior_rules=("Be concise",),
            tone="calm",
            language="zh-CN",
            default_model_id="gpt-4o-mini",
            memory_policy=MemoryPolicy(enabled=True, write_scope="user", recall_limit=5),
            published_at=datetime.now(UTC),
        )

    async def get_version(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
        version_id: UUID,
    ) -> PersonaSnapshot | None:
        return PersonaSnapshot(
            agent_id=agent_id,
            version_id=version_id,
            version=1,
            system_prompt="You are a helpful assistant.",
            behavior_rules=(),
            tone=None,
            language=None,
            default_model_id=None,
            memory_policy=MemoryPolicy(),
            published_at=datetime.now(UTC),
        )


@pytest.mark.anyio
async def test_provider_protocols_accept_typed_implementations() -> None:
    model_provider: ModelProvider = _StubModelProvider()
    memory_provider: MemoryProvider = _StubMemoryProvider()
    persona_provider: PersonaProvider = _StubPersonaProvider()

    assert isinstance(model_provider, ModelProvider)
    assert isinstance(memory_provider, MemoryProvider)
    assert isinstance(persona_provider, PersonaProvider)

    response = await model_provider.generate(
        ModelRequest(
            model_id="gpt-4o-mini",
            messages=[ModelMessage(role="user", content="hello")],
        )
    )
    assert response.content == "echo:hello"
    assert response.usage.total_tokens == 6

    chunks = [
        chunk
        async for chunk in model_provider.stream_generate(
            ModelRequest(
                model_id="gpt-4o-mini",
                messages=[ModelMessage(role="user", content="hi")],
            )
        )
    ]
    assert "".join(c.text for c in chunks) == "echo:hi"
    assert chunks[-1].is_final is True

    user_id = uuid4()
    recalled = await memory_provider.recall(
        MemoryRecallQuery(user_id=user_id, query="style", scope="user", limit=5)
    )
    assert recalled[0].memory_id == "mem_1"

    persona = await persona_provider.get_latest_published(owner_id=user_id, agent_id=uuid4())
    assert persona is not None
    assert persona.memory_policy.write_scope == "user"
