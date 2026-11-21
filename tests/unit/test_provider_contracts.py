"""Unit tests for project-owned provider contracts and data types."""

from __future__ import annotations

from uuid import uuid4

import pytest

from mem_ai.contracts.memory_provider import MemoryProvider, MemoryRecallQuery, MemoryWriteInput
from mem_ai.contracts.model_provider import ModelMessage, ModelProvider, ModelRequest
from mem_ai.contracts.persona_provider import PersonaProvider
from tests.conftest import FakeMemoryProvider, FakeModelProvider, FakePersonaProvider


@pytest.mark.anyio
async def test_provider_protocols_accept_typed_implementations() -> None:
    model_provider: ModelProvider = FakeModelProvider(prefix="echo:")
    memory_provider: MemoryProvider = FakeMemoryProvider()
    persona_provider: PersonaProvider = FakePersonaProvider()

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
    assert response.usage.total_tokens == 8

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
    created = await memory_provider.create_memory(
        MemoryWriteInput(
            write_id="w_1",
            user_id=user_id,
            scope="user",
            content="User prefers concise answers.",
        )
    )
    assert created.status == "succeeded"
    assert created.memory_id is not None

    recalled = await memory_provider.recall(
        MemoryRecallQuery(user_id=user_id, query="style", scope="user", limit=5)
    )
    assert recalled[0].memory_id == created.memory_id

    persona = await persona_provider.get_latest_published(owner_id=user_id, agent_id=uuid4())
    assert persona is not None
    assert persona.memory_policy.write_scope == "user"
