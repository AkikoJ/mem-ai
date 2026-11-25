"""Unit tests for project-owned provider contracts and data types."""

from __future__ import annotations

from uuid import uuid4

import pytest

from mem_ai.contracts.memory_provider import MemoryProvider, MemoryRecallQuery, MemoryWriteInput
from mem_ai.contracts.model_provider import ModelMessage, ModelProvider, ModelRequest
from tests.conftest import FakeMemoryProvider, FakeModelProvider


@pytest.mark.anyio
async def test_provider_protocols_accept_typed_implementations() -> None:
    model_provider: ModelProvider = FakeModelProvider(prefix="echo:")
    memory_provider: MemoryProvider = FakeMemoryProvider()
    assert isinstance(model_provider, ModelProvider)
    assert isinstance(memory_provider, MemoryProvider)

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
