"""Unit tests for pure prompt compilation from project-owned domain types."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelMessage
from mem_ai.contracts.persona_provider import MemoryPolicy, PersonaSnapshot
from mem_ai.modules.conversations.prompt import (
    DEFAULT_PLATFORM_RULES,
    PromptHistoryMessage,
    compile_chat_prompt,
)


def _make_persona(
    *,
    system_prompt: str = "You are a helpful coding assistant.",
    behavior_rules: tuple[str, ...] = ("Be direct", "Cite assumptions"),
    tone: str | None = "calm",
    language: str | None = "zh-CN",
) -> PersonaSnapshot:
    return PersonaSnapshot(
        agent_id=uuid4(),
        version_id=uuid4(),
        version=2,
        system_prompt=system_prompt,
        behavior_rules=behavior_rules,
        tone=tone,
        language=language,
        default_model_id="gpt-4o-mini",
        memory_policy=MemoryPolicy(),
        published_at=datetime.now(UTC),
    )


def test_compile_chat_prompt_layers_platform_persona_history_and_current_input() -> None:
    persona = _make_persona()
    history = [
        PromptHistoryMessage(role="user", content="Previous question"),
        PromptHistoryMessage(role="assistant", content="Previous answer"),
    ]

    messages = compile_chat_prompt(
        persona=persona,
        history_messages=history,
        current_input="Current question",
    )

    assert len(messages) == 4
    assert messages[0].role == "system"
    assert DEFAULT_PLATFORM_RULES in messages[0].content
    assert "You are a helpful coding assistant." in messages[0].content
    assert "Be direct" in messages[0].content
    assert "Cite assumptions" in messages[0].content
    assert "calm" in messages[0].content
    assert "zh-CN" in messages[0].content

    assert messages[1] == ModelMessage(role="user", content="Previous question")
    assert messages[2] == ModelMessage(role="assistant", content="Previous answer")
    assert messages[3] == ModelMessage(role="user", content="Current question")


def test_compile_chat_prompt_formats_recalled_memories_and_prioritizes_current_input() -> None:
    persona = _make_persona(behavior_rules=(), tone=None, language=None)
    recalled = [
        MemoryRecallItem(
            memory_id="mem_1",
            provider_id="memos",
            scope="user",
            content="User prefers concise Python examples.",
            relevance=0.92,
        ),
        MemoryRecallItem(
            memory_id="mem_2",
            provider_id="memos",
            scope="conversation",
            content="Current project uses FastAPI.",
            relevance=0.88,
            conversation_id=uuid4(),
        ),
    ]

    messages = compile_chat_prompt(
        persona=persona,
        history_messages=[],
        current_input="Show me a FastAPI dependency example.",
        recalled_memories=recalled,
    )

    assert len(messages) == 2
    system_content = messages[0].content
    assert "[user] User prefers concise Python examples." in system_content
    assert "[conversation] Current project uses FastAPI." in system_content
    assert "memos" not in system_content
    assert "mem_1" not in system_content
    assert messages[1] == ModelMessage(role="user", content="Show me a FastAPI dependency example.")


def test_compile_chat_prompt_bounds_history_while_keeping_most_recent_turns() -> None:
    persona = _make_persona()
    history = [
        PromptHistoryMessage(role="user", content=f"Turn {idx} user")
        if idx % 2 == 0
        else PromptHistoryMessage(role="assistant", content=f"Turn {idx} assistant")
        for idx in range(6)
    ]

    messages = compile_chat_prompt(
        persona=persona,
        history_messages=history,
        current_input="Latest input",
        max_history_messages=2,
    )

    assert [m.content for m in messages[1:]] == [
        "Turn 4 user",
        "Turn 5 assistant",
        "Latest input",
    ]


def test_compile_chat_prompt_rejects_empty_current_input() -> None:
    persona = _make_persona()
    with pytest.raises(ValueError, match="current_input"):
        compile_chat_prompt(
            persona=persona,
            history_messages=[],
            current_input="   ",
        )
