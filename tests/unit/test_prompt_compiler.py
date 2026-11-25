"""Unit tests for pure prompt compilation from project-owned domain types."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelMessage
from mem_ai.modules.agents.schemas import MemoryPolicy, PersonaSnapshot
from mem_ai.modules.conversations.prompt import (
    DEFAULT_PLATFORM_RULES,
    CompiledPrompt,
    PromptHistoryMessage,
    compile_chat_context,
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


def test_compile_chat_context_enforces_budget_prioritizing_current_input_and_tracks_injected() -> (
    None
):
    persona = _make_persona(behavior_rules=(), tone=None, language=None)
    recalled = [
        MemoryRecallItem(
            memory_id="mem_1",
            provider_id="memos",
            scope="user",
            content="User prefers concise Python\nwith type hints.",
            relevance=0.95,
        ),
        MemoryRecallItem(
            memory_id="mem_2",
            provider_id="memos",
            scope="conversation",
            content="X" * 300,
            relevance=0.90,
            conversation_id=uuid4(),
        ),
        MemoryRecallItem(
            memory_id="mem_3",
            provider_id="memos",
            scope="user",
            content="User uses PostgreSQL 17.",
            relevance=0.85,
        ),
    ]
    history = [
        PromptHistoryMessage(role="user", content="Earlier question"),
        PromptHistoryMessage(role="assistant", content="Earlier answer"),
    ]
    current_input = "Current high-priority input that must never be dropped."

    # Compute tight budget that fits base system + current_input + history + mem_1 + mem_3,
    # while excluding oversized mem_2.
    base_only = compile_chat_context(
        persona=persona,
        history_messages=history,
        current_input=current_input,
        recalled_memories=(),
    )
    assert isinstance(base_only, CompiledPrompt)
    assert "Relevant Memories:" not in base_only.messages[0].content
    assert base_only.injected_memories == ()
    base_chars = sum(len(m.content) for m in base_only.messages)

    tight = compile_chat_context(
        persona=persona,
        history_messages=history,
        current_input=current_input,
        recalled_memories=recalled,
        max_prompt_chars=base_chars + 150,
    )
    assert [m.memory_id for m in tight.injected_memories] == ["mem_1", "mem_3"]
    assert tight.messages[-1] == ModelMessage(role="user", content=current_input)
    # Multi-line memory content is normalized cleanly into a single uniform bullet line.
    assert "- [user] User prefers concise Python with type hints." in tight.messages[0].content
    assert "- [user] User uses PostgreSQL 17." in tight.messages[0].content
    assert "X" * 50 not in tight.messages[0].content

    # Extremely tight budget drops all memories (and even older history) but ALWAYS keeps
    # platform rules, persona, and current user input intact.
    minimal = compile_chat_context(
        persona=persona,
        history_messages=history,
        current_input=current_input,
        recalled_memories=recalled,
        max_prompt_chars=len(current_input) + 10,
    )
    assert minimal.injected_memories == ()
    assert minimal.messages[-1] == ModelMessage(role="user", content=current_input)
    assert persona.system_prompt in minimal.messages[0].content
    assert "Relevant Memories:" not in minimal.messages[0].content
