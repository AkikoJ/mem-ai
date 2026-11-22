"""Pure data prompt compiler combining platform rules, persona, memories, history, and input."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelMessage
from mem_ai.contracts.persona_provider import PersonaSnapshot

DEFAULT_PLATFORM_RULES = (
    "Follow the assigned persona instructions while prioritizing the user's current message. "
    "Only use recalled memories that are clearly relevant to the current request. "
    "If any recalled memory conflicts with the user's current message or appears to belong "
    "to another user, ignore that memory."
)

HistoryRole = Literal["user", "assistant"]

_RELEVANT_MEMORIES_HEADER = "Relevant Memories:\n"


@dataclass(frozen=True, slots=True)
class PromptHistoryMessage:
    """Normalized committed message from a prior succeeded generation turn."""

    role: HistoryRole
    content: str


@dataclass(frozen=True, slots=True)
class CompiledPrompt:
    """Compiled prompt messages paired with the exact subset of memories injected."""

    messages: tuple[ModelMessage, ...]
    injected_memories: tuple[MemoryRecallItem, ...]


def _format_memory_line(item: MemoryRecallItem) -> str | None:
    cleaned = " ".join(item.content.split()).strip()
    if not cleaned:
        return None
    return f"- [{item.scope}] {cleaned}"


def _render_base_system_sections(
    *,
    platform_rules: str,
    persona: PersonaSnapshot,
) -> list[str]:
    sections: list[str] = [platform_rules.strip()]

    persona_lines: list[str] = [f"Persona Instructions:\n{persona.system_prompt.strip()}"]
    if persona.behavior_rules:
        rules_text = "\n".join(f"- {rule}" for rule in persona.behavior_rules)
        persona_lines.append(f"Behavior Rules:\n{rules_text}")
    if persona.tone:
        persona_lines.append(f"Tone: {persona.tone}")
    if persona.language:
        persona_lines.append(f"Language: {persona.language}")
    sections.append("\n\n".join(persona_lines))
    return sections


def _fit_history_within_budget(
    history_messages: Sequence[PromptHistoryMessage],
    *,
    available_chars: int | None,
) -> list[PromptHistoryMessage]:
    if not history_messages:
        return []
    if available_chars is None:
        return list(history_messages)
    if available_chars <= 0:
        return []

    kept_reversed: list[PromptHistoryMessage] = []
    used = 0
    for msg in reversed(history_messages):
        msg_len = len(msg.content)
        if used + msg_len > available_chars:
            break
        kept_reversed.append(msg)
        used += msg_len
    kept_reversed.reverse()
    return kept_reversed


def _fit_memories_within_budget(
    recalled_memories: Sequence[MemoryRecallItem],
    *,
    available_chars: int | None,
) -> tuple[list[MemoryRecallItem], list[str]]:
    injected_items: list[MemoryRecallItem] = []
    rendered_lines: list[str] = []
    if not recalled_memories:
        return injected_items, rendered_lines

    header_overhead = 2 + len(_RELEVANT_MEMORIES_HEADER)  # "\n\nRelevant Memories:\n"
    used = 0
    for item in recalled_memories:
        line = _format_memory_line(item)
        if line is None:
            continue
        added_chars = (header_overhead + len(line)) if not rendered_lines else (1 + len(line))
        if available_chars is not None and used + added_chars > available_chars:
            continue
        injected_items.append(item)
        rendered_lines.append(line)
        used += added_chars
    return injected_items, rendered_lines


def compile_chat_context(
    *,
    persona: PersonaSnapshot,
    history_messages: Sequence[PromptHistoryMessage],
    current_input: str,
    recalled_memories: Sequence[MemoryRecallItem] = (),
    platform_rules: str = DEFAULT_PLATFORM_RULES,
    max_history_messages: int | None = 20,
    max_prompt_chars: int | None = None,
) -> CompiledPrompt:
    """Compile normalized project data into messages and the exact injected memory subset.

    Budget precedence:
    1. Platform rules, pinned persona instructions, and current user input are always preserved.
    2. Most recent succeeded history messages fit within the remaining budget.
    3. Recalled memories fit in priority order within whatever budget remains.
    """
    normalized_input = current_input.strip()
    if not normalized_input:
        raise ValueError("current_input must not be empty")

    if max_history_messages is not None:
        if max_history_messages < 0:
            raise ValueError("max_history_messages must be non-negative")
        bounded_history = (
            history_messages[-max_history_messages:] if max_history_messages > 0 else ()
        )
    else:
        bounded_history = history_messages

    if max_prompt_chars is not None and max_prompt_chars < 0:
        raise ValueError("max_prompt_chars must be non-negative")

    base_sections = _render_base_system_sections(
        platform_rules=platform_rules,
        persona=persona,
    )
    base_system_content = "\n\n".join(base_sections)
    mandatory_chars = len(base_system_content) + len(normalized_input)

    remaining_after_mandatory = (
        max(0, max_prompt_chars - mandatory_chars) if max_prompt_chars is not None else None
    )
    fitted_history = _fit_history_within_budget(
        bounded_history,
        available_chars=remaining_after_mandatory,
    )
    history_chars = sum(len(item.content) for item in fitted_history)
    remaining_for_memories = (
        max(0, remaining_after_mandatory - history_chars)
        if remaining_after_mandatory is not None
        else None
    )
    injected_memories, memory_lines = _fit_memories_within_budget(
        recalled_memories,
        available_chars=remaining_for_memories,
    )

    sections = list(base_sections)
    if memory_lines:
        sections.append(_RELEVANT_MEMORIES_HEADER + "\n".join(memory_lines))
    system_content = "\n\n".join(sections)

    compiled: list[ModelMessage] = [ModelMessage(role="system", content=system_content)]
    for item in fitted_history:
        compiled.append(ModelMessage(role=item.role, content=item.content))
    compiled.append(ModelMessage(role="user", content=normalized_input))
    return CompiledPrompt(
        messages=tuple(compiled),
        injected_memories=tuple(injected_memories),
    )


def compile_chat_prompt(
    *,
    persona: PersonaSnapshot,
    history_messages: Sequence[PromptHistoryMessage],
    current_input: str,
    recalled_memories: Sequence[MemoryRecallItem] = (),
    platform_rules: str = DEFAULT_PLATFORM_RULES,
    max_history_messages: int | None = 20,
    max_prompt_chars: int | None = None,
) -> list[ModelMessage]:
    """Compile normalized project data into ordered `ModelMessage` entries."""
    result = compile_chat_context(
        persona=persona,
        history_messages=history_messages,
        current_input=current_input,
        recalled_memories=recalled_memories,
        platform_rules=platform_rules,
        max_history_messages=max_history_messages,
        max_prompt_chars=max_prompt_chars,
    )
    return list(result.messages)
