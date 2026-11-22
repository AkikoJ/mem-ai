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


@dataclass(frozen=True, slots=True)
class PromptHistoryMessage:
    """Normalized committed message from a prior succeeded generation turn."""

    role: HistoryRole
    content: str


def _render_system_prompt(
    *,
    platform_rules: str,
    persona: PersonaSnapshot,
    recalled_memories: Sequence[MemoryRecallItem],
) -> str:
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

    if recalled_memories:
        memory_lines = [
            f"- [{item.scope}] {item.content.strip()}"
            for item in recalled_memories
            if item.content.strip()
        ]
        if memory_lines:
            sections.append("Relevant Memories:\n" + "\n".join(memory_lines))

    return "\n\n".join(sections)


def compile_chat_prompt(
    *,
    persona: PersonaSnapshot,
    history_messages: Sequence[PromptHistoryMessage],
    current_input: str,
    recalled_memories: Sequence[MemoryRecallItem] = (),
    platform_rules: str = DEFAULT_PLATFORM_RULES,
    max_history_messages: int | None = 20,
) -> list[ModelMessage]:
    """Compile normalized project data into ordered `ModelMessage` entries."""
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

    system_content = _render_system_prompt(
        platform_rules=platform_rules,
        persona=persona,
        recalled_memories=recalled_memories,
    )

    compiled: list[ModelMessage] = [ModelMessage(role="system", content=system_content)]
    for item in bounded_history:
        compiled.append(ModelMessage(role=item.role, content=item.content))
    compiled.append(ModelMessage(role="user", content=normalized_input))
    return compiled
