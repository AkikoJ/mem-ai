"""Unit tests for deterministic scoped memory recall selection, conflict, and guard rules."""

from __future__ import annotations

from uuid import uuid4

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.modules.memories.recall import (
    MemoryRecallGuard,
    select_memories_for_prompt,
)


def _item(
    memory_id: str,
    content: str,
    *,
    scope: str = "user",
    relevance: float | None = 0.85,
    conversation_id=None,
    memory_epoch: int = 0,
    provider_id: str = "memos",
) -> MemoryRecallItem:
    return MemoryRecallItem(
        memory_id=memory_id,
        provider_id=provider_id,
        scope=scope,  # type: ignore[arg-type]
        content=content,
        relevance=relevance,
        conversation_id=conversation_id,
        memory_epoch=memory_epoch,
    )


def test_select_memories_filters_foreign_conversation_blank_and_low_relevance() -> None:
    current_conv = uuid4()
    other_conv = uuid4()
    candidates = [
        _item("mem_user_ok", "User prefers PostgreSQL", scope="user", relevance=0.72),
        _item(
            "mem_conv_ok",
            "Current conversation builds a FastAPI API",
            scope="conversation",
            conversation_id=current_conv,
            relevance=0.80,
        ),
        _item(
            "mem_other_conv",
            "Secret from another conversation",
            scope="conversation",
            conversation_id=other_conv,
            relevance=0.99,
        ),
        _item(
            "mem_conv_missing_id",
            "Conversation item missing conversation_id",
            scope="conversation",
            conversation_id=None,
            relevance=0.98,
        ),
        _item(
            "mem_user_with_conv",
            "Corrupted user item bound to conversation",
            scope="user",
            conversation_id=other_conv,
            relevance=0.97,
        ),
        _item("mem_low_rel", "Weakly related trivia", scope="user", relevance=0.49),
        _item("mem_none_rel", "Unscored memory", scope="user", relevance=None),
        _item("mem_blank", "   \n\t  ", scope="user", relevance=0.95),
    ]

    selected = select_memories_for_prompt(
        conversation_id=current_conv,
        current_input="Help design the database schema",
        candidates=candidates,
        limit=5,
        min_relevance=0.5,
        max_memory_chars=2000,
    )

    assert [item.memory_id for item in selected] == ["mem_conv_ok", "mem_user_ok"]


def test_select_memories_ranks_deduplicates_and_prefers_conversation_on_tie() -> None:
    current_conv = uuid4()
    candidates = [
        _item("mem_b", "User likes dark mode", scope="user", relevance=0.85, memory_epoch=1),
        _item(
            "mem_a",
            "Current task targets Python 3.13",
            scope="conversation",
            conversation_id=current_conv,
            relevance=0.85,
            memory_epoch=1,
        ),
        # Duplicate content with lower priority (different spacing/casing) must be dropped.
        _item("mem_dup_text", "  user   likes DARK mode ", scope="user", relevance=0.80),
        # Exact duplicate (provider_id, memory_id) must be deduplicated.
        _item("mem_b", "User likes dark mode", scope="user", relevance=0.85, memory_epoch=1),
        # Exact duplicate of current user input is redundant and excluded.
        _item("mem_echo", "How should we structure routers?", scope="user", relevance=0.99),
        # Higher relevance item ranks first.
        _item("mem_top", "Project uses modular monolith", scope="user", relevance=0.95),
    ]

    selected = select_memories_for_prompt(
        conversation_id=current_conv,
        current_input="  how should we structure routers? ",
        candidates=candidates,
        limit=5,
        min_relevance=0.5,
        max_memory_chars=2000,
    )

    assert [item.memory_id for item in selected] == ["mem_top", "mem_a", "mem_b"]


def test_select_memories_excludes_conflicting_memories_and_preserves_generic_prefixes() -> None:
    current_conv = uuid4()
    candidates = [
        # Conflicts directly with explicit override in current_input -> excluded.
        _item("mem_lang_old", "preferred_language: Java", scope="user", relevance=0.96),
        # Multi-word spaced attribute key overridden in current_input -> also excluded.
        _item("mem_color_old", "favorite color: blue", scope="user", relevance=0.95),
        # Two memories for the same attribute key `timezone` -> keep higher-priority one only.
        _item(
            "mem_tz_conv",
            "timezone: Asia/Shanghai",
            scope="conversation",
            conversation_id=current_conv,
            relevance=0.90,
            memory_epoch=2,
        ),
        _item(
            "mem_tz_stale",
            "timezone: UTC",
            scope="user",
            relevance=0.88,
            memory_epoch=1,
        ),
        # Generic label prefixes ("Note:") must NOT trigger false-positive conflict drops.
        _item("mem_note_1", "Note: user prefers spicy food", scope="user", relevance=0.82),
        _item("mem_note_2", "Note: user works from home", scope="user", relevance=0.81),
    ]

    selected = select_memories_for_prompt(
        conversation_id=current_conv,
        current_input="preferred_language: Python. favorite color: red. Please schedule our sync.",
        candidates=candidates,
        limit=10,
        min_relevance=0.5,
        max_memory_chars=2000,
    )

    assert [item.memory_id for item in selected] == [
        "mem_tz_conv",
        "mem_note_1",
        "mem_note_2",
    ]


def test_select_memories_enforces_limit_and_character_budget_deterministically() -> None:
    current_conv = uuid4()
    candidates = [
        _item("mem_1", "A" * 40, relevance=0.95),
        _item("mem_2_too_long", "B" * 200, relevance=0.90),
        _item("mem_3", "C" * 30, relevance=0.85),
        _item("mem_4", "D" * 30, relevance=0.80),
    ]

    # Budget fits mem_1 (40) and mem_3 (30), skips oversized mem_2_too_long, and stops at limit=2.
    selected = select_memories_for_prompt(
        conversation_id=current_conv,
        current_input="Short question",
        candidates=candidates,
        limit=2,
        min_relevance=0.5,
        max_memory_chars=90,
    )
    assert [item.memory_id for item in selected] == ["mem_1", "mem_3"]

    # Zero limit or zero budget always yields empty tuple.
    assert (
        select_memories_for_prompt(
            conversation_id=current_conv,
            current_input="Short question",
            candidates=candidates,
            limit=0,
            min_relevance=0.5,
            max_memory_chars=500,
        )
        == ()
    )
    assert (
        select_memories_for_prompt(
            conversation_id=current_conv,
            current_input="Short question",
            candidates=candidates,
            limit=5,
            min_relevance=0.5,
            max_memory_chars=0,
        )
        == ()
    )


def test_select_memories_respects_barrier_epoch_and_tombstone_guard() -> None:
    current_conv = uuid4()
    candidates = [
        _item("mem_stale_epoch", "Old epoch fact", scope="user", relevance=0.95, memory_epoch=1),
        _item("mem_tombstoned", "Deleted fact", scope="user", relevance=0.94, memory_epoch=2),
        _item(
            "mem_conv_active",
            "Conversation fact",
            scope="conversation",
            conversation_id=current_conv,
            relevance=0.93,
            memory_epoch=2,
        ),
        _item("mem_valid", "Current epoch fact", scope="user", relevance=0.90, memory_epoch=2),
    ]

    # Clearing barrier degrades immediately to empty.
    clearing_guard = MemoryRecallGuard(
        provider_id="memos",
        current_epoch=2,
        barrier_state="clearing",
    )
    assert (
        select_memories_for_prompt(
            conversation_id=current_conv,
            current_input="question",
            candidates=candidates,
            limit=5,
            min_relevance=0.5,
            max_memory_chars=2000,
            guard=clearing_guard,
        )
        == ()
    )

    # Idle guard filters stale epochs, tombstoned memory IDs, and tombstoned conversation scope.
    tombstone_guard = MemoryRecallGuard(
        provider_id="memos",
        current_epoch=2,
        barrier_state="idle",
        tombstoned_memory_ids=frozenset({"mem_tombstoned"}),
        conversation_tombstoned=True,
    )
    selected = select_memories_for_prompt(
        conversation_id=current_conv,
        current_input="question",
        candidates=candidates,
        limit=5,
        min_relevance=0.5,
        max_memory_chars=2000,
        guard=tombstone_guard,
    )
    assert [item.memory_id for item in selected] == ["mem_valid"]
