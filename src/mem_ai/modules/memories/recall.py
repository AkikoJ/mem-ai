"""Scoped memory recall policy, deterministic selection, degradation, and epoch/tombstone seam.

Owned by the `memories` module and re-exported through `memories.service` for Conversation Core
(both synchronous and future SSE flows). Ticket 13 extends `load_recall_guard_in_session` with
real `memory_tombstones` queries without changing Conversation Core or adapter contracts.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryProvider,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryScope,
)
from mem_ai.core.exceptions import AppError
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.core.telemetry import current_telemetry, current_tracer
from mem_ai.modules.memories import repository
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.memories.recall")

MemoryBarrierState = Literal["idle", "clearing", "reconciling"]

# Seam for Ticket 13 (`memory_tombstones`) to provide active tombstoned memory IDs and whether
# the current conversation scope is tombstoned inside the caller's DB session.
TombstoneLookup = Callable[
    [AsyncSession, UUID, str, UUID | None],
    Awaitable[tuple[frozenset[str], bool]],
]

_GENERIC_LABEL_KEYS = frozenset(
    {
        "assistant",
        "context",
        "fact",
        "goal",
        "info",
        "memory",
        "note",
        "preference",
        "rule",
        "summary",
        "tip",
        "user",
    }
)
_KV_LINE_PATTERN = re.compile(
    r"^\s*([A-Za-z][A-Za-z0-9_.\- ]{1,39}|[\u4e00-\u9fff]{2,12})\s*[:=：]\s*(.+?)\s*$"
)
_INPUT_KV_PATTERN = re.compile(
    r"(?:^|[\n.;。；!?！？])\s*"
    r"([A-Za-z][A-Za-z0-9_.\- ]{1,39}|[\u4e00-\u9fff]{2,12})"
    r"\s*[:=：]\s*([^\n.;。；!?！？]+)"
)


class MemoryRecallGuard(BaseModel):
    """Snapshot of `(user, provider)` epoch, clearing barrier, and tombstone state."""

    model_config = ConfigDict(frozen=True)

    provider_id: str = Field(min_length=1)
    current_epoch: int = Field(default=0, ge=0)
    barrier_state: MemoryBarrierState = "idle"
    tombstoned_memory_ids: frozenset[str] = Field(default_factory=frozenset)
    conversation_tombstoned: bool = False

    @property
    def is_blocked(self) -> bool:
        """Return True when a clearing or reconciling barrier suppresses recall."""
        return self.barrier_state != "idle"


class MemoryRecallPolicy(BaseModel):
    """Effective runtime budgets governing scoped memory recall for a generation turn."""

    model_config = ConfigDict(frozen=True)

    enabled: bool = True
    recall_limit: int = Field(default=5, ge=0, le=50)
    min_relevance: float = Field(default=0.5, ge=0.0, le=1.0)
    max_memory_chars: int = Field(default=2000, ge=0)
    timeout_seconds: float | None = Field(default=5.0, gt=0.0)


class MemoryRecallOutcome(BaseModel):
    """Normalized result of policy-guarded scoped recall before prompt compilation."""

    model_config = ConfigDict(frozen=True)

    items: tuple[MemoryRecallItem, ...] = ()
    provider_id: str | None = None
    captured_epoch: int = Field(default=0, ge=0)
    degraded: bool = False
    degradation_reason: str | None = None


def _normalize_text(value: str) -> str:
    return " ".join(value.split()).strip()


def _normalize_key(raw_key: str) -> str | None:
    compact = " ".join(raw_key.strip().lower().split())
    if not compact or compact in _GENERIC_LABEL_KEYS:
        return None
    if len(compact.split()) > 3:
        return None
    return compact


def _extract_attribute_pair(text: str) -> tuple[str, str] | None:
    """Extract `(normalized_key, normalized_value)` from structured `key: value` memory lines."""
    match = _KV_LINE_PATTERN.match(text)
    if match is None:
        return None
    key = _normalize_key(match.group(1))
    if key is None:
        return None
    val = _normalize_text(match.group(2)).lower()
    if not val:
        return None
    return (key, val)


def _extract_input_overrides(current_input: str) -> dict[str, set[str]]:
    """Extract explicit `key: value` assertions declared in the current user input."""
    overrides: dict[str, set[str]] = {}
    for match in _INPUT_KV_PATTERN.finditer(current_input):
        key = _normalize_key(match.group(1))
        if key is None:
            continue
        val = _normalize_text(match.group(2)).lower()
        if not val:
            continue
        overrides.setdefault(key, set()).add(val)
    return overrides


def _passes_guard(item: MemoryRecallItem, guard: MemoryRecallGuard | None) -> bool:
    if guard is None:
        return True
    if guard.is_blocked:
        return False
    if item.memory_epoch < guard.current_epoch:
        return False
    if item.memory_id in guard.tombstoned_memory_ids:
        return False
    return not (item.scope == "conversation" and guard.conversation_tombstoned)


def _passes_scope_binding(item: MemoryRecallItem, *, conversation_id: UUID) -> bool:
    if item.scope == "conversation":
        return item.conversation_id == conversation_id
    return item.conversation_id is None


def select_memories_for_prompt(
    *,
    conversation_id: UUID,
    current_input: str,
    candidates: Sequence[MemoryRecallItem],
    limit: int,
    min_relevance: float = 0.5,
    max_memory_chars: int = 2000,
    guard: MemoryRecallGuard | None = None,
) -> tuple[MemoryRecallItem, ...]:
    """Deterministically filter, rank, deduplicate, conflict-prune, and budget recalled memories."""
    if limit <= 0 or max_memory_chars <= 0:
        return ()
    if guard is not None and guard.is_blocked:
        return ()

    normalized_input = _normalize_text(current_input).lower()
    input_overrides = _extract_input_overrides(current_input)

    eligible: list[MemoryRecallItem] = []
    for item in candidates:
        cleaned = _normalize_text(item.content)
        if not cleaned:
            continue
        if not _passes_scope_binding(item, conversation_id=conversation_id):
            continue
        if not _passes_guard(item, guard):
            continue
        if item.relevance is None:
            if min_relevance > 0.0:
                continue
        elif item.relevance < min_relevance:
            continue
        if cleaned.lower() == normalized_input:
            continue
        eligible.append(item)

    # Deterministic priority: higher relevance -> conversation scope -> newer epoch -> memory_id.
    eligible.sort(
        key=lambda item: (
            -(item.relevance if item.relevance is not None else -1.0),
            0 if item.scope == "conversation" else 1,
            -item.memory_epoch,
            item.provider_id,
            item.memory_id,
        )
    )

    selected: list[MemoryRecallItem] = []
    seen_external_ids: set[tuple[str, str]] = set()
    seen_contents: set[str] = set()
    seen_attr_keys: dict[str, str] = {}
    used_chars = 0

    for item in eligible:
        if len(selected) >= limit:
            break
        cleaned = _normalize_text(item.content)
        ext_key = (item.provider_id, item.memory_id)
        if ext_key in seen_external_ids:
            continue
        content_key = cleaned.lower()
        if content_key in seen_contents:
            continue

        attr = _extract_attribute_pair(cleaned)
        if attr is not None:
            attr_key, attr_val = attr
            # Exclude memory whose explicit attribute is overridden by the current user input.
            if attr_key in input_overrides and attr_val not in input_overrides[attr_key]:
                continue
            # Exclude lower-priority memory stating a conflicting value for the same attribute key.
            if attr_key in seen_attr_keys and seen_attr_keys[attr_key] != attr_val:
                continue

        if used_chars + len(cleaned) > max_memory_chars:
            continue

        seen_external_ids.add(ext_key)
        seen_contents.add(content_key)
        if attr is not None:
            seen_attr_keys[attr[0]] = attr[1]
        used_chars += len(cleaned)
        selected.append(item)

    return tuple(selected)


async def get_memory_epoch_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    for_share: bool = False,
) -> int:
    """Read the current `(user_id, provider_id)` memory epoch without creating a scope row."""
    scope_row = await repository.get_scope(
        session,
        user_id=user_id,
        provider_id=provider_id,
        for_share=for_share,
    )
    return scope_row.epoch if scope_row is not None else 0


async def load_recall_guard_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    conversation_id: UUID | None = None,
    tombstone_lookup: TombstoneLookup | None = None,
) -> MemoryRecallGuard:
    """Load `(epoch, barrier_state, tombstones)` inside an active DB session."""
    scope_row = await repository.get_scope(
        session,
        user_id=user_id,
        provider_id=provider_id,
    )
    current_epoch = scope_row.epoch if scope_row is not None else 0
    barrier_state: MemoryBarrierState = (
        scope_row.barrier_state  # type: ignore[assignment]
        if scope_row is not None and scope_row.barrier_state in ("idle", "clearing", "reconciling")
        else "idle"
    )

    if tombstone_lookup is not None:
        tombstoned_ids, conversation_tombstoned = await tombstone_lookup(
            session,
            user_id,
            provider_id,
            conversation_id,
        )
    else:
        tombstoned_ids, conversation_tombstoned = await repository.load_active_tombstones(
            session,
            user_id=user_id,
            provider_id=provider_id,
            conversation_id=conversation_id,
        )

    return MemoryRecallGuard(
        provider_id=provider_id,
        current_epoch=current_epoch,
        barrier_state=barrier_state,
        tombstoned_memory_ids=tombstoned_ids,
        conversation_tombstoned=conversation_tombstoned,
    )


async def filter_injectable_memories_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    provider_id: str,
    conversation_id: UUID,
    candidates: Sequence[MemoryRecallItem],
    captured_epoch: int,
    tombstone_lookup: TombstoneLookup | None = None,
) -> list[MemoryRecallItem]:
    """Re-verify barrier, epoch, and tombstones in Tx 1.5 before saving injected references."""
    if not candidates:
        return []
    guard = await load_recall_guard_in_session(
        session,
        user_id=user_id,
        provider_id=provider_id,
        conversation_id=conversation_id,
        tombstone_lookup=tombstone_lookup,
    )
    if guard.is_blocked or guard.current_epoch > captured_epoch:
        return []
    return [
        item
        for item in candidates
        if _passes_scope_binding(item, conversation_id=conversation_id)
        and _passes_guard(item, guard)
    ]


async def _invoke_scope_recall(
    *,
    memory_provider: MemoryProvider,
    query: MemoryRecallQuery,
    timeout_seconds: float | None,
) -> tuple[list[MemoryRecallItem], str | None]:
    """Invoke `memory_provider.recall` for one scope, converting failures to sanitized codes."""
    try:
        coro = memory_provider.recall(query)
        items = (
            await asyncio.wait_for(coro, timeout=timeout_seconds)
            if timeout_seconds is not None
            else await coro
        )
        return items, None
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        logger.warning(
            "Memory recall timed out; degrading to empty scope results",
            extra={
                "extra_fields": {
                    "provider": memory_provider.provider_id,
                    "scope": query.scope,
                    "error_code": "memory_provider_timeout",
                }
            },
        )
        return [], "memory_provider_timeout"
    except AppError as exc:
        logger.warning(
            "Memory recall failed with application error; degrading gracefully",
            extra={
                "extra_fields": {
                    "provider": memory_provider.provider_id,
                    "scope": query.scope,
                    "error_code": exc.code,
                }
            },
        )
        return [], exc.code
    except Exception:
        logger.warning(
            "Unexpected error during memory recall; degrading gracefully",
            extra={
                "extra_fields": {
                    "provider": memory_provider.provider_id,
                    "scope": query.scope,
                    "error_code": "memory_provider_error",
                }
            },
        )
        return [], "memory_provider_error"


async def recall_relevant_memories(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider | None,
    user_id: UUID,
    conversation_id: UUID,
    query: str,
    policy: MemoryRecallPolicy | None = None,
    tombstone_lookup: TombstoneLookup | None = None,
) -> MemoryRecallOutcome:
    """Recall and select user + conversation memories outside DB tx with safe degradation."""
    resolved_policy = policy if policy is not None else MemoryRecallPolicy()
    if (
        memory_provider is None
        or not resolved_policy.enabled
        or resolved_policy.recall_limit <= 0
        or resolved_policy.max_memory_chars <= 0
    ):
        return MemoryRecallOutcome(
            items=(),
            provider_id=memory_provider.provider_id if memory_provider is not None else None,
            captured_epoch=0,
            degraded=False,
        )

    provider_id = memory_provider.provider_id
    telemetry = current_telemetry()
    tracer = current_tracer()
    log_tokens = bind_log_context(provider=provider_id)
    try:
        with tracer.start_span(
            "memory.recall",
            attributes={"provider": provider_id, "conversation_id": str(conversation_id)},
        ) as span:
            async with session_factory() as session:
                await users_service.ensure_user_active_in_session(session, user_id=user_id)
                guard = await load_recall_guard_in_session(
                    session,
                    user_id=user_id,
                    provider_id=provider_id,
                    conversation_id=conversation_id,
                    tombstone_lookup=tombstone_lookup,
                )

            if guard.is_blocked:
                telemetry.record_memory_recall(
                    provider=provider_id,
                    outcome="degraded",
                    reason="memory_scope_clearing",
                )
                span.set_attribute("memory.degraded", True)
                span.set_attribute("memory.degradation_reason", "memory_scope_clearing")
                return MemoryRecallOutcome(
                    items=(),
                    provider_id=provider_id,
                    captured_epoch=guard.current_epoch,
                    degraded=True,
                    degradation_reason="memory_scope_clearing",
                )

            normalized_query = _normalize_text(query)
            if not normalized_query:
                return MemoryRecallOutcome(
                    items=(),
                    provider_id=provider_id,
                    captured_epoch=guard.current_epoch,
                    degraded=False,
                )

            scopes_to_query: list[tuple[MemoryScope, UUID | None]] = [("user", None)]
            if not guard.conversation_tombstoned:
                scopes_to_query.append(("conversation", conversation_id))

            recall_queries = [
                MemoryRecallQuery(
                    user_id=user_id,
                    query=normalized_query,
                    scope=scope,
                    conversation_id=conv_id,
                    limit=resolved_policy.recall_limit,
                    min_relevance=resolved_policy.min_relevance,
                )
                for scope, conv_id in scopes_to_query
            ]

            results = await asyncio.gather(
                *(
                    _invoke_scope_recall(
                        memory_provider=memory_provider,
                        query=recall_query,
                        timeout_seconds=resolved_policy.timeout_seconds,
                    )
                    for recall_query in recall_queries
                )
            )

            candidates: list[MemoryRecallItem] = []
            degradation_reason: str | None = None
            for scope_items, err_code in results:
                if err_code is not None and degradation_reason is None:
                    degradation_reason = err_code
                candidates.extend(scope_items)

            selected = select_memories_for_prompt(
                conversation_id=conversation_id,
                current_input=normalized_query,
                candidates=candidates,
                limit=resolved_policy.recall_limit,
                min_relevance=resolved_policy.min_relevance,
                max_memory_chars=resolved_policy.max_memory_chars,
                guard=guard,
            )
            if degradation_reason is not None:
                telemetry.record_memory_recall(
                    provider=provider_id,
                    outcome="degraded",
                    reason=degradation_reason,
                )
                span.set_attribute("memory.degraded", True)
                span.set_attribute("memory.degradation_reason", degradation_reason)
            else:
                telemetry.record_memory_recall(provider=provider_id, outcome="ok")
                span.set_attribute("memory.degraded", False)
                span.set_status("ok")
            return MemoryRecallOutcome(
                items=selected,
                provider_id=provider_id,
                captured_epoch=guard.current_epoch,
                degraded=degradation_reason is not None,
                degradation_reason=degradation_reason,
            )
    finally:
        reset_log_context(log_tokens)
