"""Conversation Core public orchestration for synchronous and streaming chat flows.

Implements the public orchestration boundary of Engineering Design §5.4:
- `prepare_chat_turn` / `prepare_generation`: idempotency resolution, read-only `PersonaSnapshot`
  lookup outside DB write tx, model selection, and Tx 1 lease/user-turn commit.
- `prepare_context`: scoped memory recall outside DB tx, Tx 1.5 lease/guard verification and
  metadata-only `message_recalled_memories` persistence, and budgeted prompt compilation.
- `complete_prepared_sync_generation` / `execute_prepared_sync_chat` / `run_sync_chat`:
  synchronous completion shielded from client HTTP disconnects (ADR 0004).
- `complete_prepared_stream_generation` / `cancel_stream_generation`:
  streaming lifecycle and disconnect cancellation sharing `conversations.execution` and
  `conversations.generation`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, overload
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider, MemoryRecallItem
from mem_ai.contracts.model_provider import ModelProvider, ModelResponse
from mem_ai.contracts.persona_provider import PersonaProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    NotFoundError,
    RateLimitError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.core.limits import ConcurrencyLimiter, ConcurrencyPermit
from mem_ai.core.telemetry import current_telemetry
from mem_ai.modules.conversations import execution as generation_execution
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import idempotency as idempotency_store
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations.execution import (
    DeltaCallback,
    SyncGenerationTaskRegistry,
    consume_detached_task_exception,
)
from mem_ai.modules.conversations.execution import (
    build_turn_memory_enqueue_hook as build_turn_memory_enqueue_hook,
)
from mem_ai.modules.conversations.execution import (
    execute_model_completion as execute_model_completion,
)
from mem_ai.modules.conversations.generation import (
    BeforeCommitHook,
    IdempotencyRecordHitError,
    IdempotencyScopeRaceError,
)
from mem_ai.modules.conversations.prompt import compile_chat_context
from mem_ai.modules.conversations.schemas import (
    ChatTurnOutcome,
    CommittedChatResult,
    GenerationRecord,
    IdempotencyKeySpec,
    PreparedContext,
    PreparedGeneration,
    StreamGenerationOutcome,
)
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.service import MemoryRecallPolicy, TombstoneLookup
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelRoutingPolicy
from mem_ai.modules.users import service as users_service


@dataclass(frozen=True, slots=True)
class ChatOptions:
    """Shared runtime budgets, model routing policy, memory policy, and hooks for sync & SSE."""

    model_policy: ModelRoutingPolicy | None = None
    user_permissions: frozenset[str] = frozenset()
    require_streaming: bool = False
    required_capabilities: frozenset[str] = frozenset()
    history_limit: int = 20
    lease_ttl_seconds: float = 60.0
    lease_renew_interval_seconds: float | None = 20.0
    timeout_seconds: float | None = None
    stream_timeout_seconds: float | None = None
    stream_heartbeat_interval_seconds: float = 15.0
    memory_recall_timeout_seconds: float | None = 5.0
    memory_min_relevance: float = 0.5
    memory_max_items: int = 10
    memory_context_max_chars: int = 2000
    prompt_max_chars: int | None = 24000
    max_message_chars: int = 32000
    memory_write_max_attempts: int = 5
    idempotency_retention_seconds: float = idempotency_store.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS
    tombstone_lookup: TombstoneLookup | None = None
    task_registry: SyncGenerationTaskRegistry | None = None
    before_commit_hook: BeforeCommitHook | None = None
    generation_limiter: ConcurrencyLimiter | None = None
    rate_limit_retry_after_seconds: int = 5


def chat_options_from_settings(
    settings: Settings,
    *,
    user_permissions: frozenset[str] = frozenset(),
    require_streaming: bool = False,
    task_registry: SyncGenerationTaskRegistry | None = None,
    generation_limiter: ConcurrencyLimiter | None = None,
) -> ChatOptions:
    """Construct `ChatOptions` with the shared server `ModelRoutingPolicy` and budgets."""
    return ChatOptions(
        model_policy=models_service.model_routing_policy_from_settings(settings),
        user_permissions=user_permissions,
        require_streaming=require_streaming,
        history_limit=settings.chat_history_message_limit,
        lease_ttl_seconds=float(settings.chat_lease_ttl_seconds),
        lease_renew_interval_seconds=settings.chat_lease_renew_interval_seconds,
        timeout_seconds=settings.chat_generation_timeout_seconds,
        stream_timeout_seconds=settings.chat_stream_timeout_seconds,
        stream_heartbeat_interval_seconds=settings.chat_stream_heartbeat_interval_seconds,
        memory_recall_timeout_seconds=settings.chat_memory_recall_timeout_seconds,
        memory_min_relevance=settings.chat_memory_min_relevance,
        memory_max_items=settings.chat_memory_max_items,
        memory_context_max_chars=settings.chat_memory_context_max_chars,
        prompt_max_chars=settings.chat_prompt_max_chars,
        max_message_chars=settings.chat_message_max_chars,
        memory_write_max_attempts=settings.worker_max_attempts,
        idempotency_retention_seconds=float(settings.idempotency_retention_hours * 3600),
        task_registry=task_registry,
        generation_limiter=generation_limiter,
        rate_limit_retry_after_seconds=settings.rate_limit_retry_after_seconds,
    )


def _validate_message_length(user_message: str, max_chars: int) -> None:
    if len(user_message) > max_chars:
        raise UnprocessableEntityError(
            code="validation_error",
            message=f"message exceeds maximum length of {max_chars}",
            details={"max_message_chars": max_chars},
        )


def _acquire_generation_permit(resolved: ChatOptions) -> ConcurrencyPermit | None:
    if resolved.generation_limiter is None:
        return None
    permit = resolved.generation_limiter.try_acquire()
    if permit is None:
        current_telemetry().record_rate_limit_rejection(
            scope="generations",
            reason="generation_concurrency_limit_exceeded",
        )
        raise RateLimitError(
            code="generation_concurrency_limit_exceeded",
            message="server generation concurrency budget exceeded",
            retry_after_seconds=resolved.rate_limit_retry_after_seconds,
            details={
                "limit_scope": "generations",
                "max_in_flight": resolved.generation_limiter.max_in_flight,
            },
        )
    return permit


async def prepare_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    requested_model_id: str | None = None,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    idempotency_spec: IdempotencyKeySpec | None = None,
    now: datetime | None = None,
) -> PreparedGeneration:
    """Resolve persona outside DB write tx, validate model, and commit Tx 1 preparation."""
    resolved = options if options is not None else ChatOptions()
    _validate_message_length(user_message, resolved.max_message_chars)
    await users_service.get_active_user(session_factory, user_id=owner_id)

    if conversation_id is None:
        persona = await persona_provider.get_latest_published(owner_id=owner_id, agent_id=agent_id)
        if persona is None:
            raise NotFoundError(
                code="agent_persona_not_found",
                message="published agent persona not found",
            )
    else:
        existing_conv = await conversation_queries.load_conversation_for_continuation(
            session_factory,
            owner_id=owner_id,
            conversation_id=conversation_id,
            expected_agent_id=agent_id,
        )
        persona = await persona_provider.get_version(
            owner_id=owner_id,
            agent_id=agent_id,
            version_id=existing_conv.persona_version_id,
        )
        if persona is None:
            raise NotFoundError(
                code="persona_version_not_found",
                message="pinned persona version not found",
            )

    selected_model = await models_service.resolve_model_for_chat(
        session_factory,
        agent_default_model_id=persona.default_model_id,
        requested_model_id=requested_model_id,
        user_permissions=resolved.user_permissions,
        require_streaming=resolved.require_streaming,
        required_capabilities=resolved.required_capabilities,
        policy=resolved.model_policy,
    )
    return await generation_store.prepare_generation_phase(
        session_factory,
        owner_id=owner_id,
        agent_id=agent_id,
        conversation_id=conversation_id,
        persona=persona,
        model_id=selected_model.model_id,
        upstream_model=selected_model.upstream_model,
        user_message_content=user_message,
        max_output_tokens=selected_model.max_output_tokens,
        memory_provider_id=memory_provider.provider_id if memory_provider is not None else None,
        history_limit=resolved.history_limit,
        lease_ttl_seconds=resolved.lease_ttl_seconds,
        idempotency_spec=idempotency_spec,
        idempotency_retention_seconds=resolved.idempotency_retention_seconds,
        now=now,
    )


async def prepare_context(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    recalled_memories: Sequence[MemoryRecallItem] | None = None,
    max_history_messages: int | None = None,
) -> PreparedContext:
    """Recall scoped memories outside DB tx, verify lease/guard, save refs, and compile prompt."""
    resolved = options if options is not None else ChatOptions()
    history_limit = (
        max_history_messages if max_history_messages is not None else resolved.history_limit
    )
    degraded = False
    degradation_reason: str | None = None
    captured_epoch = prepared.memory_epoch
    provider_id = memory_provider.provider_id if memory_provider is not None else None

    if recalled_memories is not None:
        candidates: tuple[MemoryRecallItem, ...] = tuple(recalled_memories)
    elif memory_provider is not None:
        policy = prepared.persona.memory_policy
        outcome = await memories_service.recall_relevant_memories(
            session_factory,
            memory_provider=memory_provider,
            user_id=prepared.owner_id,
            conversation_id=prepared.conversation_id,
            query=prepared.user_message_content,
            policy=MemoryRecallPolicy(
                enabled=policy.enabled,
                recall_limit=min(policy.recall_limit, resolved.memory_max_items)
                if policy.enabled
                else 0,
                min_relevance=resolved.memory_min_relevance,
                max_memory_chars=resolved.memory_context_max_chars,
                timeout_seconds=resolved.memory_recall_timeout_seconds,
            ),
            tombstone_lookup=resolved.tombstone_lookup,
        )
        candidates = outcome.items
        degraded = outcome.degraded
        degradation_reason = outcome.degradation_reason
        captured_epoch = max(captured_epoch, outcome.captured_epoch)
    else:
        candidates = ()

    def _compile(items: Sequence[MemoryRecallItem]) -> Any:
        return compile_chat_context(
            persona=prepared.persona,
            history_messages=prepared.history_messages,
            current_input=prepared.user_message_content,
            recalled_memories=items,
            max_history_messages=history_limit,
            max_prompt_chars=resolved.prompt_max_chars,
        )

    pre_compiled = _compile(candidates)

    async def _filter_in_tx(
        session: AsyncSession, items: Sequence[MemoryRecallItem]
    ) -> list[MemoryRecallItem]:
        if not items:
            return []
        return await memories_service.filter_injectable_memories_in_session(
            session,
            user_id=prepared.owner_id,
            provider_id=provider_id or items[0].provider_id,
            conversation_id=prepared.conversation_id,
            candidates=items,
            captured_epoch=captured_epoch,
            tombstone_lookup=resolved.tombstone_lookup,
        )

    persisted, _ = await generation_store.persist_recalled_memories_phase(
        session_factory,
        prepared=prepared,
        candidates=pre_compiled.injected_memories,
        filter_in_session=_filter_in_tx if pre_compiled.injected_memories else None,
    )
    final = _compile(persisted) if persisted != pre_compiled.injected_memories else pre_compiled
    return PreparedContext(
        prepared=prepared,
        compiled_messages=final.messages,
        recalled_memories=final.injected_memories,
        memory_recall_degraded=degraded,
        memory_degradation_reason=degradation_reason,
    )


async def complete_prepared_sync_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    timeout_seconds: float | None = None,
    max_history_messages: int | None = None,
    lease_ttl_seconds: float | None = None,
    lease_renew_interval_seconds: float | None = None,
    before_commit_hook: BeforeCommitHook | None = None,
) -> CommittedChatResult:
    """Run context preparation, external model completion, and atomic Tx 2 commit/convergence."""
    base = options if options is not None else ChatOptions()
    effective = replace(
        base,
        timeout_seconds=timeout_seconds if timeout_seconds is not None else base.timeout_seconds,
        history_limit=(
            max_history_messages if max_history_messages is not None else base.history_limit
        ),
        lease_ttl_seconds=(
            lease_ttl_seconds if lease_ttl_seconds is not None else base.lease_ttl_seconds
        ),
        lease_renew_interval_seconds=(
            lease_renew_interval_seconds
            if lease_renew_interval_seconds is not None
            else base.lease_renew_interval_seconds
        ),
        before_commit_hook=(
            before_commit_hook if before_commit_hook is not None else base.before_commit_hook
        ),
    )

    async def _prepare() -> PreparedContext:
        return await prepare_context(
            session_factory,
            prepared=prepared,
            memory_provider=memory_provider,
            options=effective,
            max_history_messages=effective.history_limit,
        )

    async def _run_sync(context: PreparedContext) -> ModelResponse:
        return await generation_execution.execute_model_completion(
            model_provider=model_provider,
            context=context,
            timeout_seconds=effective.timeout_seconds,
        )

    committed, _, error = await generation_execution.run_prepared_generation_lifecycle(
        session_factory,
        prepared=prepared,
        memory_provider=memory_provider,
        lease_ttl_seconds=effective.lease_ttl_seconds,
        lease_renew_interval_seconds=effective.lease_renew_interval_seconds,
        memory_write_max_attempts=effective.memory_write_max_attempts,
        idempotency_retention_seconds=effective.idempotency_retention_seconds,
        before_commit_hook=effective.before_commit_hook,
        prepare_context_fn=_prepare,
        run_model=_run_sync,
        overall_deadline_seconds=effective.timeout_seconds,
        mode="sync",
    )
    if error is not None:
        raise error
    assert committed is not None
    return committed


async def complete_prepared_stream_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    on_delta: DeltaCallback,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
) -> StreamGenerationOutcome:
    """Run streaming generation via the shared lifecycle, emitting text deltas via callback."""
    resolved = options if options is not None else ChatOptions()
    effective_timeout = (
        resolved.stream_timeout_seconds
        if resolved.stream_timeout_seconds is not None
        else resolved.timeout_seconds
    )
    loop = asyncio.get_running_loop()
    deadline_mono = loop.time() + effective_timeout if effective_timeout is not None else None

    async def _prepare() -> PreparedContext:
        return await prepare_context(
            session_factory,
            prepared=prepared,
            memory_provider=memory_provider,
            options=resolved,
            max_history_messages=resolved.history_limit,
        )

    async def _run_stream(context: PreparedContext) -> ModelResponse:
        remaining = max(0.001, deadline_mono - loop.time()) if deadline_mono is not None else None
        return await generation_execution.execute_model_stream(
            model_provider=model_provider,
            context=context,
            on_delta=on_delta,
            timeout_seconds=remaining,
        )

    committed, failure, _ = await generation_execution.run_prepared_generation_lifecycle(
        session_factory,
        prepared=prepared,
        memory_provider=memory_provider,
        lease_ttl_seconds=resolved.lease_ttl_seconds,
        lease_renew_interval_seconds=resolved.lease_renew_interval_seconds,
        memory_write_max_attempts=resolved.memory_write_max_attempts,
        idempotency_retention_seconds=resolved.idempotency_retention_seconds,
        before_commit_hook=resolved.before_commit_hook,
        prepare_context_fn=_prepare,
        run_model=_run_stream,
        overall_deadline_seconds=effective_timeout,
        mode="stream",
    )
    if committed is not None:
        return committed
    assert failure is not None
    return failure


async def cancel_stream_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    options: ChatOptions | None = None,
) -> GenerationRecord | None:
    """Converge a disconnected streaming generation to `cancelled` unless already terminal."""
    resolved = options if options is not None else ChatOptions()
    converged = await generation_store.converge_generation_failure_safely(
        session_factory,
        prepared=prepared,
        target_status="cancelled",
        error_code="generation_cancelled",
        error_message="client disconnected before generation completed",
        error_status_code=409,
        idempotency_retention_seconds=resolved.idempotency_retention_seconds,
    )
    if converged is not None and converged.status == "cancelled":
        current_telemetry().record_generation_outcome(
            mode="stream",
            outcome="cancelled",
            error_code="generation_cancelled",
        )
    return converged


async def execute_prepared_sync_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    on_finished: Callable[[], None] | None = None,
) -> CommittedChatResult:
    """Execute an already-prepared sync generation shielded from client HTTP disconnects."""
    resolved = options if options is not None else ChatOptions()

    async def _run() -> CommittedChatResult:
        try:
            return await complete_prepared_sync_generation(
                session_factory,
                model_provider=model_provider,
                memory_provider=memory_provider,
                prepared=prepared,
                options=resolved,
            )
        finally:
            if on_finished is not None:
                on_finished()

    coro = _run()
    if resolved.task_registry is not None:
        task = resolved.task_registry.spawn(coro)
    else:
        task = asyncio.create_task(coro)
        task.add_done_callback(consume_detached_task_exception)
    return await asyncio.shield(task)


async def prepare_chat_turn(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    idempotency_key: str,
    requested_model_id: str | None = None,
    stream: bool = False,
    allow_streaming: bool = True,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    now: datetime | None = None,
    on_generation_permit: Callable[[ConcurrencyPermit], None] | None = None,
) -> PreparedGeneration | ChatTurnOutcome:
    """Shared idempotent chat preparation entry point for both sync and SSE execution."""
    resolved = options if options is not None else ChatOptions()
    _validate_message_length(user_message, resolved.max_message_chars)
    spec = idempotency_store.build_chat_idempotency_spec(
        raw_key=idempotency_key,
        agent_id=agent_id,
        conversation_id=conversation_id,
        message=user_message,
        requested_model_id=requested_model_id,
        retention_seconds=resolved.idempotency_retention_seconds,
    )

    async def _resolve_replay() -> ChatTurnOutcome | None:
        return await idempotency_store.resolve_chat_idempotency(
            session_factory,
            owner_id=owner_id,
            spec=spec,
            load_detail_in_session=conversation_queries.load_generation_detail_in_session,
            now=now,
        )

    replayed = await _resolve_replay()
    if replayed is not None:
        return replayed
    if stream and not allow_streaming:
        raise UnprocessableEntityError(
            code="streaming_not_enabled",
            message="streaming responses are not enabled on this endpoint yet",
        )

    permit = _acquire_generation_permit(resolved)

    effective = (
        replace(resolved, require_streaming=True)
        if stream and not resolved.require_streaming
        else resolved
    )
    permit_transferred = False
    try:
        for _ in range(2):
            try:
                prepared = await prepare_generation(
                    session_factory,
                    persona_provider=persona_provider,
                    memory_provider=memory_provider,
                    owner_id=owner_id,
                    agent_id=agent_id,
                    conversation_id=conversation_id,
                    user_message=user_message,
                    requested_model_id=requested_model_id,
                    options=effective,
                    idempotency_spec=spec,
                    now=now,
                )
                if permit is not None and on_generation_permit is not None:
                    on_generation_permit(permit)
                    permit_transferred = True
                return prepared
            except (IdempotencyRecordHitError, IdempotencyScopeRaceError):
                replay_after_race = await _resolve_replay()
                if replay_after_race is not None:
                    return replay_after_race

        raise ServiceUnavailableError(
            code="idempotency_resolution_failed",
            message="failed to resolve concurrent idempotent chat request",
        )
    finally:
        if permit is not None and not permit_transferred:
            permit.release()


@overload
async def run_sync_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    model_provider: ModelProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    requested_model_id: str | None = None,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    idempotency_key: None = None,
    stream: bool = False,
    now: datetime | None = None,
) -> CommittedChatResult: ...


@overload
async def run_sync_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    model_provider: ModelProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    idempotency_key: str,
    requested_model_id: str | None = None,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    stream: bool = False,
    now: datetime | None = None,
) -> ChatTurnOutcome: ...


async def run_sync_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    model_provider: ModelProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    requested_model_id: str | None = None,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
    idempotency_key: str | None = None,
    stream: bool = False,
    now: datetime | None = None,
) -> ChatTurnOutcome:
    """Execute or idempotently replay a synchronous chat turn shielded from HTTP disconnects."""
    resolved = options if options is not None else ChatOptions()
    acquired_permit: list[ConcurrencyPermit] = []
    if idempotency_key is not None:
        prepared_or_replay = await prepare_chat_turn(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=owner_id,
            agent_id=agent_id,
            conversation_id=conversation_id,
            user_message=user_message,
            idempotency_key=idempotency_key,
            requested_model_id=requested_model_id,
            stream=stream,
            allow_streaming=False,
            options=resolved,
            now=now,
            on_generation_permit=acquired_permit.append,
        )
        if not isinstance(prepared_or_replay, PreparedGeneration):
            if acquired_permit:
                acquired_permit[0].release()
            return prepared_or_replay
        prepared = prepared_or_replay
    else:
        permit_no_idem = _acquire_generation_permit(resolved)
        try:
            prepared = await prepare_generation(
                session_factory,
                persona_provider=persona_provider,
                memory_provider=memory_provider,
                owner_id=owner_id,
                agent_id=agent_id,
                conversation_id=conversation_id,
                user_message=user_message,
                requested_model_id=requested_model_id,
                options=resolved,
                now=now,
            )
        except BaseException:
            if permit_no_idem is not None:
                permit_no_idem.release()
            raise
        if permit_no_idem is not None:
            acquired_permit.append(permit_no_idem)

    permit = acquired_permit[0] if acquired_permit else None
    return await execute_prepared_sync_chat(
        session_factory,
        model_provider=model_provider,
        memory_provider=memory_provider,
        prepared=prepared,
        options=resolved,
        on_finished=permit.release if permit is not None else None,
    )
