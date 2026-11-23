"""Conversation Core orchestration for synchronous and future streaming chat flows.

Implements the orchestration half of Engineering Design §5.4 (Shared Generation Lifecycle):
- `prepare_generation`: validates user/conversation, resolves read-only `PersonaSnapshot`
  strictly outside DB write transactions, resolves server-registered model, then commits Tx 1.
- `prepare_context`: recalls scoped memories outside DB transactions via `memories.service`,
  verifies lease and epoch/tombstone guards in short Tx 1.5 while persisting metadata-only
  `message_recalled_memories` rows, and compiles budgeted prompt messages.
- `execute_model_completion`: invokes `ModelProvider.generate` outside any DB transaction under
  an overall generation deadline.
- `complete_prepared_sync_generation`: executes context -> model -> atomic Tx 2 or failure
  convergence; wrapped in a shielded task by `run_sync_chat` so client HTTP disconnects do not
  abort an already-started synchronous model call (ADR 0004).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Coroutine, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar, overload
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider, MemoryRecallItem
from mem_ai.contracts.model_provider import ModelProvider, ModelRequest, ModelResponse
from mem_ai.contracts.persona_provider import PersonaProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    GatewayTimeoutError,
    NotFoundError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import idempotency as idempotency_store
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations.generation import (
    BeforeCommitHook,
    GenerationLeaseHeartbeat,
    IdempotencyRecordHitError,
    IdempotencyScopeRaceError,
)
from mem_ai.modules.conversations.prompt import compile_chat_context
from mem_ai.modules.conversations.schemas import (
    ChatTurnOutcome,
    CommittedChatResult,
    GenerationRecord,
    IdempotencyKeySpec,
    MessageRecord,
    PreparedContext,
    PreparedGeneration,
)
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.service import MemoryRecallPolicy, TombstoneLookup
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelRoutingPolicy
from mem_ai.modules.users import service as users_service

logger = logging.getLogger("mem_ai.conversations")

_ResultT = TypeVar("_ResultT")


def _consume_detached_task_exception(done_task: asyncio.Task[Any]) -> None:
    if not done_task.cancelled():
        with contextlib.suppress(Exception):
            done_task.exception()


@dataclass(frozen=True, slots=True)
class SyncChatOptions:
    """Bundled runtime budgets, model routing policy, memory policy, and hooks for chat."""

    model_policy: ModelRoutingPolicy | None = None
    user_permissions: frozenset[str] = frozenset()
    require_streaming: bool = False
    required_capabilities: frozenset[str] = frozenset()
    history_limit: int = 20
    lease_ttl_seconds: float = 60.0
    lease_renew_interval_seconds: float | None = 20.0
    timeout_seconds: float | None = None
    memory_recall_timeout_seconds: float | None = 5.0
    memory_min_relevance: float = 0.5
    memory_max_items: int = 10
    memory_context_max_chars: int = 2000
    prompt_max_chars: int | None = 24000
    memory_write_max_attempts: int = 5
    idempotency_retention_seconds: float = idempotency_store.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS
    tombstone_lookup: TombstoneLookup | None = None
    task_registry: SyncGenerationTaskRegistry | None = None
    before_commit_hook: BeforeCommitHook | None = None


def sync_chat_options_from_settings(
    settings: Settings,
    *,
    user_permissions: frozenset[str] = frozenset(),
    require_streaming: bool = False,
    task_registry: SyncGenerationTaskRegistry | None = None,
) -> SyncChatOptions:
    """Construct `SyncChatOptions` with the shared server `ModelRoutingPolicy` and budgets."""
    return SyncChatOptions(
        model_policy=models_service.model_routing_policy_from_settings(settings),
        user_permissions=user_permissions,
        require_streaming=require_streaming,
        history_limit=settings.chat_history_message_limit,
        lease_ttl_seconds=float(settings.chat_lease_ttl_seconds),
        lease_renew_interval_seconds=settings.chat_lease_renew_interval_seconds,
        timeout_seconds=settings.chat_generation_timeout_seconds,
        memory_recall_timeout_seconds=settings.chat_memory_recall_timeout_seconds,
        memory_min_relevance=settings.chat_memory_min_relevance,
        memory_max_items=settings.chat_memory_max_items,
        memory_context_max_chars=settings.chat_memory_context_max_chars,
        prompt_max_chars=settings.chat_prompt_max_chars,
        memory_write_max_attempts=settings.worker_max_attempts,
        idempotency_retention_seconds=float(settings.idempotency_retention_hours * 3600),
        task_registry=task_registry,
    )


class SyncGenerationTaskRegistry:
    """Tracks in-flight shielded sync generation tasks for graceful shutdown draining."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    def spawn(self, coro: Coroutine[Any, Any, _ResultT]) -> asyncio.Task[_ResultT]:
        task: asyncio.Task[_ResultT] = asyncio.create_task(coro)
        self._tasks.add(task)

        def _on_done(done_task: asyncio.Task[_ResultT]) -> None:
            self._tasks.discard(done_task)
            _consume_detached_task_exception(done_task)

        task.add_done_callback(_on_done)
        return task

    async def drain(self, *, timeout_seconds: float = 10.0) -> None:
        """Wait bounded time for in-flight shielded sync tasks to finish on app shutdown."""
        if not self._tasks:
            return
        pending = list(self._tasks)
        _, still_pending = await asyncio.wait(pending, timeout=timeout_seconds)
        for task in still_pending:
            task.cancel()
        if still_pending:
            await asyncio.gather(*still_pending, return_exceptions=True)


def build_turn_memory_enqueue_hook(
    *,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None,
    max_attempts: int = 5,
    extra_hook: BeforeCommitHook | None = None,
) -> BeforeCommitHook | None:
    """Build the Tx 2 `before_commit_hook` shared by sync and streaming generation commits."""
    if memory_provider is None and extra_hook is None:
        return None

    async def _before_commit(
        session: AsyncSession,
        generation_record: GenerationRecord,
        assistant_record: MessageRecord,
    ) -> None:
        if memory_provider is not None:
            await memories_service.enqueue_turn_memory_write_in_session(
                session,
                provider_id=memory_provider.provider_id,
                owner_id=prepared.owner_id,
                conversation_id=prepared.conversation_id,
                generation_id=generation_record.id,
                user_message_id=prepared.user_message_id,
                assistant_message_id=assistant_record.id,
                memory_epoch=prepared.memory_epoch,
                memory_policy=prepared.persona.memory_policy,
                max_attempts=max_attempts,
            )
        if extra_hook is not None:
            await extra_hook(session, generation_record, assistant_record)

    return _before_commit


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
    options: SyncChatOptions | None = None,
    idempotency_spec: IdempotencyKeySpec | None = None,
    now: datetime | None = None,
) -> PreparedGeneration:
    """Resolve persona outside DB write tx, validate model, and commit Tx 1 preparation."""
    resolved_options = options if options is not None else SyncChatOptions()
    await users_service.get_active_user(session_factory, user_id=owner_id)

    # Read existing conversation (if any) in a short read session before calling PersonaProvider.
    if conversation_id is None:
        persona = await persona_provider.get_latest_published(owner_id=owner_id, agent_id=agent_id)
        if persona is None:
            raise NotFoundError(
                code="agent_persona_not_found",
                message="published agent persona not found",
            )
    else:
        existing_conversation = await conversation_queries.load_conversation_for_continuation(
            session_factory,
            owner_id=owner_id,
            conversation_id=conversation_id,
            expected_agent_id=agent_id,
        )
        persona = await persona_provider.get_version(
            owner_id=owner_id,
            agent_id=agent_id,
            version_id=existing_conversation.persona_version_id,
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
        user_permissions=resolved_options.user_permissions,
        require_streaming=resolved_options.require_streaming,
        required_capabilities=resolved_options.required_capabilities,
        policy=resolved_options.model_policy,
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
        history_limit=resolved_options.history_limit,
        lease_ttl_seconds=resolved_options.lease_ttl_seconds,
        idempotency_spec=idempotency_spec,
        idempotency_retention_seconds=resolved_options.idempotency_retention_seconds,
        now=now,
    )


async def prepare_context(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: SyncChatOptions | None = None,
    recalled_memories: Sequence[MemoryRecallItem] | None = None,
    max_history_messages: int | None = None,
) -> PreparedContext:
    """Recall scoped memories outside DB tx, verify lease/guard, save refs, and compile prompt."""
    resolved_options = options if options is not None else SyncChatOptions()
    effective_history_limit = (
        max_history_messages if max_history_messages is not None else resolved_options.history_limit
    )

    degraded = False
    degradation_reason: str | None = None
    captured_epoch = prepared.memory_epoch
    provider_id = memory_provider.provider_id if memory_provider is not None else None

    if recalled_memories is not None:
        candidate_memories: tuple[MemoryRecallItem, ...] = tuple(recalled_memories)
    elif memory_provider is not None:
        persona_policy = prepared.persona.memory_policy
        effective_limit = (
            min(persona_policy.recall_limit, resolved_options.memory_max_items)
            if persona_policy.enabled
            else 0
        )
        recall_policy = MemoryRecallPolicy(
            enabled=persona_policy.enabled,
            recall_limit=effective_limit,
            min_relevance=resolved_options.memory_min_relevance,
            max_memory_chars=resolved_options.memory_context_max_chars,
            timeout_seconds=resolved_options.memory_recall_timeout_seconds,
        )
        outcome = await memories_service.recall_relevant_memories(
            session_factory,
            memory_provider=memory_provider,
            user_id=prepared.owner_id,
            conversation_id=prepared.conversation_id,
            query=prepared.user_message_content,
            policy=recall_policy,
            tombstone_lookup=resolved_options.tombstone_lookup,
        )
        candidate_memories = outcome.items
        degraded = outcome.degraded
        degradation_reason = outcome.degradation_reason
        captured_epoch = max(captured_epoch, outcome.captured_epoch)
    else:
        candidate_memories = ()

    # Pre-fit candidates against the total prompt budget so only actually injectable items
    # are persisted into `message_recalled_memories`.
    pre_compiled = compile_chat_context(
        persona=prepared.persona,
        history_messages=prepared.history_messages,
        current_input=prepared.user_message_content,
        recalled_memories=candidate_memories,
        max_history_messages=effective_history_limit,
        max_prompt_chars=resolved_options.prompt_max_chars,
    )

    async def _filter_in_tx(
        session: AsyncSession,
        items: Sequence[MemoryRecallItem],
    ) -> list[MemoryRecallItem]:
        if not items:
            return []
        effective_provider_id = provider_id or items[0].provider_id
        return await memories_service.filter_injectable_memories_in_session(
            session,
            user_id=prepared.owner_id,
            provider_id=effective_provider_id,
            conversation_id=prepared.conversation_id,
            candidates=items,
            captured_epoch=captured_epoch,
            tombstone_lookup=resolved_options.tombstone_lookup,
        )

    persisted_memories, _ = await generation_store.persist_recalled_memories_phase(
        session_factory,
        prepared=prepared,
        candidates=pre_compiled.injected_memories,
        filter_in_session=_filter_in_tx if pre_compiled.injected_memories else None,
    )

    if persisted_memories != pre_compiled.injected_memories:
        final_compiled = compile_chat_context(
            persona=prepared.persona,
            history_messages=prepared.history_messages,
            current_input=prepared.user_message_content,
            recalled_memories=persisted_memories,
            max_history_messages=effective_history_limit,
            max_prompt_chars=resolved_options.prompt_max_chars,
        )
    else:
        final_compiled = pre_compiled

    return PreparedContext(
        prepared=prepared,
        compiled_messages=final_compiled.messages,
        recalled_memories=final_compiled.injected_memories,
        memory_recall_degraded=degraded,
        memory_degradation_reason=degradation_reason,
    )


async def execute_model_completion(
    *,
    model_provider: ModelProvider,
    context: PreparedContext,
    timeout_seconds: float | None = None,
) -> ModelResponse:
    """Invoke `ModelProvider.generate` outside any DB transaction under an overall deadline."""
    request = ModelRequest(
        model_id=context.prepared.model_id,
        upstream_model=context.prepared.upstream_model,
        messages=list(context.compiled_messages),
        max_output_tokens=context.prepared.max_output_tokens,
        timeout_seconds=timeout_seconds,
    )
    if timeout_seconds is None:
        return await model_provider.generate(request)

    try:
        return await asyncio.wait_for(
            model_provider.generate(request),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        raise GatewayTimeoutError(
            code="generation_timeout",
            message="model generation timed out",
        ) from None


async def _safe_converge_failure(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    error_code: str,
    error_message: str,
    error_status_code: int | None = None,
    idempotency_retention_seconds: float = idempotency_store.DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
) -> None:
    if error_code == "generation_lease_lost":
        return
    try:
        await generation_store.converge_generation_failure_phase(
            session_factory,
            generation_id=prepared.generation_id,
            owner_id=prepared.owner_id,
            conversation_id=prepared.conversation_id,
            lease_token=prepared.lease_token,
            target_status="failed",
            error_code=error_code,
            error_message=error_message,
            error_status_code=error_status_code,
            idempotency_retention_seconds=idempotency_retention_seconds,
        )
    except Exception:
        logger.exception(
            "Failed to converge generation to failed state",
            extra={"extra_fields": {"error_code": error_code}},
        )


async def complete_prepared_sync_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: SyncChatOptions | None = None,
    timeout_seconds: float | None = None,
    max_history_messages: int | None = None,
    lease_ttl_seconds: float | None = None,
    lease_renew_interval_seconds: float | None = None,
    before_commit_hook: BeforeCommitHook | None = None,
) -> CommittedChatResult:
    """Run context preparation, external model completion, and atomic Tx 2 commit/convergence."""
    resolved_options = options if options is not None else SyncChatOptions()
    effective_timeout = (
        timeout_seconds if timeout_seconds is not None else resolved_options.timeout_seconds
    )
    effective_history = (
        max_history_messages if max_history_messages is not None else resolved_options.history_limit
    )
    effective_lease_ttl = (
        lease_ttl_seconds if lease_ttl_seconds is not None else resolved_options.lease_ttl_seconds
    )
    effective_renew_interval = (
        lease_renew_interval_seconds
        if lease_renew_interval_seconds is not None
        else resolved_options.lease_renew_interval_seconds
    )
    effective_before_commit = (
        before_commit_hook
        if before_commit_hook is not None
        else resolved_options.before_commit_hook
    )

    log_tokens = bind_log_context(
        conversation_id=str(prepared.conversation_id),
        generation_id=str(prepared.generation_id),
    )
    model_task: asyncio.Task[ModelResponse] | None = None

    def _abort_model_on_lease_lost(_error: AppError) -> None:
        if model_task is not None and not model_task.done():
            model_task.cancel()

    async def _converge(error_code: str, error_message: str, status_code: int) -> None:
        await _safe_converge_failure(
            session_factory,
            prepared=prepared,
            error_code=error_code,
            error_message=error_message,
            error_status_code=status_code,
            idempotency_retention_seconds=resolved_options.idempotency_retention_seconds,
        )

    try:
        try:
            async with GenerationLeaseHeartbeat(
                session_factory=session_factory,
                prepared=prepared,
                lease_ttl_seconds=effective_lease_ttl,
                renew_interval_seconds=effective_renew_interval,
                on_lease_lost=_abort_model_on_lease_lost,
            ) as heartbeat:
                try:
                    context = await prepare_context(
                        session_factory,
                        prepared=prepared,
                        memory_provider=memory_provider,
                        options=resolved_options,
                        max_history_messages=effective_history,
                    )
                except AppError:
                    raise
                except Exception:
                    logger.exception(
                        "Unexpected exception during context preparation",
                        extra={"extra_fields": {"model_id": prepared.model_id}},
                    )
                    raise ServiceUnavailableError(
                        code="generation_context_failed",
                        message="failed to prepare generation context",
                    ) from None

                if heartbeat.loss_error is not None:
                    raise heartbeat.loss_error

                model_task = asyncio.create_task(
                    execute_model_completion(
                        model_provider=model_provider,
                        context=context,
                        timeout_seconds=effective_timeout,
                    )
                )
                try:
                    model_response = await model_task
                except asyncio.CancelledError:
                    if heartbeat.loss_error is not None:
                        raise heartbeat.loss_error from None
                    raise
                finally:
                    if not model_task.done():
                        model_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await model_task

                if heartbeat.loss_error is not None:
                    raise heartbeat.loss_error
        except AppError as exc:
            await _converge(exc.code, exc.message, exc.status_code)
            raise
        except Exception:
            logger.exception(
                "Unexpected exception during model completion",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            await _converge("model_upstream_error", "model generation failed", 502)
            raise BadGatewayError(
                code="model_upstream_error", message="model generation failed"
            ) from None

        tx2_before_commit = build_turn_memory_enqueue_hook(
            prepared=prepared,
            memory_provider=memory_provider,
            max_attempts=resolved_options.memory_write_max_attempts,
            extra_hook=effective_before_commit,
        )
        try:
            return await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared,
                assistant_content=model_response.content,
                usage=model_response.usage,
                finish_reason=model_response.finish_reason,
                before_commit_hook=tx2_before_commit,
                idempotency_retention_seconds=resolved_options.idempotency_retention_seconds,
            )
        except AppError as exc:
            await _converge(exc.code, exc.message, exc.status_code)
            raise
        except Exception:
            logger.exception(
                "Failed to commit completed generation atomically",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            await _converge("generation_commit_failed", "failed to commit generation result", 503)
            raise ServiceUnavailableError(
                code="generation_commit_failed",
                message="failed to commit generation result",
            ) from None
    finally:
        reset_log_context(log_tokens)


async def _execute_shielded_sync_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None,
    options: SyncChatOptions,
) -> CommittedChatResult:
    coro = complete_prepared_sync_generation(
        session_factory,
        model_provider=model_provider,
        memory_provider=memory_provider,
        prepared=prepared,
        options=options,
    )
    if options.task_registry is not None:
        task = options.task_registry.spawn(coro)
    else:
        task = asyncio.create_task(coro)
        task.add_done_callback(_consume_detached_task_exception)
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
    allow_streaming: bool = False,
    memory_provider: MemoryProvider | None = None,
    options: SyncChatOptions | None = None,
    now: datetime | None = None,
) -> PreparedGeneration | ChatTurnOutcome:
    """Shared idempotent chat preparation entry point for sync and future SSE execution.

    Resolves any existing live idempotency record BEFORE stream gating, persona/model lookups,
    or conversation lease contention. Concurrent same-key races resolve to the winning record.
    """
    resolved_options = options if options is not None else SyncChatOptions()
    spec = idempotency_store.build_chat_idempotency_spec(
        raw_key=idempotency_key,
        agent_id=agent_id,
        conversation_id=conversation_id,
        message=user_message,
        requested_model_id=requested_model_id,
        retention_seconds=resolved_options.idempotency_retention_seconds,
    )

    replayed = await idempotency_store.resolve_chat_idempotency(
        session_factory,
        owner_id=owner_id,
        spec=spec,
        load_detail_in_session=conversation_queries.load_generation_detail_in_session,
        now=now,
    )
    if replayed is not None:
        return replayed

    if stream and not allow_streaming:
        raise UnprocessableEntityError(
            code="streaming_not_enabled",
            message="streaming responses are not enabled on this endpoint yet",
        )

    for _ in range(2):
        try:
            return await prepare_generation(
                session_factory,
                persona_provider=persona_provider,
                memory_provider=memory_provider,
                owner_id=owner_id,
                agent_id=agent_id,
                conversation_id=conversation_id,
                user_message=user_message,
                requested_model_id=requested_model_id,
                options=resolved_options,
                idempotency_spec=spec,
                now=now,
            )
        except (IdempotencyRecordHitError, IdempotencyScopeRaceError):
            resolved = await idempotency_store.resolve_chat_idempotency(
                session_factory,
                owner_id=owner_id,
                spec=spec,
                load_detail_in_session=conversation_queries.load_generation_detail_in_session,
                now=now,
            )
            if resolved is not None:
                return resolved

    raise ServiceUnavailableError(
        code="idempotency_resolution_failed",
        message="failed to resolve concurrent idempotent chat request",
    )


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
    options: SyncChatOptions | None = None,
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
    options: SyncChatOptions | None = None,
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
    options: SyncChatOptions | None = None,
    idempotency_key: str | None = None,
    stream: bool = False,
    now: datetime | None = None,
) -> ChatTurnOutcome:
    """Execute or idempotently replay a synchronous chat turn shielded from HTTP disconnects."""
    resolved_options = options if options is not None else SyncChatOptions()
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
            options=resolved_options,
            now=now,
        )
        if not isinstance(prepared_or_replay, PreparedGeneration):
            return prepared_or_replay
        prepared = prepared_or_replay
    else:
        prepared = await prepare_generation(
            session_factory,
            persona_provider=persona_provider,
            memory_provider=memory_provider,
            owner_id=owner_id,
            agent_id=agent_id,
            conversation_id=conversation_id,
            user_message=user_message,
            requested_model_id=requested_model_id,
            options=resolved_options,
            now=now,
        )

    return await _execute_shielded_sync_generation(
        session_factory,
        model_provider=model_provider,
        memory_provider=memory_provider,
        prepared=prepared,
        options=resolved_options,
    )
