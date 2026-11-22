"""Conversation Core orchestration for synchronous and future streaming chat flows.

Implements the orchestration half of Engineering Design §5.4 (Shared Generation Lifecycle):
- `prepare_generation`: validates user/conversation, resolves read-only `PersonaSnapshot`
  strictly outside DB write transactions, resolves server-registered model, then commits Tx 1.
- `prepare_context`: pure prompt compilation seam (Ticket 08 extends with scoped memory recall).
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
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, TypeVar
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryRecallItem
from mem_ai.contracts.model_provider import ModelProvider, ModelRequest, ModelResponse
from mem_ai.contracts.persona_provider import PersonaProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    GatewayTimeoutError,
    NotFoundError,
    ServiceUnavailableError,
)
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations.generation import BeforeCommitHook
from mem_ai.modules.conversations.prompt import compile_chat_prompt
from mem_ai.modules.conversations.schemas import (
    CommittedChatResult,
    PreparedContext,
    PreparedGeneration,
)
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
    """Bundled runtime budgets, model routing policy, and hooks for chat execution."""

    model_policy: ModelRoutingPolicy | None = None
    user_permissions: frozenset[str] = frozenset()
    require_streaming: bool = False
    required_capabilities: frozenset[str] = frozenset()
    history_limit: int = 20
    lease_ttl_seconds: float = 60.0
    lease_renew_interval_seconds: float | None = 20.0
    timeout_seconds: float | None = None
    task_registry: SyncGenerationTaskRegistry | None = None
    before_commit_hook: BeforeCommitHook | None = None


def sync_chat_options_from_settings(
    settings: Settings,
    *,
    user_permissions: frozenset[str] = frozenset(),
    require_streaming: bool = False,
    task_registry: SyncGenerationTaskRegistry | None = None,
) -> SyncChatOptions:
    """Construct `SyncChatOptions` with the shared server `ModelRoutingPolicy`."""
    return SyncChatOptions(
        model_policy=models_service.model_routing_policy_from_settings(settings),
        user_permissions=user_permissions,
        require_streaming=require_streaming,
        history_limit=settings.chat_history_message_limit,
        lease_ttl_seconds=float(settings.chat_lease_ttl_seconds),
        lease_renew_interval_seconds=settings.chat_lease_renew_interval_seconds,
        timeout_seconds=settings.chat_generation_timeout_seconds,
        task_registry=task_registry,
    )


class GenerationLeaseHeartbeat:
    """Periodic lease renewal context manager shared by sync and streaming generation execution."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        prepared: PreparedGeneration,
        lease_ttl_seconds: float,
        renew_interval_seconds: float | None,
        on_lease_lost: Callable[[AppError], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._prepared = prepared
        self._lease_ttl_seconds = lease_ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._on_lease_lost = on_lease_lost
        self._task: asyncio.Task[None] | None = None
        self.loss_error: AppError | None = None

    async def __aenter__(self) -> GenerationLeaseHeartbeat:
        if self._renew_interval_seconds is not None and self._renew_interval_seconds > 0:
            self._task = asyncio.create_task(self._run_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)
        await self.stop()

    async def stop(self) -> None:
        """Stop the background renewal loop cleanly before Tx 2 or failure convergence."""
        task = self._task
        self._task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run_loop(self) -> None:
        assert self._renew_interval_seconds is not None
        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                await generation_store.renew_generation_lease_phase(
                    self._session_factory,
                    owner_id=self._prepared.owner_id,
                    conversation_id=self._prepared.conversation_id,
                    generation_id=self._prepared.generation_id,
                    lease_token=self._prepared.lease_token,
                    lease_ttl_seconds=self._lease_ttl_seconds,
                )
            except AppError as exc:
                self.loss_error = exc
                if self._on_lease_lost is not None:
                    self._on_lease_lost(exc)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Transient error renewing generation lease; retrying before expiry",
                    exc_info=True,
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


class DatabaseConversationAccessChecker:
    """Real `ConversationAccessChecker` backed by `conversations.generation`."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def ensure_conversation_accessible(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> None:
        await generation_store.ensure_conversation_accessible(
            self._session_factory,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )


async def prepare_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    persona_provider: PersonaProvider,
    owner_id: UUID,
    agent_id: UUID,
    conversation_id: UUID | None,
    user_message: str,
    requested_model_id: str | None = None,
    options: SyncChatOptions | None = None,
) -> PreparedGeneration:
    """Resolve persona outside DB write tx, validate model, and commit Tx 1 preparation."""
    resolved_options = options if options is not None else SyncChatOptions()
    await users_service.get_active_user(session_factory, user_id=owner_id)

    # Read existing conversation (if any) in a short read session before calling PersonaProvider.
    if conversation_id is None:
        persona = await persona_provider.get_latest_published(
            owner_id=owner_id,
            agent_id=agent_id,
        )
        if persona is None:
            raise NotFoundError(
                code="agent_persona_not_found",
                message="published agent persona not found",
            )
    else:
        existing_conversation = await generation_store.load_conversation_for_continuation(
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
        memory_epoch=0,
        history_limit=resolved_options.history_limit,
        lease_ttl_seconds=resolved_options.lease_ttl_seconds,
    )


def prepare_context(
    *,
    prepared: PreparedGeneration,
    recalled_memories: Sequence[MemoryRecallItem] = (),
    max_history_messages: int | None = 20,
) -> PreparedContext:
    """Compile pure domain data into ordered model prompt messages (extended in Ticket 08)."""
    compiled = compile_chat_prompt(
        persona=prepared.persona,
        history_messages=prepared.history_messages,
        current_input=prepared.user_message_content,
        recalled_memories=recalled_memories,
        max_history_messages=max_history_messages,
    )
    return PreparedContext(
        prepared=prepared,
        compiled_messages=tuple(compiled),
        recalled_memories=tuple(recalled_memories),
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
        )
    except Exception:
        logger.exception(
            "Failed to converge generation to failed state",
            extra={
                "extra_fields": {
                    "error_code": error_code,
                }
            },
        )


async def complete_prepared_sync_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    timeout_seconds: float | None = None,
    max_history_messages: int | None = 20,
    lease_ttl_seconds: float = 60.0,
    lease_renew_interval_seconds: float | None = 20.0,
    before_commit_hook: BeforeCommitHook | None = None,
) -> CommittedChatResult:
    """Run context compilation, external model completion, and atomic Tx 2 commit/convergence."""
    log_tokens = bind_log_context(
        conversation_id=str(prepared.conversation_id),
        generation_id=str(prepared.generation_id),
    )
    model_task: asyncio.Task[ModelResponse] | None = None

    def _abort_model_on_lease_lost(_error: AppError) -> None:
        if model_task is not None and not model_task.done():
            model_task.cancel()

    try:
        try:
            async with GenerationLeaseHeartbeat(
                session_factory=session_factory,
                prepared=prepared,
                lease_ttl_seconds=lease_ttl_seconds,
                renew_interval_seconds=lease_renew_interval_seconds,
                on_lease_lost=_abort_model_on_lease_lost,
            ) as heartbeat:
                context = prepare_context(
                    prepared=prepared,
                    max_history_messages=max_history_messages,
                )
                if heartbeat.loss_error is not None:
                    raise heartbeat.loss_error

                model_task = asyncio.create_task(
                    execute_model_completion(
                        model_provider=model_provider,
                        context=context,
                        timeout_seconds=timeout_seconds,
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
            await _safe_converge_failure(
                session_factory,
                prepared=prepared,
                error_code=exc.code,
                error_message=exc.message,
            )
            raise
        except Exception:
            logger.exception(
                "Unexpected exception during context preparation or model completion",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            await _safe_converge_failure(
                session_factory,
                prepared=prepared,
                error_code="model_upstream_error",
                error_message="model generation failed",
            )
            raise BadGatewayError(
                code="model_upstream_error",
                message="model generation failed",
            ) from None

        try:
            return await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared,
                assistant_content=model_response.content,
                usage=model_response.usage,
                finish_reason=model_response.finish_reason,
                before_commit_hook=before_commit_hook,
            )
        except AppError as exc:
            await _safe_converge_failure(
                session_factory,
                prepared=prepared,
                error_code=exc.code,
                error_message=exc.message,
            )
            raise
        except Exception:
            logger.exception(
                "Failed to commit completed generation atomically",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            await _safe_converge_failure(
                session_factory,
                prepared=prepared,
                error_code="generation_commit_failed",
                error_message="failed to commit generation result",
            )
            raise ServiceUnavailableError(
                code="generation_commit_failed",
                message="failed to commit generation result",
            ) from None
    finally:
        reset_log_context(log_tokens)


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
    options: SyncChatOptions | None = None,
) -> CommittedChatResult:
    """Execute a full synchronous chat turn shielded from client HTTP disconnects."""
    resolved_options = options if options is not None else SyncChatOptions()
    prepared = await prepare_generation(
        session_factory,
        persona_provider=persona_provider,
        owner_id=owner_id,
        agent_id=agent_id,
        conversation_id=conversation_id,
        user_message=user_message,
        requested_model_id=requested_model_id,
        options=resolved_options,
    )

    coro = complete_prepared_sync_generation(
        session_factory,
        model_provider=model_provider,
        prepared=prepared,
        timeout_seconds=resolved_options.timeout_seconds,
        max_history_messages=resolved_options.history_limit,
        lease_ttl_seconds=resolved_options.lease_ttl_seconds,
        lease_renew_interval_seconds=resolved_options.lease_renew_interval_seconds,
        before_commit_hook=resolved_options.before_commit_hook,
    )
    if resolved_options.task_registry is not None:
        task = resolved_options.task_registry.spawn(coro)
    else:
        task = asyncio.create_task(coro)
        task.add_done_callback(_consume_detached_task_exception)

    return await asyncio.shield(task)
