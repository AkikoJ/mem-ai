"""Shared lease-guarded generation execution engine for sync and streaming chat flows.

Owns the single execution pipeline invoked by `conversations/service.py`:
- `SyncGenerationTaskRegistry`: tracks shielded sync generation tasks for graceful shutdown.
- `build_turn_memory_enqueue_hook`: builds the Tx 2 `before_commit_hook` enqueuing turn memory jobs.
- `execute_model_completion` / `execute_model_stream`: invokes `ModelProvider` outside DB tx.
- `run_prepared_generation_lifecycle`: runs `GenerationLeaseHeartbeat`, context preparation,
  model execution, atomic Tx 2 commit (`commit_generation_success_phase`), and DB failure
  convergence (`converge_generation_failure_safely`) via one shared code path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar

import anyio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.contracts.model_provider import (
    ModelProvider,
    ModelRequest,
    ModelResponse,
    ModelUsage,
)
from mem_ai.core.exceptions import (
    AppError,
    BadGatewayError,
    GatewayTimeoutError,
    ServiceUnavailableError,
)
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations.generation import (
    BeforeCommitHook,
    GenerationLeaseHeartbeat,
)
from mem_ai.modules.conversations.schemas import (
    CommittedChatResult,
    GenerationRecord,
    MessageRecord,
    PreparedContext,
    PreparedGeneration,
    StreamGenerationFailure,
)
from mem_ai.modules.memories import service as memories_service

logger = logging.getLogger("mem_ai.conversations.execution")

_ResultT = TypeVar("_ResultT")
DeltaCallback = Callable[[str], None]
ContextPreparer = Callable[[], Awaitable[PreparedContext]]
ModelRunner = Callable[[PreparedContext], Awaitable[ModelResponse]]


def consume_detached_task_exception(done_task: asyncio.Task[Any]) -> None:
    """Suppress unretrieved-exception warnings on intentionally detached background tasks."""
    if not done_task.cancelled():
        with contextlib.suppress(Exception):
            done_task.exception()


class SyncGenerationTaskRegistry:
    """Tracks in-flight shielded sync generation tasks for graceful shutdown draining."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    def spawn(self, coro: Coroutine[Any, Any, _ResultT]) -> asyncio.Task[_ResultT]:
        task: asyncio.Task[_ResultT] = asyncio.create_task(coro)
        self._tasks.add(task)

        def _on_done(done_task: asyncio.Task[_ResultT]) -> None:
            self._tasks.discard(done_task)
            consume_detached_task_exception(done_task)

        task.add_done_callback(_on_done)
        return task

    async def drain(self, *, timeout_seconds: float = 10.0) -> None:
        """Wait bounded time for in-flight shielded sync tasks to finish on app shutdown."""
        if not self._tasks:
            return
        _, still_pending = await asyncio.wait(list(self._tasks), timeout=timeout_seconds)
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
        return await asyncio.wait_for(model_provider.generate(request), timeout=timeout_seconds)
    except TimeoutError:
        raise GatewayTimeoutError(
            code="generation_timeout",
            message="model generation timed out",
        ) from None


async def _close_stream_iterator(iterator: object) -> None:
    aclose = getattr(iterator, "aclose", None)
    if callable(aclose):
        with anyio.CancelScope(shield=True), contextlib.suppress(Exception):
            await aclose()


async def execute_model_stream(
    *,
    model_provider: ModelProvider,
    context: PreparedContext,
    on_delta: DeltaCallback,
    timeout_seconds: float | None = None,
) -> ModelResponse:
    """Invoke `ModelProvider.stream_generate`, forwarding non-empty text chunks to `on_delta`."""
    request = ModelRequest(
        model_id=context.prepared.model_id,
        upstream_model=context.prepared.upstream_model,
        messages=list(context.compiled_messages),
        max_output_tokens=context.prepared.max_output_tokens,
        timeout_seconds=timeout_seconds,
    )
    collected: list[str] = []
    final_usage = ModelUsage()
    final_finish_reason: str | None = None
    stream_iter = model_provider.stream_generate(request)
    try:
        async for chunk in stream_iter:
            if chunk.usage is not None:
                final_usage = chunk.usage
            if chunk.finish_reason is not None:
                final_finish_reason = chunk.finish_reason
            if chunk.text:
                collected.append(chunk.text)
                on_delta(chunk.text)
    finally:
        await _close_stream_iterator(stream_iter)
    return ModelResponse(
        model_id=context.prepared.model_id,
        content="".join(collected),
        usage=final_usage,
        finish_reason=final_finish_reason,
    )


async def run_prepared_generation_lifecycle(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None,
    lease_ttl_seconds: float,
    lease_renew_interval_seconds: float | None,
    memory_write_max_attempts: int,
    idempotency_retention_seconds: float,
    before_commit_hook: BeforeCommitHook | None,
    prepare_context_fn: ContextPreparer,
    run_model: ModelRunner,
    overall_deadline_seconds: float | None = None,
) -> tuple[CommittedChatResult | None, StreamGenerationFailure | None, AppError | None]:
    """Single shared generation execution & convergence pipeline for sync and streaming flows."""
    log_tokens = bind_log_context(
        conversation_id=str(prepared.conversation_id),
        generation_id=str(prepared.generation_id),
    )
    execution_task: asyncio.Task[ModelResponse] | None = None

    def _abort_on_lease_lost(_error: AppError) -> None:
        if execution_task is not None and not execution_task.done():
            execution_task.cancel()

    async def _fail(exc: AppError) -> tuple[None, StreamGenerationFailure, AppError]:
        converged = await generation_store.converge_generation_failure_safely(
            session_factory,
            prepared=prepared,
            target_status="failed",
            error_code=exc.code,
            error_message=exc.message,
            error_status_code=exc.status_code,
            idempotency_retention_seconds=idempotency_retention_seconds,
        )
        return (
            None,
            StreamGenerationFailure(
                code=exc.code,
                message=exc.message,
                converged=converged is not None,
            ),
            exc,
        )

    async def _prepare_and_invoke(heartbeat: GenerationLeaseHeartbeat) -> ModelResponse:
        timeout_ctx = (
            asyncio.timeout(overall_deadline_seconds)
            if overall_deadline_seconds is not None
            else contextlib.nullcontext()
        )
        try:
            async with timeout_ctx:
                try:
                    context = await prepare_context_fn()
                except (AppError, TimeoutError, asyncio.CancelledError):
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
                return await run_model(context)
        except TimeoutError:
            raise GatewayTimeoutError(
                code="generation_timeout",
                message="model generation timed out",
            ) from None

    try:
        try:
            async with GenerationLeaseHeartbeat(
                session_factory=session_factory,
                prepared=prepared,
                lease_ttl_seconds=lease_ttl_seconds,
                renew_interval_seconds=lease_renew_interval_seconds,
                on_lease_lost=_abort_on_lease_lost,
            ) as heartbeat:
                execution_task = asyncio.create_task(_prepare_and_invoke(heartbeat))
                try:
                    model_response = await execution_task
                except asyncio.CancelledError:
                    if heartbeat.loss_error is not None:
                        raise heartbeat.loss_error from None
                    raise
                finally:
                    if not execution_task.done():
                        execution_task.cancel()
                        with (
                            anyio.CancelScope(shield=True),
                            contextlib.suppress(asyncio.CancelledError, Exception),
                        ):
                            await execution_task

                if heartbeat.loss_error is not None:
                    raise heartbeat.loss_error
        except asyncio.CancelledError:
            raise
        except AppError as exc:
            return await _fail(exc)
        except Exception:
            logger.exception(
                "Unexpected exception during model execution",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            return await _fail(
                BadGatewayError(code="model_upstream_error", message="model generation failed")
            )

        tx2_hook = build_turn_memory_enqueue_hook(
            prepared=prepared,
            memory_provider=memory_provider,
            max_attempts=memory_write_max_attempts,
            extra_hook=before_commit_hook,
        )
        try:
            committed = await generation_store.commit_generation_success_phase(
                session_factory,
                prepared=prepared,
                assistant_content=model_response.content,
                usage=model_response.usage,
                finish_reason=model_response.finish_reason,
                before_commit_hook=tx2_hook,
                idempotency_retention_seconds=idempotency_retention_seconds,
            )
            return committed, None, None
        except asyncio.CancelledError:
            raise
        except AppError as exc:
            return await _fail(exc)
        except Exception:
            logger.exception(
                "Failed to commit completed generation atomically",
                extra={"extra_fields": {"model_id": prepared.model_id}},
            )
            return await _fail(
                ServiceUnavailableError(
                    code="generation_commit_failed",
                    message="failed to commit generation result",
                )
            )
    finally:
        reset_log_context(log_tokens)
