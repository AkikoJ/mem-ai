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
import time
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
from mem_ai.core.tasks import drain_tasks_bounded
from mem_ai.core.telemetry import (
    GenerationMode,
    GenerationOutcome,
    ModelOutcome,
    current_telemetry,
    current_tracer,
)
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
        """Drain sync tasks, then bound cancellation cleanup within `timeout_seconds`."""
        cancellation_pending = await drain_tasks_bounded(
            list(self._tasks),
            timeout_seconds=timeout_seconds,
        )
        if cancellation_pending:
            logger.warning(
                "Synchronous generation tasks remained active after cancellation grace",
                extra={"extra_fields": {"active_task_count": len(cancellation_pending)}},
            )


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


def _model_provider_name(model_provider: ModelProvider) -> str:
    raw = getattr(model_provider, "provider_id", None)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return type(model_provider).__name__


async def _run_observed_model_call(
    *,
    model_provider: ModelProvider,
    model_id: str,
    mode: GenerationMode,
    span_name: str,
    timeout_seconds: float | None,
    invoke: Callable[[float], Awaitable[ModelResponse]],
) -> ModelResponse:
    """Shared span, provider log binding, deadline timeout, and latency metrics wrapper."""
    telemetry = current_telemetry()
    tracer = current_tracer()
    provider_name = _model_provider_name(model_provider)
    started = time.perf_counter()
    outcome: ModelOutcome = "error"
    log_tokens = bind_log_context(provider=provider_name)
    try:
        with tracer.start_span(
            span_name,
            attributes={"model_id": model_id, "mode": mode, "provider": provider_name},
        ) as span:
            try:
                timeout_ctx = (
                    asyncio.timeout(timeout_seconds)
                    if timeout_seconds is not None
                    else contextlib.nullcontext()
                )
                async with timeout_ctx:
                    response = await invoke(started)
                outcome = "succeeded"
                span.set_status("ok")
                return response
            except TimeoutError:
                outcome = "timeout"
                span.set_status("error", "generation_timeout")
                raise GatewayTimeoutError(
                    code="generation_timeout",
                    message="model generation timed out",
                ) from None
            except asyncio.CancelledError:
                elapsed = max(0.0, time.perf_counter() - started)
                if timeout_seconds is not None and elapsed >= max(0.0, timeout_seconds - 0.05):
                    outcome = "timeout"
                    span.set_status("error", "generation_timeout")
                else:
                    outcome = "cancelled"
                    span.set_status("error", "cancelled")
                raise
            except BaseException as exc:
                span.record_exception(exc)
                span.set_status("error", getattr(exc, "code", type(exc).__name__))
                raise
            finally:
                telemetry.record_model_request(
                    model_id=model_id,
                    mode=mode,
                    outcome=outcome,
                    duration_seconds=max(0.0, time.perf_counter() - started),
                )
    finally:
        reset_log_context(log_tokens)


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

    async def _invoke(_started: float) -> ModelResponse:
        return await model_provider.generate(request)

    return await _run_observed_model_call(
        model_provider=model_provider,
        model_id=request.model_id,
        mode="sync",
        span_name="model.generate",
        timeout_seconds=timeout_seconds,
        invoke=_invoke,
    )


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
    telemetry = current_telemetry()

    async def _invoke(started: float) -> ModelResponse:
        first_token_recorded = False
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
                    if not first_token_recorded:
                        first_token_recorded = True
                        telemetry.record_model_first_token(
                            model_id=request.model_id,
                            duration_seconds=max(0.0, time.perf_counter() - started),
                        )
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

    return await _run_observed_model_call(
        model_provider=model_provider,
        model_id=request.model_id,
        mode="stream",
        span_name="model.stream_generate",
        timeout_seconds=timeout_seconds,
        invoke=_invoke,
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
    mode: GenerationMode = "sync",
) -> tuple[CommittedChatResult | None, StreamGenerationFailure | None, AppError | None]:
    """Single shared generation execution & convergence pipeline for sync and streaming flows."""
    telemetry = current_telemetry()
    tracer = current_tracer()
    started = time.perf_counter()
    log_tokens = bind_log_context(
        conversation_id=str(prepared.conversation_id),
        generation_id=str(prepared.generation_id),
    )
    telemetry.inc_gauge(
        "mem_ai_generations_active",
        help_text="Current number of in-process active generations.",
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
        outcome_label: GenerationOutcome = (
            "timeout" if exc.code == "generation_timeout" else "failed"
        )
        telemetry.record_generation_outcome(mode=mode, outcome=outcome_label, error_code=exc.code)
        logger.warning(
            "Generation converged to failure",
            extra={
                "extra_fields": {
                    "mode": mode,
                    "model_id": prepared.model_id,
                    "error_code": exc.code,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                }
            },
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
        with tracer.start_span(
            "chat.generation",
            attributes={
                "mode": mode,
                "model_id": prepared.model_id,
                "conversation_id": str(prepared.conversation_id),
                "generation_id": str(prepared.generation_id),
            },
        ) as span:
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
                span.set_status("error", "cancelled")
                raise
            except AppError as exc:
                span.set_status("error", exc.code)
                return await _fail(exc)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status("error", "model_upstream_error")
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
                telemetry.record_generation_outcome(mode=mode, outcome="succeeded")
                span.set_status("ok")
                logger.info(
                    "Generation completed",
                    extra={
                        "extra_fields": {
                            "mode": mode,
                            "model_id": prepared.model_id,
                            "input_tokens": committed.usage.input_tokens,
                            "output_tokens": committed.usage.output_tokens,
                            "total_tokens": committed.usage.total_tokens,
                            "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                        }
                    },
                )
                return committed, None, None
            except asyncio.CancelledError:
                span.set_status("error", "cancelled")
                raise
            except AppError as exc:
                span.set_status("error", exc.code)
                return await _fail(exc)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status("error", "generation_commit_failed")
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
        telemetry.dec_gauge("mem_ai_generations_active")
        reset_log_context(log_tokens)
