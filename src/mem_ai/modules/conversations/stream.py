"""SSE wire encoding, heartbeat multiplexing, and disconnect transport bridge.

Implements the transport layer of Engineering Design §5.4 and §8.4:
- Pure SSE wire-format encoders for `conversation.started`, `message.delta`,
  `conversation.completed`, `conversation.failed`, and `heartbeat`.
- `SseStreamingResponse` ensuring shielded async-generator closure on client disconnect.
- `stream_prepared_generation` delegating generation execution, lease renewal, Tx 2 commit,
  and convergence exclusively to `conversations.service`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import anyio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.responses import StreamingResponse
from starlette.types import Send

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.contracts.model_provider import ModelProvider, ModelUsage
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.schemas import (
    CommittedChatResult,
    PreparedGeneration,
    StreamGenerationFailure,
)
from mem_ai.modules.conversations.service import ChatOptions

SSE_MEDIA_TYPE = "text/event-stream"
SSE_RESPONSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache, no-transform",
    "X-Accel-Buffering": "no",
}
_PRODUCER_CANCEL_GRACE_SECONDS = 2.0
_CANCEL_CONVERGENCE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class _DeltaQueueEvent:
    text: str


@dataclass(frozen=True, slots=True)
class _CompletedQueueEvent:
    result: CommittedChatResult


@dataclass(frozen=True, slots=True)
class _FailedQueueEvent:
    code: str
    message: str


_StreamQueueItem = _DeltaQueueEvent | _CompletedQueueEvent | _FailedQueueEvent


def _consume_task_exception(done_task: asyncio.Task[Any]) -> None:
    if not done_task.cancelled():
        with contextlib.suppress(Exception):
            done_task.exception()


def format_sse_event(
    *,
    event: str,
    data: Mapping[str, Any],
    event_id: int | None = None,
) -> str:
    """Encode a single SSE frame with optional positive numeric business event ID."""
    if event_id is not None and event_id < 1:
        raise ValueError("event_id must be a positive integer")
    encoded_data = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    if event_id is None:
        return f"event: {event}\ndata: {encoded_data}\n\n"
    return f"id: {event_id}\nevent: {event}\ndata: {encoded_data}\n\n"


def format_conversation_started(
    *,
    event_id: int,
    conversation_id: UUID,
    generation_id: UUID,
    user_message_id: UUID,
) -> str:
    """Format the `conversation.started` SSE event emitted after Tx 1 commits."""
    return format_sse_event(
        event="conversation.started",
        event_id=event_id,
        data={
            "conversation_id": str(conversation_id),
            "generation_id": str(generation_id),
            "message_id": str(user_message_id),
        },
    )


def format_message_delta(
    *,
    event_id: int,
    generation_id: UUID,
    sequence: int,
    text: str,
) -> str:
    """Format a normalized `message.delta` SSE event with monotonic 1-based sequence."""
    if sequence < 1:
        raise ValueError("sequence must be a positive integer")
    return format_sse_event(
        event="message.delta",
        event_id=event_id,
        data={
            "generation_id": str(generation_id),
            "sequence": sequence,
            "text": text,
        },
    )


def format_conversation_completed(
    *,
    event_id: int,
    generation_id: UUID,
    assistant_message_id: UUID,
    usage: ModelUsage,
) -> str:
    """Format `conversation.completed` emitted strictly after atomic Tx 2 commit."""
    return format_sse_event(
        event="conversation.completed",
        event_id=event_id,
        data={
            "generation_id": str(generation_id),
            "message_id": str(assistant_message_id),
            "usage": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
            },
        },
    )


def format_conversation_failed(
    *,
    event_id: int,
    generation_id: UUID,
    code: str,
    message: str,
) -> str:
    """Format `conversation.failed` emitted after DB convergence; partial output is never saved."""
    return format_sse_event(
        event="conversation.failed",
        event_id=event_id,
        data={
            "generation_id": str(generation_id),
            "code": code,
            "message": message,
            "partial_output_persisted": False,
        },
    )


def format_sse_heartbeat() -> str:
    """Format a keep-alive `heartbeat` SSE event without business ID or state change."""
    return format_sse_event(event="heartbeat", data={}, event_id=None)


class SseStreamingResponse(StreamingResponse):
    """StreamingResponse ensuring shielded async-generator closure on client disconnect."""

    async def stream_response(self, send: Send) -> None:
        try:
            await super().stream_response(send)
        finally:
            aclose = getattr(self.body_iterator, "aclose", None)
            if callable(aclose):
                with anyio.CancelScope(shield=True):
                    await aclose()


async def stream_prepared_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
) -> AsyncIterator[str]:
    """Stream SSE frames for an already-prepared generation via `conversations.service`."""
    resolved_options = options if options is not None else ChatOptions()
    heartbeat_interval = resolved_options.stream_heartbeat_interval_seconds
    if heartbeat_interval <= 0:
        raise ValueError("stream_heartbeat_interval_seconds must be positive")

    event_queue: asyncio.Queue[_StreamQueueItem | None] = asyncio.Queue()
    terminal_state: Literal["running", "succeeded", "failed", "cancelled"] = "running"
    producer_task: asyncio.Task[None] | None = None

    def _on_delta(text: str) -> None:
        event_queue.put_nowait(_DeltaQueueEvent(text=text))

    async def _produce() -> None:
        nonlocal terminal_state
        try:
            outcome = await conversations_service.complete_prepared_stream_generation(
                session_factory,
                model_provider=model_provider,
                prepared=prepared,
                on_delta=_on_delta,
                memory_provider=memory_provider,
                options=resolved_options,
            )
            if isinstance(outcome, CommittedChatResult):
                terminal_state = "succeeded"
                event_queue.put_nowait(_CompletedQueueEvent(result=outcome))
            elif isinstance(outcome, StreamGenerationFailure):
                terminal_state = "failed"
                if outcome.converged:
                    event_queue.put_nowait(
                        _FailedQueueEvent(code=outcome.code, message=outcome.message)
                    )
        finally:
            event_queue.put_nowait(None)

    event_id = 1
    delta_sequence = 0
    try:
        producer_task = asyncio.create_task(_produce())
        producer_task.add_done_callback(_consume_task_exception)

        # Tx 1 has already committed conversation_id, generation_id, and user_message_id.
        yield format_conversation_started(
            event_id=event_id,
            conversation_id=prepared.conversation_id,
            generation_id=prepared.generation_id,
            user_message_id=prepared.user_message_id,
        )

        while True:
            try:
                async with asyncio.timeout(heartbeat_interval):
                    item = await event_queue.get()
            except TimeoutError:
                yield format_sse_heartbeat()
                continue

            if item is None:
                break
            if isinstance(item, _DeltaQueueEvent):
                event_id += 1
                delta_sequence += 1
                yield format_message_delta(
                    event_id=event_id,
                    generation_id=prepared.generation_id,
                    sequence=delta_sequence,
                    text=item.text,
                )
            elif isinstance(item, _CompletedQueueEvent):
                event_id += 1
                yield format_conversation_completed(
                    event_id=event_id,
                    generation_id=prepared.generation_id,
                    assistant_message_id=item.result.assistant_message.id,
                    usage=item.result.usage,
                )
                break
            elif isinstance(item, _FailedQueueEvent):
                event_id += 1
                yield format_conversation_failed(
                    event_id=event_id,
                    generation_id=prepared.generation_id,
                    code=item.code,
                    message=item.message,
                )
                break
    finally:
        with anyio.CancelScope(shield=True):
            if producer_task is not None and not producer_task.done():
                producer_task.cancel()
                with contextlib.suppress(TimeoutError, asyncio.CancelledError, Exception):
                    async with asyncio.timeout(_PRODUCER_CANCEL_GRACE_SECONDS):
                        await producer_task
            if terminal_state == "running":
                terminal_state = "cancelled"
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(_CANCEL_CONVERGENCE_TIMEOUT_SECONDS):
                        await conversations_service.cancel_stream_generation(
                            session_factory,
                            prepared=prepared,
                            options=resolved_options,
                        )


def build_chat_sse_response(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    model_provider: ModelProvider,
    prepared: PreparedGeneration,
    memory_provider: MemoryProvider | None = None,
    options: ChatOptions | None = None,
) -> SseStreamingResponse:
    """Create the `text/event-stream` response wired with proxy headers and shielded cleanup."""
    return SseStreamingResponse(
        stream_prepared_generation(
            session_factory,
            model_provider=model_provider,
            prepared=prepared,
            memory_provider=memory_provider,
            options=options,
        ),
        status_code=200,
        media_type=SSE_MEDIA_TYPE,
        headers=SSE_RESPONSE_HEADERS,
    )
