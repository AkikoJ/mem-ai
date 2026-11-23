"""Unit tests for SSE event encoding and parsing against Engineering Design §8.4."""

from __future__ import annotations

from uuid import UUID

import pytest

from mem_ai.contracts.model_provider import ModelUsage
from mem_ai.modules.conversations.stream import (
    SSE_RESPONSE_HEADERS,
    format_conversation_completed,
    format_conversation_failed,
    format_conversation_started,
    format_message_delta,
    format_sse_heartbeat,
)
from tests.conftest import ParsedSseEvent, parse_sse_text


def _parse_single_sse_frame(frame: str) -> ParsedSseEvent:
    assert frame.endswith("\n\n")
    events = parse_sse_text(frame)
    assert len(events) == 1
    return events[0]


def test_sse_response_headers_disable_proxy_buffering() -> None:
    assert SSE_RESPONSE_HEADERS["Cache-Control"] == "no-cache, no-transform"
    assert SSE_RESPONSE_HEADERS["X-Accel-Buffering"] == "no"


def test_sse_business_events_follow_snake_case_schema_and_ids() -> None:
    conv_id = UUID("11111111-1111-4111-8111-111111111111")
    gen_id = UUID("22222222-2222-4222-8222-222222222222")
    user_msg_id = UUID("33333333-3333-4333-8333-333333333333")
    assistant_msg_id = UUID("44444444-4444-4444-8444-444444444444")

    started = _parse_single_sse_frame(
        format_conversation_started(
            event_id=1,
            conversation_id=conv_id,
            generation_id=gen_id,
            user_message_id=user_msg_id,
        )
    )
    assert started.event_id == "1"
    assert started.event == "conversation.started"
    assert started.data == {
        "conversation_id": str(conv_id),
        "generation_id": str(gen_id),
        "message_id": str(user_msg_id),
    }

    delta = _parse_single_sse_frame(
        format_message_delta(
            event_id=2,
            generation_id=gen_id,
            sequence=1,
            text="hello\nworld",
        )
    )
    assert delta.event_id == "2"
    assert delta.event == "message.delta"
    assert delta.data == {
        "generation_id": str(gen_id),
        "sequence": 1,
        "text": "hello\nworld",
    }

    completed = _parse_single_sse_frame(
        format_conversation_completed(
            event_id=3,
            generation_id=gen_id,
            assistant_message_id=assistant_msg_id,
            usage=ModelUsage(input_tokens=12, output_tokens=5, total_tokens=17),
        )
    )
    assert completed.event_id == "3"
    assert completed.event == "conversation.completed"
    assert completed.data == {
        "generation_id": str(gen_id),
        "message_id": str(assistant_msg_id),
        "usage": {
            "input_tokens": 12,
            "output_tokens": 5,
            "total_tokens": 17,
        },
    }

    failed = _parse_single_sse_frame(
        format_conversation_failed(
            event_id=4,
            generation_id=gen_id,
            code="model_upstream_error",
            message="model provider returned an error response",
        )
    )
    assert failed.event_id == "4"
    assert failed.event == "conversation.failed"
    assert failed.data == {
        "generation_id": str(gen_id),
        "code": "model_upstream_error",
        "message": "model provider returned an error response",
        "partial_output_persisted": False,
    }


def test_sse_heartbeat_has_no_event_id_and_empty_json_object() -> None:
    heartbeat = _parse_single_sse_frame(format_sse_heartbeat())
    assert heartbeat.event_id is None
    assert heartbeat.event == "heartbeat"
    assert heartbeat.data == {}


def test_sse_event_id_and_delta_sequence_must_be_positive() -> None:
    gen_id = UUID("22222222-2222-4222-8222-222222222222")
    with pytest.raises(ValueError, match="event_id"):
        format_message_delta(event_id=0, generation_id=gen_id, sequence=1, text="x")
    with pytest.raises(ValueError, match="sequence"):
        format_message_delta(event_id=1, generation_id=gen_id, sequence=0, text="x")
