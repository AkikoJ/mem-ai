"""Unit tests for chat idempotency key normalization, semantic digest, expiry, and replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from mem_ai.core.exceptions import AppError, UnprocessableEntityError
from mem_ai.modules.conversations.idempotency import (
    CHAT_CANONICAL_ROUTE,
    CHAT_HTTP_METHOD,
    DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    build_chat_idempotency_spec,
    build_replay_error,
    compute_chat_request_digest,
    infer_error_http_status,
    is_record_expired,
    normalize_idempotency_key,
    resolve_replay_from_detail,
)
from mem_ai.modules.conversations.schemas import (
    ChatAcceptedResult,
    CommittedChatResult,
    GenerationDetail,
    GenerationRecord,
    GenerationStatus,
    IdempotencyRecordDTO,
    MessageRecord,
    generation_status_url,
)


def _sample_generation(
    *,
    status: GenerationStatus = "running",
    error_code: str | None = None,
    error_message: str | None = None,
    assistant_message_id: UUID | None = None,
) -> GenerationRecord:
    now = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
    return GenerationRecord(
        id=uuid4(),
        conversation_id=uuid4(),
        owner_id=uuid4(),
        agent_id=uuid4(),
        persona_version_id=uuid4(),
        user_message_id=uuid4(),
        assistant_message_id=assistant_message_id,
        model_id="gpt-4o-mini",
        memory_epoch=0,
        status=status,
        error_code=error_code,
        error_message=error_message,
        input_tokens=10 if status == "succeeded" else None,
        output_tokens=5 if status == "succeeded" else None,
        total_tokens=15 if status == "succeeded" else None,
        finish_reason="stop" if status == "succeeded" else None,
        lease_token="lease_1" if status == "running" else None,
        lease_expires_at=(now + timedelta(seconds=60)) if status == "running" else None,
        started_at=now,
        completed_at=None if status == "running" else now + timedelta(seconds=2),
        created_at=now,
        updated_at=now,
    )


def test_normalize_idempotency_key_accepts_valid_and_rejects_blank_oversized_or_control() -> None:
    assert normalize_idempotency_key("  chat-req-001  ") == "chat-req-001"
    assert normalize_idempotency_key("k" * 255) == "k" * 255

    for invalid in ("", "   ", "k" * 256, "bad\nkey", "bad\rkey", "bad\x00key"):
        with pytest.raises(UnprocessableEntityError) as exc_info:
            normalize_idempotency_key(invalid)
        assert exc_info.value.status_code == 422
        assert exc_info.value.code == "invalid_idempotency_key"


def test_chat_request_digest_includes_business_params_and_excludes_stream() -> None:
    agent_id = uuid4()
    conv_id = uuid4()

    d_implicit = compute_chat_request_digest(
        agent_id=agent_id,
        conversation_id=None,
        message="  hello world  ",
        requested_model_id=None,
    )
    d_implicit_same = compute_chat_request_digest(
        agent_id=agent_id,
        conversation_id=None,
        message="hello world",
        requested_model_id=None,
    )
    assert d_implicit == d_implicit_same
    assert len(d_implicit) == 64

    # Conversation omission vs explicit conversationId changes the digest.
    d_with_conv = compute_chat_request_digest(
        agent_id=agent_id,
        conversation_id=conv_id,
        message="hello world",
        requested_model_id=None,
    )
    assert d_with_conv != d_implicit

    # Different agent, message, or requested model changes the digest.
    assert (
        compute_chat_request_digest(
            agent_id=uuid4(),
            conversation_id=None,
            message="hello world",
            requested_model_id=None,
        )
        != d_implicit
    )
    assert (
        compute_chat_request_digest(
            agent_id=agent_id,
            conversation_id=None,
            message="different message",
            requested_model_id=None,
        )
        != d_implicit
    )
    assert (
        compute_chat_request_digest(
            agent_id=agent_id,
            conversation_id=None,
            message="hello world",
            requested_model_id="gpt-4o",
        )
        != d_implicit
    )

    # `build_chat_idempotency_spec` fixes canonical method/route and does not depend on stream.
    spec = build_chat_idempotency_spec(
        raw_key=" req-1 ",
        agent_id=agent_id,
        conversation_id=None,
        message="hello world",
        requested_model_id=" gpt-4o-mini ",
    )
    assert spec.method == CHAT_HTTP_METHOD
    assert spec.canonical_route == CHAT_CANONICAL_ROUTE
    assert spec.key == "req-1"
    assert spec.retention_seconds == DEFAULT_IDEMPOTENCY_RETENTION_SECONDS
    assert spec.request_digest == compute_chat_request_digest(
        agent_id=agent_id,
        conversation_id=None,
        message="hello world",
        requested_model_id="gpt-4o-mini",
    )


def test_active_record_never_expires_and_terminal_expires_at_exact_24h_boundary() -> None:
    t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
    terminal_at = t0 + timedelta(seconds=5)
    expires_at = terminal_at + timedelta(hours=24)

    active = IdempotencyRecordDTO(
        id=uuid4(),
        user_id=uuid4(),
        method="POST",
        canonical_route="/v1/chat",
        idempotency_key="k-active",
        request_digest="a" * 64,
        conversation_id=uuid4(),
        generation_id=uuid4(),
        status="running",
        terminal_at=None,
        expires_at=None,
        created_at=t0,
        updated_at=t0,
    )
    assert is_record_expired(active, current_time=t0 + timedelta(days=30)) is False

    terminal = active.model_copy(
        update={
            "idempotency_key": "k-done",
            "status": "succeeded",
            "response_status_code": 200,
            "terminal_at": terminal_at,
            "expires_at": expires_at,
        }
    )
    assert is_record_expired(terminal, current_time=expires_at - timedelta(seconds=1)) is False
    assert is_record_expired(terminal, current_time=expires_at) is True
    assert is_record_expired(terminal, current_time=expires_at + timedelta(seconds=1)) is True


def test_resolve_replay_from_detail_covers_running_succeeded_failed_and_cancelled() -> None:
    # 1. running -> ChatAcceptedResult (202)
    running_gen = _sample_generation(status="running")
    running_outcome = resolve_replay_from_detail(
        GenerationDetail(generation=running_gen, assistant_message=None),
        response_status_code=None,
    )
    assert isinstance(running_outcome, ChatAcceptedResult)
    assert running_outcome.conversation_id == running_gen.conversation_id
    assert running_outcome.generation_id == running_gen.id
    assert running_outcome.user_message_id == running_gen.user_message_id
    assert running_outcome.status == "running"
    assert running_outcome.status_url == generation_status_url(
        running_gen.conversation_id, running_gen.id
    )

    # 2. succeeded -> CommittedChatResult (200 JSON)
    asst_id = uuid4()
    succ_gen = _sample_generation(status="succeeded", assistant_message_id=asst_id)
    asst_msg = MessageRecord(
        id=asst_id,
        conversation_id=succ_gen.conversation_id,
        generation_id=succ_gen.id,
        role="assistant",
        content="saved reply",
        sequence=2,
        created_at=succ_gen.created_at,
    )
    succ_outcome = resolve_replay_from_detail(
        GenerationDetail(generation=succ_gen, assistant_message=asst_msg),
        response_status_code=200,
    )
    assert isinstance(succ_outcome, CommittedChatResult)
    assert succ_outcome.assistant_message.content == "saved reply"
    assert succ_outcome.model_id == "gpt-4o-mini"
    assert succ_outcome.usage.total_tokens == 15

    # 3. failed -> original HTTP error status with generation ID and terminal status in details
    failed_gen = _sample_generation(
        status="failed",
        error_code="generation_timeout",
        error_message="model generation timed out",
    )
    with pytest.raises(AppError) as failed_exc:
        resolve_replay_from_detail(
            GenerationDetail(generation=failed_gen, assistant_message=None),
            response_status_code=504,
        )
    assert failed_exc.value.status_code == 504
    assert failed_exc.value.code == "generation_timeout"
    assert failed_exc.value.details["generation_id"] == str(failed_gen.id)
    assert failed_exc.value.details["conversation_id"] == str(failed_gen.conversation_id)
    assert failed_exc.value.details["status"] == "failed"

    # 4. cancelled -> always 409 generation_cancelled even if error_code differed
    cancelled_gen = _sample_generation(
        status="cancelled",
        error_code="some_other_code",
        error_message="client disconnected",
    )
    err = build_replay_error(
        GenerationDetail(generation=cancelled_gen, assistant_message=None),
        response_status_code=500,
    )
    assert err.status_code == 409
    assert err.code == "generation_cancelled"
    assert err.details["status"] == "cancelled"
    assert err.details["generation_id"] == str(cancelled_gen.id)

    # 5. fallback HTTP status inference
    assert infer_error_http_status("generation_lease_lost", generation_status="failed") == 409
    assert infer_error_http_status("generation_timeout", generation_status="failed") == 504
    assert infer_error_http_status("model_upstream_error", generation_status="failed") == 502
    assert infer_error_http_status("generation_commit_failed", generation_status="failed") == 503
    assert infer_error_http_status("user_disabled", generation_status="failed") == 403
    assert infer_error_http_status(None, generation_status="cancelled") == 409
