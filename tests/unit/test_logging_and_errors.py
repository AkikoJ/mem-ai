"""Unit tests for stable application error codes and JSON structured logging."""

from __future__ import annotations

import json
import logging

import pytest

from mem_ai.core.exceptions import (
    AppError,
    ConflictError,
    NotFoundError,
    PayloadTooLargeError,
    RateLimitError,
)
from mem_ai.core.logging import (
    JsonLogFormatter,
    bind_log_context,
    clear_log_context,
    reset_log_context,
)


def test_app_error_enforces_lowercase_snake_case_code() -> None:
    err = ConflictError(
        code="conversation_generation_conflict",
        message="conversation already has an active generation",
        details={"conversation_id": "conv_1"},
    )
    assert err.status_code == 409
    assert err.code == "conversation_generation_conflict"
    assert err.details == {"conversation_id": "conv_1"}

    with pytest.raises(ValueError, match="lowercase snake_case"):
        AppError(status_code=400, code="InvalidCode", message="bad code")


def test_not_found_error_defaults_to_404_not_found() -> None:
    err = NotFoundError()
    assert err.status_code == 404
    assert err.code == "not_found"


def test_json_formatter_includes_correlation_fields_and_redacts_secrets() -> None:
    clear_log_context()
    tokens = bind_log_context(
        request_id="req_abc123",
        user_id="usr_1",
        conversation_id="conv_1",
    )
    try:
        formatter = JsonLogFormatter()
        record = logging.LogRecord(
            name="mem_ai.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="request completed",
            args=(),
            exc_info=None,
        )
        record.extra_fields = {
            "authorization": "Bearer secret-token",
            "api_key": "sk-12345",
            "deletion_receipt": "rcpt_secret",
            "memory_value": "user private memory text",
            "content": "raw message text",
            "content_type": "application/json",
            "content_length": 128,
            "nested": {"password": "pw", "safe": "ok"},
            "status_code": 200,
        }

        payload = json.loads(formatter.format(record))
        assert payload["level"] == "INFO"
        assert payload["message"] == "request completed"
        assert payload["request_id"] == "req_abc123"
        assert payload["user_id"] == "usr_1"
        assert payload["conversation_id"] == "conv_1"
        assert payload["status_code"] == 200
        assert payload["authorization"] == "[REDACTED]"
        assert payload["api_key"] == "[REDACTED]"
        assert payload["deletion_receipt"] == "[REDACTED]"
        assert payload["memory_value"] == "[REDACTED]"
        assert payload["content"] == "[REDACTED]"
        assert payload["content_type"] == "application/json"
        assert payload["content_length"] == 128
        assert payload["nested"] == {"password": "[REDACTED]", "safe": "ok"}
    finally:
        reset_log_context(tokens)


def test_json_formatter_redacts_inline_bearer_receipts_and_preserves_token_counts() -> None:
    clear_log_context()
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="mem_ai.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="auth failed for Bearer tok_secret_123 and DeletionReceipt rcpt_secret_456",
        args=(),
        exc_info=None,
    )
    record.extra_fields = {
        "user_message": "my secret diary entry",
        "assistant_message": "assistant reply text",
        "user_message_id": "msg_1",
        "input_tokens": 12,
        "output_tokens": 25,
        "total_tokens": 37,
        "lease_token": "genlease_secret",
        "header_sample": "Authorization: Bearer another_secret_token",
    }

    payload = json.loads(formatter.format(record))
    assert "tok_secret_123" not in payload["message"]
    assert "rcpt_secret_456" not in payload["message"]
    assert payload["user_message"] == "[REDACTED]"
    assert payload["assistant_message"] == "[REDACTED]"
    assert payload["user_message_id"] == "msg_1"
    assert payload["input_tokens"] == 12
    assert payload["output_tokens"] == 25
    assert payload["total_tokens"] == 37
    assert payload["lease_token"] == "[REDACTED]"
    assert "another_secret_token" not in payload["header_sample"]


def test_rate_limit_and_payload_too_large_errors_expose_stable_headers_and_details() -> None:
    rate_err = RateLimitError(
        code="request_concurrency_limit_exceeded",
        message="too many concurrent requests",
        retry_after_seconds=4.2,
    )
    assert rate_err.status_code == 429
    assert rate_err.code == "request_concurrency_limit_exceeded"
    assert rate_err.headers["Retry-After"] == "5"
    assert rate_err.details["retry_after_seconds"] == 5

    large_err = PayloadTooLargeError(max_bytes=1024)
    assert large_err.status_code == 413
    assert large_err.code == "payload_too_large"
    assert large_err.details["max_request_body_bytes"] == 1024
