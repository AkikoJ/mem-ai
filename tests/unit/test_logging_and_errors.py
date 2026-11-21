"""Unit tests for stable application error codes and JSON structured logging."""

from __future__ import annotations

import json
import logging

import pytest

from mem_ai.core.exceptions import AppError, ConflictError, NotFoundError
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
