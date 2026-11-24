"""Structured JSON logging with request/domain correlation and secret redaction."""

from __future__ import annotations

import json
import logging
import re
import sys
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any

_REQUEST_ID: ContextVar[str | None] = ContextVar("mem_ai_request_id", default=None)
_USER_ID: ContextVar[str | None] = ContextVar("mem_ai_user_id", default=None)
_CONVERSATION_ID: ContextVar[str | None] = ContextVar("mem_ai_conversation_id", default=None)
_GENERATION_ID: ContextVar[str | None] = ContextVar("mem_ai_generation_id", default=None)
_PROVIDER: ContextVar[str | None] = ContextVar("mem_ai_provider", default=None)
_JOB_ID: ContextVar[str | None] = ContextVar("mem_ai_job_id", default=None)

_CONTEXT_VARS: dict[str, ContextVar[str | None]] = {
    "request_id": _REQUEST_ID,
    "user_id": _USER_ID,
    "conversation_id": _CONVERSATION_ID,
    "generation_id": _GENERATION_ID,
    "provider": _PROVIDER,
    "job_id": _JOB_ID,
}

_SENSITIVE_KEY_MARKERS = (
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "receipt",
    "credential",
    "prompt",
    "message_body",
    "memory_value",
    "memory_content",
    "user_content",
    "assistant_content",
    "recall_query",
    "private_key",
    "signing_key",
    "gateway_auth",
)
_SENSITIVE_EXACT_KEYS = frozenset(
    {
        "content",
        "user_message",
        "assistant_message",
        "reply",
        "delta",
        "text",
        "body",
        "raw_body",
    }
)
_SAFE_EXACT_KEYS = frozenset(
    {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "max_output_tokens",
        "max_context_tokens",
        "user_message_id",
        "assistant_message_id",
    }
)
_INLINE_SECRET_PATTERN = re.compile(
    r"\b(Bearer|DeletionReceipt)\s+([A-Za-z0-9._~+/=-]{4,})\b",
    re.IGNORECASE,
)

REDACTED_VALUE = "[REDACTED]"


def get_request_id() -> str | None:
    """Return the current bound request ID, if any."""
    return _REQUEST_ID.get()


def bind_log_context(**fields: str | None) -> dict[str, Token[str | None]]:
    """Bind correlation fields into contextvars and return reset tokens."""
    tokens: dict[str, Token[str | None]] = {}
    for key, value in fields.items():
        var = _CONTEXT_VARS.get(key)
        if var is not None:
            tokens[key] = var.set(value)
    return tokens


def reset_log_context(tokens: dict[str, Token[str | None]]) -> None:
    """Restore previously bound correlation contextvars."""
    for key, token in tokens.items():
        var = _CONTEXT_VARS.get(key)
        if var is not None:
            var.reset(token)


def clear_log_context() -> None:
    """Reset all correlation contextvars to None."""
    for var in _CONTEXT_VARS.values():
        var.set(None)


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in _SAFE_EXACT_KEYS:
        return False
    if lowered in _SENSITIVE_EXACT_KEYS:
        return True
    return any(marker in lowered for marker in _SENSITIVE_KEY_MARKERS)


def redact_inline_secrets(text: str) -> str:
    """Redact inline `Bearer <token>` and `DeletionReceipt <receipt>` values from strings."""
    return _INLINE_SECRET_PATTERN.sub(lambda m: f"{m.group(1)} {REDACTED_VALUE}", text)


def sanitize_log_value(key: str | None, value: Any) -> Any:
    """Recursively redact sensitive keys and inline credentials in structured log payloads."""
    if key is not None and _is_sensitive_key(key):
        return REDACTED_VALUE
    if isinstance(value, str):
        return redact_inline_secrets(value)
    if isinstance(value, dict):
        return {str(k): sanitize_log_value(str(k), v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_log_value(None, item) for item in value]
    return value


class JsonLogFormatter(logging.Formatter):
    """Serialize log records as single-line JSON with correlation fields and redaction."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_inline_secrets(record.getMessage()),
        }

        for field_name, var in _CONTEXT_VARS.items():
            value = var.get()
            if value is not None:
                payload[field_name] = value

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            for key, value in extra_fields.items():
                payload[str(key)] = sanitize_log_value(str(key), value)

        if record.exc_info:
            exc_type = record.exc_info[0]
            payload["exception_type"] = exc_type.__name__ if exc_type is not None else "Exception"

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    """Configure the `mem_ai` logger hierarchy with JSON output on stdout."""
    logger = logging.getLogger("mem_ai")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    has_json_handler = any(
        isinstance(getattr(h, "formatter", None), JsonLogFormatter) for h in logger.handlers
    )
    if not has_json_handler:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
    logger.propagate = False
