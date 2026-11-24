"""Stable domain and HTTP error types with lowercase snake_case error codes."""

from __future__ import annotations

import math
import re
from typing import Any

_ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class AppError(Exception):
    """Base application error mapped to the nested JSON error envelope."""

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        if not _ERROR_CODE_PATTERN.fullmatch(code):
            raise ValueError(f"Error code must be lowercase snake_case, got: {code!r}")
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details: dict[str, Any] = dict(details) if details is not None else {}
        self.headers: dict[str, str] = dict(headers) if headers is not None else {}


class UnauthorizedError(AppError):
    """401 authentication failure."""

    def __init__(
        self,
        *,
        code: str = "unauthorized",
        message: str = "authentication required",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=401, code=code, message=message, details=details)


class ForbiddenError(AppError):
    """403 permission denied or disabled/deleting subject."""

    def __init__(
        self,
        *,
        code: str = "forbidden",
        message: str = "access forbidden",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=403, code=code, message=message, details=details)


class NotFoundError(AppError):
    """404 resource not found (also used to hide cross-user existence)."""

    def __init__(
        self,
        *,
        code: str = "not_found",
        message: str = "resource not found",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=404, code=code, message=message, details=details)


class ConflictError(AppError):
    """409 generation conflict, idempotency conflict, version conflict, or barrier."""

    def __init__(
        self,
        *,
        code: str = "conflict",
        message: str = "resource conflict",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=409, code=code, message=message, details=details)


class UnprocessableEntityError(AppError):
    """422 semantic validation, cursor, or confirmation failure."""

    def __init__(
        self,
        *,
        code: str = "validation_error",
        message: str = "invalid request parameters",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=422, code=code, message=message, details=details)


class PayloadTooLargeError(AppError):
    """413 HTTP request payload exceeds configured byte budget."""

    def __init__(
        self,
        *,
        code: str = "payload_too_large",
        message: str = "request body exceeds maximum allowed size",
        max_bytes: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        merged_details = dict(details) if details is not None else {}
        if max_bytes is not None:
            merged_details.setdefault("max_request_body_bytes", max_bytes)
        super().__init__(status_code=413, code=code, message=message, details=merged_details)


class RateLimitError(AppError):
    """429 rate or resource budget exceeded."""

    def __init__(
        self,
        *,
        code: str = "rate_limit_exceeded",
        message: str = "rate limit exceeded",
        retry_after_seconds: int | float | None = None,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        merged_details = dict(details) if details is not None else {}
        merged_headers = dict(headers) if headers is not None else {}
        if retry_after_seconds is not None:
            retry_after_int = max(1, math.ceil(float(retry_after_seconds)))
            merged_details.setdefault("retry_after_seconds", retry_after_int)
            merged_headers.setdefault("Retry-After", str(retry_after_int))
        super().__init__(
            status_code=429,
            code=code,
            message=message,
            details=merged_details,
            headers=merged_headers,
        )


class BadGatewayError(AppError):
    """502 upstream provider failure."""

    def __init__(
        self,
        *,
        code: str = "upstream_error",
        message: str = "upstream dependency failed",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=502, code=code, message=message, details=details)


class ServiceUnavailableError(AppError):
    """503 required dependency unavailable."""

    def __init__(
        self,
        *,
        code: str = "service_unavailable",
        message: str = "service unavailable",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=503, code=code, message=message, details=details)


class GatewayTimeoutError(AppError):
    """504 upstream or generation timeout."""

    def __init__(
        self,
        *,
        code: str = "gateway_timeout",
        message: str = "upstream request timed out",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(status_code=504, code=code, message=message, details=details)
