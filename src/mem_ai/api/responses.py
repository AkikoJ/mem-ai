"""Unified HTTP JSON response envelope (`success`, `code`, `message`, `data`, `request_id`)."""

from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, Field

from mem_ai.core.logging import get_request_id

T = TypeVar("T")


def _current_request_id() -> str:
    return get_request_id() or "req_unknown"


class ApiResponse(BaseModel, Generic[T]):  # noqa: UP046
    """Canonical JSON response envelope for successful HTTP responses."""

    success: bool = True
    code: str = "ok"
    message: str = "ok"
    data: T
    request_id: str = Field(default_factory=_current_request_id)


def ok_response(  # noqa: UP047
    data: T,
    *,
    message: str = "ok",
    code: str = "ok",
) -> ApiResponse[T]:
    """Wrap a response payload in the canonical successful `ApiResponse` envelope."""
    return ApiResponse(
        success=True,
        code=code,
        message=message,
        data=data,
        request_id=_current_request_id(),
    )
