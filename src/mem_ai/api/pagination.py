"""Stable cursor pagination parameters, cursor codec, and collection envelope."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mem_ai.core.exceptions import UnprocessableEntityError

DEFAULT_PAGE_LIMIT = 20
MAX_PAGE_LIMIT = 100

SortDirection = Literal["asc", "desc"]
T = TypeVar("T")


class PaginationParams(BaseModel):
    """Query parameters for cursor-paginated collections."""

    model_config = ConfigDict(extra="forbid")

    limit: int = Field(default=DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT)
    cursor: str | None = Field(default=None, min_length=1, max_length=2048)


class CursorPayload(BaseModel):
    """Structured sort key and validation context embedded in an opaque cursor."""

    model_config = ConfigDict(extra="forbid")

    created_at: datetime
    id: UUID | str
    direction: SortDirection
    filters_hash: str | None = None
    expires_at: datetime | None = None


class PaginatedResponse(BaseModel, Generic[T]):  # noqa: UP046
    """Unified snake_case collection response envelope."""

    items: list[T]
    next_cursor: str | None = None
    has_more: bool = False


def hash_filters(filters: Mapping[str, Any]) -> str:
    """Compute a deterministic SHA-256 digest of query filter criteria."""
    canonical = json.dumps(
        {str(key): value for key, value in sorted(filters.items())},
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def encode_cursor(payload: CursorPayload) -> str:
    """Encode a cursor payload into an opaque URL-safe base64 token."""
    raw = payload.model_dump_json().encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(
    token: str,
    *,
    expected_direction: SortDirection,
    expected_filters_hash: str | None = None,
    now: datetime | None = None,
) -> CursorPayload:
    """Decode and validate an opaque pagination cursor."""
    try:
        padding = "=" * (-len(token) % 4)
        decoded_bytes = base64.urlsafe_b64decode((token + padding).encode("ascii"))
        payload = CursorPayload.model_validate_json(decoded_bytes)
    except (ValueError, UnicodeError, binascii.Error, ValidationError) as exc:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor is invalid",
        ) from exc

    if payload.direction != expected_direction:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor direction does not match request",
        )

    if expected_filters_hash is not None and payload.filters_hash != expected_filters_hash:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor does not match query filters",
        )

    if payload.expires_at is not None:
        current_time = now if now is not None else datetime.now(UTC)
        expires_at = (
            payload.expires_at
            if payload.expires_at.tzinfo is not None
            else payload.expires_at.replace(tzinfo=UTC)
        )
        if expires_at <= current_time:
            raise UnprocessableEntityError(
                code="cursor_expired",
                message="pagination cursor has expired",
            )

    return payload
