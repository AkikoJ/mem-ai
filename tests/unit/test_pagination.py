"""Unit tests for stable cursor pagination and unified collection envelopes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from mem_ai.api.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    CursorPayload,
    PaginatedResponse,
    PaginationParams,
    decode_cursor,
    encode_cursor,
    hash_filters,
)
from mem_ai.core.exceptions import UnprocessableEntityError


def test_pagination_params_enforce_default_and_upper_bound() -> None:
    default_params = PaginationParams()
    assert default_params.limit == DEFAULT_PAGE_LIMIT == 20
    assert default_params.cursor is None

    max_params = PaginationParams(limit=MAX_PAGE_LIMIT)
    assert max_params.limit == 100

    with pytest.raises(ValidationError):
        PaginationParams(limit=0)

    with pytest.raises(ValidationError):
        PaginationParams(limit=101)


def test_cursor_round_trip_preserves_sort_keys_direction_and_filters() -> None:
    created_at = datetime(2026, 11, 21, 7, 30, 0, 123456, tzinfo=UTC)
    item_id = UUID("11111111-1111-4111-8111-111111111111")
    filters_hash = hash_filters({"scope": "user", "owner_id": "usr_1"})

    token = encode_cursor(
        CursorPayload(
            created_at=created_at,
            id=item_id,
            direction="desc",
            filters_hash=filters_hash,
        )
    )
    decoded = decode_cursor(token, expected_direction="desc", expected_filters_hash=filters_hash)

    assert decoded.created_at == created_at
    assert decoded.id == item_id
    assert decoded.direction == "desc"
    assert decoded.filters_hash == filters_hash


def test_cursor_rejects_tampered_direction_mismatch_filter_mismatch_and_expiry() -> None:
    created_at = datetime(2026, 11, 21, 7, 30, 0, tzinfo=UTC)
    item_id = UUID("22222222-2222-4222-8222-222222222222")
    filters_hash = hash_filters({"owner_id": "usr_1"})

    token = encode_cursor(
        CursorPayload(
            created_at=created_at,
            id=item_id,
            direction="asc",
            filters_hash=filters_hash,
        )
    )

    with pytest.raises(UnprocessableEntityError) as malformed:
        decode_cursor("not-valid-base64!!!", expected_direction="asc")
    assert malformed.value.code == "invalid_cursor"

    with pytest.raises(UnprocessableEntityError) as wrong_dir:
        decode_cursor(token, expected_direction="desc", expected_filters_hash=filters_hash)
    assert wrong_dir.value.code == "invalid_cursor"

    with pytest.raises(UnprocessableEntityError) as wrong_filter:
        decode_cursor(
            token,
            expected_direction="asc",
            expected_filters_hash=hash_filters({"owner_id": "usr_2"}),
        )
    assert wrong_filter.value.code == "invalid_cursor"

    expired_token = encode_cursor(
        CursorPayload(
            created_at=created_at,
            id=item_id,
            direction="desc",
            filters_hash=filters_hash,
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    with pytest.raises(UnprocessableEntityError) as expired:
        decode_cursor(
            expired_token,
            expected_direction="desc",
            expected_filters_hash=filters_hash,
        )
    assert expired.value.code == "cursor_expired"


def test_paginated_response_serializes_snake_case_envelope() -> None:
    response = PaginatedResponse[str](items=["a", "b"], next_cursor="cur_1", has_more=True)
    assert response.model_dump() == {
        "items": ["a", "b"],
        "next_cursor": "cur_1",
        "has_more": True,
    }
