"""PostgreSQL-backed bounded pagination snapshot store for page-based memory providers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.api.pagination import CursorPayload, decode_cursor, encode_cursor, hash_filters
from mem_ai.contracts.memory_provider import MemoryListPage, MemoryRecord, MemoryScope
from mem_ai.core.exceptions import UnprocessableEntityError
from mem_ai.modules.memories import repository

_CURSOR_SEP = "|"


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class MemoryListScopeKey:
    """Canonical owner, provider, and filter key binding a paginated memory list."""

    user_id: UUID
    provider_id: str
    scope: MemoryScope | None = None
    conversation_id: UUID | None = None

    def filters_hash(self) -> str:
        """Compute a deterministic filter digest binding user, provider, scope, and conversation."""
        return hash_filters(
            {
                "user_id": str(self.user_id),
                "provider_id": self.provider_id,
                "scope": self.scope or "",
                "conversation_id": (
                    str(self.conversation_id) if self.conversation_id is not None else ""
                ),
            }
        )


def sort_memory_records_desc(records: list[MemoryRecord]) -> list[MemoryRecord]:
    """Deduplicate by `memory_id` and sort stably by `(created_at DESC, memory_id DESC)`."""
    deduped: dict[str, MemoryRecord] = {}
    for record in records:
        existing = deduped.get(record.memory_id)
        if existing is None or _normalize_utc(record.created_at) >= _normalize_utc(
            existing.created_at
        ):
            deduped[record.memory_id] = record
    return sorted(
        deduped.values(),
        key=lambda item: (_normalize_utc(item.created_at), item.memory_id),
        reverse=True,
    )


def _encode_snapshot_cursor(
    *,
    snapshot_id: UUID,
    last_record: MemoryRecord,
    filters_hash: str,
    expires_at: datetime,
) -> str:
    payload = CursorPayload(
        created_at=_normalize_utc(last_record.created_at),
        id=f"{snapshot_id.hex}{_CURSOR_SEP}{last_record.memory_id}",
        direction="desc",
        filters_hash=filters_hash,
        expires_at=_normalize_utc(expires_at),
    )
    return encode_cursor(payload)


def _parse_snapshot_cursor_id(raw_id: UUID | str) -> tuple[UUID, str]:
    raw_text = str(raw_id)
    if _CURSOR_SEP not in raw_text:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor is invalid",
        )
    snapshot_hex, last_memory_id = raw_text.split(_CURSOR_SEP, 1)
    if not last_memory_id:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor is invalid",
        )
    try:
        snapshot_id = UUID(hex=snapshot_hex)
    except ValueError as exc:
        raise UnprocessableEntityError(
            code="invalid_cursor",
            message="pagination cursor is invalid",
        ) from exc
    return snapshot_id, last_memory_id


class MemorySnapshotStore:
    """Persists bounded sorted memory lists into PostgreSQL for cross-replica cursor pagination."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        ttl_seconds: int = 600,
    ) -> None:
        self._session_factory = session_factory
        self._ttl_seconds = ttl_seconds

    async def create_first_page(
        self,
        *,
        scope_key: MemoryListScopeKey,
        records: list[MemoryRecord],
        limit: int,
        now: datetime | None = None,
    ) -> MemoryListPage:
        """Sort `records`, persist a snapshot when `has_more`, and return the first page."""
        current_time = _normalize_utc(now if now is not None else datetime.now(UTC))
        sorted_records = sort_memory_records_desc(records)
        page_items = sorted_records[:limit]
        has_more = len(sorted_records) > limit
        if not has_more or not page_items:
            return MemoryListPage(items=page_items, next_cursor=None, has_more=False)

        filters_hash = scope_key.filters_hash()
        expires_at = current_time + timedelta(seconds=self._ttl_seconds)
        serialized_items = [item.model_dump(mode="json") for item in sorted_records]

        async with self._session_factory.begin() as session:
            snapshot = await repository.create_list_snapshot(
                session,
                user_id=scope_key.user_id,
                provider_id=scope_key.provider_id,
                filters_hash=filters_hash,
                items_json=serialized_items,
                expires_at=expires_at,
            )
            snapshot_id = snapshot.id

        next_cursor = _encode_snapshot_cursor(
            snapshot_id=snapshot_id,
            last_record=page_items[-1],
            filters_hash=filters_hash,
            expires_at=expires_at,
        )
        return MemoryListPage(items=page_items, next_cursor=next_cursor, has_more=True)

    async def read_next_page(
        self,
        *,
        scope_key: MemoryListScopeKey,
        cursor: str,
        limit: int,
        now: datetime | None = None,
    ) -> MemoryListPage:
        """Serve a subsequent page from a persisted PostgreSQL snapshot using keyset order."""
        current_time = _normalize_utc(now if now is not None else datetime.now(UTC))
        filters_hash = scope_key.filters_hash()
        cursor_payload = decode_cursor(
            cursor,
            expected_direction="desc",
            expected_filters_hash=filters_hash,
            now=current_time,
        )
        snapshot_id, last_memory_id = _parse_snapshot_cursor_id(cursor_payload.id)
        cursor_key = (_normalize_utc(cursor_payload.created_at), last_memory_id)

        async with self._session_factory() as session:
            snapshot = await repository.get_list_snapshot(
                session,
                snapshot_id=snapshot_id,
                user_id=scope_key.user_id,
                provider_id=scope_key.provider_id,
                filters_hash=filters_hash,
            )

        if snapshot is None or _normalize_utc(snapshot.expires_at) <= current_time:
            raise UnprocessableEntityError(
                code="cursor_expired",
                message="pagination cursor has expired",
            )

        records = [MemoryRecord.model_validate(raw) for raw in snapshot.items_json]
        remaining = [
            record
            for record in records
            if (_normalize_utc(record.created_at), record.memory_id) < cursor_key
        ]
        page_items = remaining[:limit]
        has_more = len(remaining) > limit
        next_cursor = (
            _encode_snapshot_cursor(
                snapshot_id=snapshot.id,
                last_record=page_items[-1],
                filters_hash=filters_hash,
                expires_at=snapshot.expires_at,
            )
            if has_more and page_items
            else None
        )
        return MemoryListPage(items=page_items, next_cursor=next_cursor, has_more=has_more)
