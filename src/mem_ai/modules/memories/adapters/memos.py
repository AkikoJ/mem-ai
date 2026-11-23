"""MemOS Cloud HTTP adapter implementing the project-owned `MemoryProvider` protocol."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    CLEAR_MEMORIES_REF_PREFIX,
    DELETE_MEMORY_REF_PREFIX,
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryRecord,
    MemoryScope,
    MemoryWriteInput,
    MemoryWriteResult,
    build_clear_memories_external_ref,
    build_delete_memory_external_ref,
)
from mem_ai.core.exceptions import AppError
from mem_ai.modules.memories.service import map_memory_provider_error
from mem_ai.modules.memories.snapshots import MemoryListScopeKey, MemorySnapshotStore

logger = logging.getLogger("mem_ai.memories.memos")

_TAG_SCOPE_PREFIX = "memai:scope:"
_TAG_CONV_PREFIX = "memai:conv:"
_TAG_EPOCH_PREFIX = "memai:epoch:"
_TAG_WRITE_PREFIX = "memai:write:"


@dataclass(frozen=True, slots=True)
class _MemosCallOutcome:
    """Normalized outcome of a single MemOS HTTP POST call."""

    ok: bool
    data: dict[str, Any]
    error_code: str | None = None
    uncertain: bool = False


def _parse_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return UUID(value.strip())
    except ValueError:
        return None


def _epoch_to_utc(raw_number: int | float) -> datetime:
    seconds = raw_number / 1000.0 if raw_number > 1e11 else float(raw_number)
    return datetime.fromtimestamp(seconds, tz=UTC)


def _parse_timestamp(raw: Any, *, fallback: datetime) -> datetime:
    if isinstance(raw, (int, float)):
        return _epoch_to_utc(raw)
    if isinstance(raw, str) and raw.strip():
        text = raw.strip()
        if text.isdigit():
            return _epoch_to_utc(int(text))
        with contextlib.suppress(ValueError):
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return fallback


def _extract_scope_metadata(raw: dict[str, Any]) -> tuple[MemoryScope, UUID | None, int]:
    tags_raw = raw.get("tags")
    tags: list[str] = [str(tag) for tag in tags_raw] if isinstance(tags_raw, list) else []
    info_raw = raw.get("info")
    info: dict[str, Any] = info_raw if isinstance(info_raw, dict) else {}

    scope: MemoryScope = "user"
    conversation_id: UUID | None = None
    memory_epoch = 0

    for tag in tags:
        if tag == f"{_TAG_SCOPE_PREFIX}conversation":
            scope = "conversation"
        elif tag == f"{_TAG_SCOPE_PREFIX}user":
            scope = "user"
        elif tag.startswith(_TAG_CONV_PREFIX):
            parsed = _parse_uuid(tag[len(_TAG_CONV_PREFIX) :])
            if parsed is not None:
                conversation_id = parsed
        elif tag.startswith(_TAG_EPOCH_PREFIX):
            with contextlib.suppress(ValueError):
                memory_epoch = max(0, int(tag[len(_TAG_EPOCH_PREFIX) :]))

    scene = info.get("scene") if "scene" in info else raw.get("scene")
    if scene == "scope:conversation":
        scope = "conversation"
    elif scene == "scope:user":
        scope = "user"

    if conversation_id is None:
        business_type = (
            info.get("business_type") if "business_type" in info else raw.get("business_type")
        )
        if isinstance(business_type, str) and business_type.startswith("conv:"):
            conversation_id = _parse_uuid(business_type[len("conv:") :])
        elif scope == "conversation":
            conversation_id = _parse_uuid(raw.get("conversation_id"))

    if memory_epoch == 0:
        custom_status = (
            info.get("custom_status") if "custom_status" in info else raw.get("custom_status")
        )
        if isinstance(custom_status, str) and custom_status.startswith("epoch:"):
            with contextlib.suppress(ValueError):
                memory_epoch = max(0, int(custom_status[len("epoch:") :]))

    if scope == "user":
        conversation_id = None

    return scope, conversation_id, memory_epoch


def _encode_write_metadata(item: MemoryWriteInput) -> tuple[str, list[str], dict[str, str]]:
    tags = [
        f"{_TAG_SCOPE_PREFIX}{item.scope}",
        f"{_TAG_EPOCH_PREFIX}{item.memory_epoch}",
        f"{_TAG_WRITE_PREFIX}{item.write_id}",
    ]
    if item.scope == "conversation" and item.conversation_id is not None:
        conv_str = str(item.conversation_id)
        tags.append(f"{_TAG_CONV_PREFIX}{conv_str}")
        business_type = f"conv:{conv_str}"
    else:
        conv_str = f"memai-user-{item.user_id}"
        business_type = "user"

    info = {
        "biz_id": item.write_id,
        "scene": f"scope:{item.scope}",
        "business_type": business_type,
        "custom_status": f"epoch:{item.memory_epoch}",
    }
    return conv_str, tags, info


def _extract_single_memory_id(data: dict[str, Any]) -> str | None:
    for direct_key in ("memory_id", "id"):
        val = data.get(direct_key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    for list_key in ("memory_detail_list", "memories"):
        items = data.get(list_key)
        if isinstance(items, list) and len(items) == 1 and isinstance(items[0], dict):
            raw_id = items[0].get("id") or items[0].get("memory_id")
            if isinstance(raw_id, str) and raw_id.strip():
                return raw_id.strip()

    return None


def _extract_external_ref(data: dict[str, Any], *, fallback: str) -> str:
    for key in ("task_id", "request_id", "batch_id", "trace_id"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return fallback


def _is_soft_failure(data: dict[str, Any]) -> bool:
    raw_success = data.get("success")
    if raw_success is False:
        return True
    return isinstance(raw_success, str) and raw_success.strip().lower() == "false"


def _http_status_to_error(status_code: int) -> tuple[str, bool]:
    """Return `(error_code, uncertain)` for an upstream HTTP error status."""
    if status_code in (408, 504):
        return ("memory_provider_timeout", True)
    if status_code in (401, 403):
        return ("memory_provider_auth_error", False)
    if status_code == 429:
        return ("memory_provider_rate_limited", False)
    if status_code == 503:
        return ("memory_provider_unavailable", False)
    return ("memory_provider_error", False)


class MemOSMemoryProvider:
    """Replaceable MemOS Cloud adapter encapsulating HTTP, auth, metadata, and snapshots."""

    provider_id: str = "memos"

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        api_key: SecretStr,
        base_url: str = "https://memos.memtensor.cn/api/openmem/v1",
        timeout_seconds: float = 15.0,
        list_page_size: int = 100,
        list_max_items: int = 1000,
        snapshot_ttl_seconds: int = 600,
        supports_write_status_verification: bool = True,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._http_client = http_client
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._list_page_size = list_page_size
        self._list_max_items = list_max_items
        self.supports_write_status_verification = supports_write_status_verification
        self._snapshots = MemorySnapshotStore(
            session_factory=session_factory,
            ttl_seconds=snapshot_ttl_seconds,
        )
        self._clock = clock

    def __repr__(self) -> str:
        return f"MemOSMemoryProvider(provider_id={self.provider_id!r}, base_url={self._base_url!r})"

    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Token {self._api_key.get_secret_value()}",
            "Content-Type": "application/json",
        }

    async def _post_memos(self, endpoint: str, payload: dict[str, Any]) -> _MemosCallOutcome:
        """Send one MemOS POST request and normalize transport, HTTP, and envelope outcomes."""
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        try:
            response = await self._http_client.post(
                url,
                headers=self._headers(),
                json=payload,
                timeout=self._timeout,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            logger.warning(
                "MemOS connection unavailable before request dispatch",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "endpoint": endpoint,
                        "error_code": "memory_provider_unavailable",
                    }
                },
            )
            return _MemosCallOutcome(
                ok=False,
                data={},
                error_code="memory_provider_unavailable",
                uncertain=False,
            )
        except (httpx.TimeoutException, httpx.RemoteProtocolError):
            logger.warning(
                "MemOS response timed out or dropped after send",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "endpoint": endpoint,
                        "error_code": "memory_provider_timeout",
                    }
                },
            )
            return _MemosCallOutcome(
                ok=False,
                data={},
                error_code="memory_provider_timeout",
                uncertain=True,
            )
        except httpx.HTTPError:
            logger.warning(
                "MemOS transport error",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "endpoint": endpoint,
                        "error_code": "memory_provider_error",
                    }
                },
            )
            return _MemosCallOutcome(
                ok=False,
                data={},
                error_code="memory_provider_error",
                uncertain=False,
            )

        try:
            body = response.json()
        except ValueError:
            body = None

        if response.status_code >= 400:
            error_code, uncertain = _http_status_to_error(response.status_code)
            logger.warning(
                "MemOS returned non-success HTTP status",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "endpoint": endpoint,
                        "http_status": response.status_code,
                        "error_code": error_code,
                    }
                },
            )
            return _MemosCallOutcome(
                ok=False,
                data={},
                error_code=error_code,
                uncertain=uncertain,
            )

        if not isinstance(body, dict) or body.get("code") not in (0, None):
            logger.warning(
                "MemOS returned invalid JSON or non-zero application code",
                extra={
                    "extra_fields": {
                        "provider": self.provider_id,
                        "endpoint": endpoint,
                        "memos_code": body.get("code") if isinstance(body, dict) else None,
                        "error_code": "memory_provider_error",
                    }
                },
            )
            return _MemosCallOutcome(
                ok=False,
                data={},
                error_code="memory_provider_error",
                uncertain=False,
            )

        data = body.get("data")
        data_dict: dict[str, Any] = data if isinstance(data, dict) else {}
        if _is_soft_failure(data_dict):
            return _MemosCallOutcome(
                ok=False,
                data=data_dict,
                error_code="memory_provider_error",
                uncertain=False,
            )
        return _MemosCallOutcome(ok=True, data=data_dict)

    async def _add_message(
        self,
        item: MemoryWriteInput,
        *,
        async_mode: bool,
        source: str,
    ) -> MemoryWriteResult:
        conv_str, tags, info = _encode_write_metadata(item)
        payload: dict[str, Any] = {
            "user_id": str(item.user_id),
            "conversation_id": conv_str,
            "messages": [{"role": "user", "content": item.content}],
            "async_mode": async_mode,
            "tags": tags,
            "info": info,
            "source": source,
        }
        outcome = await self._post_memos("/add/message", payload)
        if not outcome.ok:
            return MemoryWriteResult(
                status="reconciling" if outcome.uncertain else "failed",
                provider_id=self.provider_id,
                external_ref=item.write_id,
                error_code=outcome.error_code or "memory_provider_error",
            )

        external_ref = _extract_external_ref(outcome.data, fallback=item.write_id)
        single_id = _extract_single_memory_id(outcome.data)
        if not async_mode and single_id is not None:
            return MemoryWriteResult(
                status="succeeded",
                provider_id=self.provider_id,
                memory_id=single_id,
                external_ref=external_ref,
            )

        return MemoryWriteResult(
            status="accepted",
            provider_id=self.provider_id,
            memory_id=None,
            external_ref=external_ref,
        )

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        """Explicitly create a memory via MemOS `POST /add/message` with `async_mode=false`."""
        return await self._add_message(item, async_mode=False, source="mem-ai:explicit")

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        """Persist derived turn memory via MemOS `POST /add/message` with `async_mode=true`."""
        return await self._add_message(item, async_mode=True, source="mem-ai:turn")

    def _parse_memory_items(
        self,
        *,
        data: dict[str, Any],
        expected_user_id: UUID,
        fallback_time: datetime,
    ) -> tuple[int, list[MemoryRecord]]:
        raw_items: list[dict[str, Any]] = []
        for list_key in ("memory_detail_list", "preference_detail_list", "memories"):
            candidate = data.get(list_key)
            if isinstance(candidate, list):
                raw_items.extend(entry for entry in candidate if isinstance(entry, dict))

        records: list[MemoryRecord] = []
        for raw in raw_items:
            raw_id = raw.get("id") or raw.get("memory_id")
            if not isinstance(raw_id, str) or not raw_id.strip():
                continue

            # Preserve upstream user_id ownership if provided; reject non-matching foreign entries.
            raw_user_id = raw.get("user_id")
            if raw_user_id is not None:
                parsed_owner = _parse_uuid(raw_user_id)
                if parsed_owner is None or parsed_owner != expected_user_id:
                    continue
                record_user_id = parsed_owner
            else:
                record_user_id = expected_user_id

            raw_content = raw.get("memory_value") or raw.get("preference") or raw.get("content")
            content = str(raw_content) if raw_content is not None else ""
            scope, conv_id, epoch = _extract_scope_metadata(raw)
            created_at = _parse_timestamp(
                raw.get("create_time") or raw.get("created_at"),
                fallback=fallback_time,
            )
            records.append(
                MemoryRecord(
                    memory_id=raw_id.strip(),
                    provider_id=self.provider_id,
                    user_id=record_user_id,
                    scope=scope,
                    content=content,
                    conversation_id=conv_id,
                    memory_epoch=epoch,
                    created_at=created_at,
                )
            )
        return len(raw_items), records

    async def _fetch_all_user_memories_bounded(self, *, user_id: UUID) -> list[MemoryRecord]:
        collected: list[MemoryRecord] = []
        seen_ids: set[str] = set()
        fallback_time = self._now()
        page = 1

        while len(collected) < self._list_max_items:
            payload = {
                "user_id": str(user_id),
                "page": page,
                "size": self._list_page_size,
                "include_preference": True,
                "include_tool_memory": False,
            }
            outcome = await self._post_memos("/get/memory", payload)
            if not outcome.ok:
                raise map_memory_provider_error(
                    outcome.error_code,
                    default_message="memory provider request failed",
                )

            raw_count, batch = self._parse_memory_items(
                data=outcome.data,
                expected_user_id=user_id,
                fallback_time=fallback_time,
            )
            if raw_count == 0:
                break

            new_count = 0
            for item in batch:
                if item.memory_id not in seen_ids:
                    seen_ids.add(item.memory_id)
                    collected.append(item)
                    new_count += 1
                    if len(collected) >= self._list_max_items:
                        break

            if raw_count < self._list_page_size or (batch and new_count == 0):
                break
            page += 1

        return collected

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        """List memories with PostgreSQL-backed bounded snapshot cursor pagination."""
        now = self._now()
        scope_key = MemoryListScopeKey(
            user_id=query.user_id,
            provider_id=self.provider_id,
            scope=query.scope,
            conversation_id=query.conversation_id,
        )
        if query.cursor is not None:
            return await self._snapshots.read_next_page(
                scope_key=scope_key,
                cursor=query.cursor,
                limit=query.limit,
                now=now,
            )

        all_records = await self._fetch_all_user_memories_bounded(user_id=query.user_id)
        filtered = [
            rec
            for rec in all_records
            if (query.scope is None or rec.scope == query.scope)
            and (query.conversation_id is None or rec.conversation_id == query.conversation_id)
        ]
        return await self._snapshots.create_first_page(
            scope_key=scope_key,
            records=filtered,
            limit=query.limit,
            now=now,
        )

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        """Recall relevant scoped memories via MemOS `POST /search/memory`."""
        if query.scope == "conversation" and query.conversation_id is None:
            return []

        filter_conditions: list[dict[str, Any]] = [
            {"tags": {"contains": f"{_TAG_SCOPE_PREFIX}{query.scope}"}},
            {"scene": f"scope:{query.scope}"},
        ]
        if query.scope == "conversation" and query.conversation_id is not None:
            conv_str = str(query.conversation_id)
            filter_conditions.append({"tags": {"contains": f"{_TAG_CONV_PREFIX}{conv_str}"}})
            filter_conditions.append({"business_type": f"conv:{conv_str}"})

        # MemOS Cloud caps search output limits at 25 per category.
        bounded_limit = min(query.limit, 25)
        payload: dict[str, Any] = {
            "user_id": str(query.user_id),
            "query": query.query,
            "memory_limit_number": bounded_limit,
            "preference_limit_number": bounded_limit,
            "include_preference": True,
            "include_tool_memory": False,
            "filter": {"and": filter_conditions},
        }
        if query.scope == "conversation" and query.conversation_id is not None:
            payload["conversation_id"] = str(query.conversation_id)
        if query.min_relevance is not None:
            payload["relativity"] = query.min_relevance

        outcome = await self._post_memos("/search/memory", payload)
        if not outcome.ok:
            raise map_memory_provider_error(
                outcome.error_code,
                default_message="memory recall failed",
            )

        _, parsed = self._parse_memory_items(
            data=outcome.data,
            expected_user_id=query.user_id,
            fallback_time=self._now(),
        )
        relevances: dict[str, float] = {}
        for key in ("memory_detail_list", "preference_detail_list", "memories"):
            entries = outcome.data.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                raw_id = entry.get("id") or entry.get("memory_id")
                if not isinstance(raw_id, str) or not raw_id.strip():
                    continue
                raw_rel: Any = None
                for rel_key in ("relativity", "relevance", "score"):
                    if entry.get(rel_key) is not None:
                        raw_rel = entry.get(rel_key)
                        break
                if isinstance(raw_rel, (int, float)) and not isinstance(raw_rel, bool):
                    clamped = max(0.0, min(1.0, float(raw_rel)))
                    relevances[raw_id.strip()] = clamped

        recalled: list[MemoryRecallItem] = []
        for rec in parsed:
            if rec.scope != query.scope:
                continue
            if query.scope == "conversation" and rec.conversation_id != query.conversation_id:
                continue
            if query.scope == "user" and rec.conversation_id is not None:
                continue
            rel = relevances.get(rec.memory_id)
            if query.min_relevance is not None:
                if rel is None:
                    if query.min_relevance > 0.0:
                        continue
                elif rel < query.min_relevance:
                    continue
            recalled.append(
                MemoryRecallItem(
                    memory_id=rec.memory_id,
                    provider_id=self.provider_id,
                    scope=rec.scope,
                    content=rec.content,
                    relevance=rel,
                    conversation_id=rec.conversation_id,
                    memory_epoch=rec.memory_epoch,
                )
            )
        recalled.sort(
            key=lambda item: (
                -(item.relevance if item.relevance is not None else -1.0),
                -item.memory_epoch,
                item.memory_id,
            )
        )
        return recalled[: query.limit]

    async def _fetch_owned_records_or_error(
        self, *, user_id: UUID
    ) -> tuple[list[MemoryRecord] | None, str | None]:
        """Fetch owned records for ownership/verification guards, normalizing `AppError`."""
        try:
            return await self._fetch_all_user_memories_bounded(user_id=user_id), None
        except AppError as exc:
            return None, exc.code

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        """Verify ownership for `user_id` before deleting one memory via `POST /delete/memory`."""
        default_ref = build_delete_memory_external_ref(memory_id)
        # Pre-delete ownership read occurs before any upstream mutation: fail closed on any error
        # (including timeout) so we never enter `reconciling` before `/delete/memory` is sent.
        owned_records, fetch_error = await self._fetch_owned_records_or_error(user_id=user_id)
        if owned_records is None:
            return MemoryDeleteResult(
                status="failed",
                external_ref=default_ref,
                error_code=fetch_error or "memory_provider_error",
            )

        owned_ids = {rec.memory_id for rec in owned_records}
        if memory_id not in owned_ids:
            if len(owned_records) < self._list_max_items:
                return MemoryDeleteResult(
                    status="failed",
                    external_ref=default_ref,
                    error_code="memory_not_found",
                )
            # Bounded scan hit `list_max_items` without proving ownership; fail closed to block
            # cross-user deletion because `/delete/memory` with `memory_ids` omits `user_id`.
            return MemoryDeleteResult(
                status="failed",
                external_ref=default_ref,
                error_code="memory_provider_error",
            )

        outcome = await self._post_memos("/delete/memory", {"memory_ids": [memory_id]})
        if not outcome.ok:
            return MemoryDeleteResult(
                status="reconciling" if outcome.uncertain else "failed",
                external_ref=default_ref,
                error_code=outcome.error_code or "memory_provider_error",
            )
        ext_ref = _extract_external_ref(outcome.data, fallback=default_ref)
        return MemoryDeleteResult(status="succeeded", external_ref=ext_ref)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        """Clear all memories for a user via MemOS `POST /delete/memory`."""
        default_ref = build_clear_memories_external_ref(user_id)
        outcome = await self._post_memos("/delete/memory", {"user_id": str(user_id)})
        if not outcome.ok:
            return MemoryDeleteResult(
                status="reconciling" if outcome.uncertain else "failed",
                external_ref=default_ref,
                error_code=outcome.error_code or "memory_provider_error",
            )
        ext_ref = _extract_external_ref(outcome.data, fallback=default_ref)
        return MemoryDeleteResult(status="succeeded", external_ref=ext_ref)

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        """Verify an async or uncertain external MemOS write, delete, or clear operation."""
        if external_ref.startswith(DELETE_MEMORY_REF_PREFIX):
            memory_id = external_ref[len(DELETE_MEMORY_REF_PREFIX) :]
            owned_records, fetch_error = await self._fetch_owned_records_or_error(user_id=user_id)
            if owned_records is None:
                return MemoryOperationStatus(
                    status="reconciling",
                    external_ref=external_ref,
                    error_code=fetch_error,
                )
            if memory_id not in {rec.memory_id for rec in owned_records}:
                if len(owned_records) >= self._list_max_items:
                    return MemoryOperationStatus(
                        status="reconciling",
                        external_ref=external_ref,
                        error_code="memory_verification_truncated",
                    )
                return MemoryOperationStatus(status="succeeded", external_ref=external_ref)
            return MemoryOperationStatus(
                status="failed",
                external_ref=external_ref,
                error_code="memory_still_present",
            )

        if external_ref.startswith(CLEAR_MEMORIES_REF_PREFIX):
            owned_records, fetch_error = await self._fetch_owned_records_or_error(user_id=user_id)
            if owned_records is None:
                return MemoryOperationStatus(
                    status="reconciling",
                    external_ref=external_ref,
                    error_code=fetch_error,
                )
            if not owned_records:
                return MemoryOperationStatus(status="succeeded", external_ref=external_ref)
            return MemoryOperationStatus(
                status="failed",
                external_ref=external_ref,
                error_code="memories_still_present",
            )

        outcome = await self._post_memos(
            "/get/status",
            {"user_id": str(user_id), "task_id": external_ref},
        )
        if not outcome.ok:
            return MemoryOperationStatus(
                status="reconciling",
                external_ref=external_ref,
                error_code=outcome.error_code or "memory_provider_error",
            )

        raw_status = str(outcome.data.get("status") or "").lower()
        single_id = _extract_single_memory_id(outcome.data)
        if raw_status in ("succeeded", "completed", "done", "success"):
            return MemoryOperationStatus(
                status="succeeded",
                external_ref=external_ref,
                memory_id=single_id,
            )
        if raw_status in ("failed", "error"):
            return MemoryOperationStatus(
                status="failed",
                external_ref=external_ref,
                error_code="memory_provider_error",
            )
        return MemoryOperationStatus(status="reconciling", external_ref=external_ref)
