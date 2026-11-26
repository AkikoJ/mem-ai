"""Contract and PostgreSQL integration tests for `MemOSMemoryProvider` with controlled HTTP."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from mem_ai.contracts.memory_provider import (
    MemoryListQuery,
    MemoryProvider,
    MemoryRecallQuery,
    MemoryWriteInput,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    BadGatewayError,
    GatewayTimeoutError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from tests.conftest import create_builtin_user

_SECRET_API_KEY = "memos-secret-key-do-not-leak-999"
_BASE_URL = "https://memos.test.local/api/openmem/v1"


def _make_memos_provider(
    *,
    session_factory: Any,
    handler: Any,
    now_ref: list[datetime] | None = None,
    list_page_size: int = 2,
    list_max_items: int = 100,
    snapshot_ttl_seconds: int = 120,
) -> tuple[MemOSMemoryProvider, httpx.AsyncClient]:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    clock = (lambda: now_ref[0]) if now_ref is not None else None
    provider = MemOSMemoryProvider(
        http_client=client,
        session_factory=session_factory,
        api_key=SecretStr(_SECRET_API_KEY),
        base_url=_BASE_URL,
        timeout_seconds=5.0,
        list_page_size=list_page_size,
        list_max_items=list_max_items,
        snapshot_ttl_seconds=snapshot_ttl_seconds,
        clock=clock,
    )
    return provider, client


@pytest.mark.integration
@pytest.mark.anyio
async def test_memos_create_memory_covers_sync_async_timeout_failure_and_redaction(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    captured_requests: list[tuple[str, dict[str, str], dict[str, Any]]] = []
    mode = {"behavior": "sync_single_id"}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        captured_requests.append((str(request.url), dict(request.headers), body))
        current = mode["behavior"]
        if current == "sync_single_id":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {
                        "request_id": "req_memos_1",
                        "memory_detail_list": [{"id": "memos_id_001"}],
                    },
                },
            )
        if current == "async_accepted":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {"success": True, "task_id": "task_memos_77"},
                },
            )
        if current == "read_timeout":
            raise httpx.ReadTimeout("simulated read timeout", request=request)
        if current == "pool_timeout":
            raise httpx.PoolTimeout("pool exhausted before send", request=request)
        if current == "http_504":
            return httpx.Response(504, json={"code": 504, "message": "gateway timeout"})
        if current == "soft_failure":
            return httpx.Response(200, json={"code": 0, "data": {"success": False}})
        if current == "connect_error":
            raise httpx.ConnectError("simulated connect failure", request=request)
        if current == "auth_error":
            return httpx.Response(
                401,
                json={"code": 401, "message": f"invalid token {_SECRET_API_KEY}"},
            )
        if current == "rate_limited":
            return httpx.Response(429, json={"code": 429, "message": "too many requests"})
        return httpx.Response(500, json={"code": 500, "message": "upstream boom"})

    provider, client = _make_memos_provider(session_factory=session_factory, handler=handler)

    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    try:
        assert isinstance(provider, MemoryProvider)
        assert _SECRET_API_KEY not in repr(provider)

        user_id = uuid4()
        conv_id = uuid4()
        secret_user_text = "SENSITIVE_USER_SECRET_CONTENT_XYZ"

        # 1. Sync single ID response + conversation scope metadata encoding.
        res_sync = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_sync_1",
                user_id=user_id,
                scope="conversation",
                content=secret_user_text,
                conversation_id=conv_id,
                memory_epoch=3,
            )
        )
        assert res_sync.status == "succeeded"
        assert res_sync.provider_id == "memos"
        assert res_sync.memory_id == "memos_id_001"
        assert res_sync.external_ref == "req_memos_1"

        url, headers, payload = captured_requests[-1]
        assert url == f"{_BASE_URL}/add/message"
        assert headers["authorization"] == f"Token {_SECRET_API_KEY}"
        assert payload["user_id"] == str(user_id)
        assert payload["conversation_id"] == str(conv_id)
        assert payload["async_mode"] is False
        assert "memai:scope:conversation" in payload["tags"]
        assert f"memai:conv:{conv_id}" in payload["tags"]
        assert "memai:epoch:3" in payload["tags"]
        assert "memai:write:mw_sync_1" in payload["tags"]
        assert payload["info"]["biz_id"] == "mw_sync_1"
        assert payload["info"]["scene"] == "scope:conversation"
        assert payload["info"]["business_type"] == f"conv:{conv_id}"
        assert payload["info"]["custom_status"] == "epoch:3"

        # 2. Async extraction acceptance (no single memory ID yet).
        mode["behavior"] = "async_accepted"
        res_async = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_async_2",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
                memory_epoch=0,
            )
        )
        assert res_async.status == "accepted"
        assert res_async.memory_id is None
        assert res_async.external_ref == "task_memos_77"
        assert captured_requests[-1][2]["conversation_id"] == f"memai-user-{user_id}"

        # 3. ReadTimeout after send -> reconciling with write_id for later verification.
        mode["behavior"] = "read_timeout"
        res_timeout = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_timeout_3",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_timeout.status == "reconciling"
        assert res_timeout.external_ref == "mw_timeout_3"
        assert res_timeout.error_code == "memory_provider_timeout"

        # 4. PoolTimeout / ConnectError before send -> deterministic failure (unavailable).
        mode["behavior"] = "pool_timeout"
        res_pool = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_pool_3b",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_pool.status == "failed"
        assert res_pool.error_code == "memory_provider_unavailable"

        # Upstream HTTP 504 -> reconciling with memory_provider_timeout.
        mode["behavior"] = "http_504"
        res_504 = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_504_3c",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_504.status == "reconciling"
        assert res_504.error_code == "memory_provider_timeout"

        # Soft failure `data.success == False` -> failed, never accepted.
        mode["behavior"] = "soft_failure"
        res_soft = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_soft_3d",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_soft.status == "failed"
        assert res_soft.error_code == "memory_provider_error"

        mode["behavior"] = "connect_error"
        res_conn = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_conn_4",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_conn.status == "failed"
        assert res_conn.error_code == "memory_provider_unavailable"

        # 5. Auth error, rate limit, and server error map to stable error codes.
        mode["behavior"] = "auth_error"
        res_auth = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_auth_5",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_auth.status == "failed"
        assert res_auth.error_code == "memory_provider_auth_error"

        mode["behavior"] = "rate_limited"
        res_429 = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_429_6",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_429.status == "failed"
        assert res_429.error_code == "memory_provider_rate_limited"

        mode["behavior"] = "server_error"
        res_500 = await provider.create_memory(
            MemoryWriteInput(
                write_id="mw_500_7",
                user_id=user_id,
                scope="user",
                content=secret_user_text,
            )
        )
        assert res_500.status == "failed"
        assert res_500.error_code == "memory_provider_error"

        # Neither secret API key nor user memory content may ever appear in logs.
        all_logs = log_buffer.getvalue()
        assert _SECRET_API_KEY not in all_logs
        assert secret_user_text not in all_logs
    finally:
        logger.removeHandler(log_handler)
        await client.aclose()
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memos_list_memories_paginates_via_pg_snapshot_with_stable_sort_expiry_and_isolation(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    now_ref = [datetime(2026, 11, 21, 12, 0, 0, tzinfo=UTC)]
    http_calls: list[dict[str, Any]] = []

    try:
        alice = await create_builtin_user(session_factory, email="alice-memos-list@example.com")
        bob = await create_builtin_user(session_factory, email="bob-memos-list@example.com")
        conv_1 = uuid4()

        ts_same_ms = int(datetime(2026, 11, 21, 11, 0, 0, tzinfo=UTC).timestamp() * 1000)
        ts_older_ms = int(datetime(2026, 11, 21, 10, 0, 0, tzinfo=UTC).timestamp() * 1000)

        alice_pages: dict[int, list[dict[str, Any]]] = {
            1: [
                {
                    "id": "mem_10",
                    "user_id": str(alice.id),
                    "memory_value": "Older user memory",
                    "create_time": ts_older_ms,
                    "tags": ["memai:scope:user", "memai:epoch:1"],
                },
                {
                    "id": "mem_foreign_leak",
                    "user_id": str(bob.id),
                    "memory_value": "Must be filtered out as foreign user",
                    "create_time": ts_same_ms,
                    "tags": ["memai:scope:user"],
                },
            ],
            2: [
                {
                    "id": "mem_20",
                    "user_id": str(alice.id),
                    "memory_value": "Same-ts B",
                    "create_time": ts_same_ms,
                    "tags": ["memai:scope:user", "memai:epoch:1"],
                },
                {
                    "memory_value": "Malformed entry without id must not truncate paging",
                    "create_time": ts_same_ms,
                },
            ],
            3: [
                {
                    "id": "mem_30",
                    "memory_value": "Same-ts C (should sort first)",
                    "create_time": ts_same_ms,
                    "tags": ["memai:scope:user", "memai:epoch:1"],
                },
                {
                    "id": "mem_conv",
                    "memory_value": "Conversation-only memory",
                    "create_time": ts_same_ms,
                    "conversation_id": str(conv_1),
                    "tags": ["memai:scope:conversation", f"memai:conv:{conv_1}", "memai:epoch:1"],
                },
            ],
            4: [],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            http_calls.append(body)
            assert request.headers["authorization"] == f"Token {_SECRET_API_KEY}"
            if body["user_id"] == str(alice.id):
                page_items = alice_pages.get(int(body.get("page", 1)), [])
                return httpx.Response(
                    200,
                    json={"code": 0, "message": "ok", "data": {"memory_detail_list": page_items}},
                )
            return httpx.Response(
                200,
                json={"code": 0, "message": "ok", "data": {"memory_detail_list": []}},
            )

        provider_replica_1, client_1 = _make_memos_provider(
            session_factory=session_factory,
            handler=handler,
            now_ref=now_ref,
            list_page_size=2,
            snapshot_ttl_seconds=60,
        )
        provider_replica_2, client_2 = _make_memos_provider(
            session_factory=session_factory,
            handler=handler,
            now_ref=now_ref,
            list_page_size=2,
            snapshot_ttl_seconds=60,
        )
        try:
            # First page of user-scoped memories (3 matching items: mem_30, mem_20, mem_10).
            page1 = await provider_replica_1.list_memories(
                MemoryListQuery(user_id=alice.id, scope="user", limit=2)
            )
            assert [item.memory_id for item in page1.items] == ["mem_30", "mem_20"]
            assert page1.has_more is True
            assert page1.next_cursor is not None
            calls_after_page1 = len(http_calls)
            assert calls_after_page1 >= 2

            # Second page via a separate replica reads directly from PostgreSQL snapshot (0 HTTP).
            page2 = await provider_replica_2.list_memories(
                MemoryListQuery(
                    user_id=alice.id,
                    scope="user",
                    limit=2,
                    cursor=page1.next_cursor,
                )
            )
            assert len(http_calls) == calls_after_page1
            assert [item.memory_id for item in page2.items] == ["mem_10"]
            assert page2.has_more is False
            assert page2.next_cursor is None

            # Conversation scope filter returns only conversation-scoped items.
            conv_page = await provider_replica_1.list_memories(
                MemoryListQuery(
                    user_id=alice.id,
                    scope="conversation",
                    conversation_id=conv_1,
                    limit=10,
                )
            )
            assert [item.memory_id for item in conv_page.items] == ["mem_conv"]
            assert conv_page.items[0].conversation_id == conv_1
            assert conv_page.items[0].scope == "conversation"

            # Cross-user isolation: Bob gets empty list and cannot reuse Alice's cursor.
            bob_page = await provider_replica_1.list_memories(
                MemoryListQuery(user_id=bob.id, scope="user", limit=10)
            )
            assert bob_page.items == []
            with pytest.raises(UnprocessableEntityError) as cross_err:
                await provider_replica_2.list_memories(
                    MemoryListQuery(
                        user_id=bob.id,
                        scope="user",
                        limit=2,
                        cursor=page1.next_cursor,
                    )
                )
            assert cross_err.value.code == "invalid_cursor"

            # Expired cursor raises 422 cursor_expired.
            now_ref[0] = now_ref[0] + timedelta(seconds=120)
            with pytest.raises(UnprocessableEntityError) as exp_err:
                await provider_replica_2.list_memories(
                    MemoryListQuery(
                        user_id=alice.id,
                        scope="user",
                        limit=2,
                        cursor=page1.next_cursor,
                    )
                )
            assert exp_err.value.code == "cursor_expired"
        finally:
            await client_1.aclose()
            await client_2.aclose()
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memos_list_memories_maps_upstream_timeout_and_errors(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    mode = {"behavior": "timeout"}

    def handler(request: httpx.Request) -> httpx.Response:
        if mode["behavior"] == "timeout":
            raise httpx.ReadTimeout("slow upstream", request=request)
        if mode["behavior"] == "http_504":
            return httpx.Response(504, json={"code": 504, "message": "gateway timeout"})
        if mode["behavior"] == "connect":
            raise httpx.ConnectError("down", request=request)
        return httpx.Response(500, json={"code": 500, "message": "internal"})

    provider, client = _make_memos_provider(session_factory=session_factory, handler=handler)
    try:
        user_id = uuid4()
        with pytest.raises(GatewayTimeoutError) as timeout_err:
            await provider.list_memories(MemoryListQuery(user_id=user_id, limit=10))
        assert timeout_err.value.code == "memory_provider_timeout"

        mode["behavior"] = "http_504"
        with pytest.raises(GatewayTimeoutError) as http_504_err:
            await provider.list_memories(MemoryListQuery(user_id=user_id, limit=10))
        assert http_504_err.value.code == "memory_provider_timeout"

        mode["behavior"] = "connect"
        with pytest.raises(ServiceUnavailableError) as conn_err:
            await provider.list_memories(MemoryListQuery(user_id=user_id, limit=10))
        assert conn_err.value.code == "memory_provider_unavailable"

        mode["behavior"] = "server_error"
        with pytest.raises(BadGatewayError) as bad_gw:
            await provider.list_memories(MemoryListQuery(user_id=user_id, limit=10))
        assert bad_gw.value.code == "memory_provider_error"
    finally:
        await client.aclose()
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memos_recall_filters_by_tags_info_scope_user_and_redacts_logs(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    captured_requests: list[tuple[str, dict[str, str], dict[str, Any]]] = []
    mode = {"behavior": "user_and_conv"}

    alice_id = uuid4()
    bob_id = uuid4()
    conv_a = uuid4()
    conv_b = uuid4()
    secret_query = "SENSITIVE_RECALL_QUERY_XYZ"

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        captured_requests.append((str(request.url), dict(request.headers), body))
        if mode["behavior"] == "timeout":
            raise httpx.ReadTimeout("recall slow", request=request)
        if mode["behavior"] == "connect":
            raise httpx.ConnectError("recall down", request=request)
        if mode["behavior"] == "server_error":
            return httpx.Response(500, json={"code": 500, "message": "boom"})

        if body.get("conversation_id") == str(conv_a):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {
                        "memory_detail_list": [
                            {
                                "id": "mem_conv_a_ok",
                                "user_id": str(alice_id),
                                "memory_value": "Conv A task context",
                                "conversation_id": str(conv_a),
                                "tags": [
                                    "memai:scope:conversation",
                                    f"memai:conv:{conv_a}",
                                    "memai:epoch:2",
                                ],
                                "relativity": 0.88,
                            },
                            {
                                "id": "mem_conv_b_leak",
                                "user_id": str(alice_id),
                                "memory_value": "Conv B leaked item",
                                "conversation_id": str(conv_b),
                                "tags": ["memai:scope:conversation", f"memai:conv:{conv_b}"],
                                "relativity": 0.99,
                            },
                            {
                                "id": "mem_bob_leak",
                                "user_id": str(bob_id),
                                "memory_value": "Bob secret",
                                "conversation_id": str(conv_a),
                                "tags": ["memai:scope:conversation", f"memai:conv:{conv_a}"],
                                "relativity": 0.99,
                            },
                        ],
                        "preference_detail_list": [],
                    },
                },
            )

        return httpx.Response(
            200,
            json={
                "code": 0,
                "message": "ok",
                "data": {
                    "memory_detail_list": [
                        {
                            "id": "mem_user_ok",
                            "user_id": str(alice_id),
                            "memory_value": "Alice prefers concise answers",
                            "scene": "scope:user",
                            "custom_status": "epoch:2",
                            "relativity": 0.91,
                        },
                        {
                            "id": "mem_user_low_rel",
                            "user_id": str(alice_id),
                            "memory_value": "Low relevance item",
                            "tags": ["memai:scope:user"],
                            "relativity": 0.2,
                        },
                        {
                            "id": "mem_user_zero_rel",
                            "user_id": str(alice_id),
                            "memory_value": "Zero relevance item",
                            "tags": ["memai:scope:user"],
                            "relativity": 0.0,
                        },
                        {
                            "id": "mem_wrong_scope",
                            "user_id": str(alice_id),
                            "memory_value": "Conversation memory returned in user search",
                            "tags": ["memai:scope:conversation", f"memai:conv:{conv_a}"],
                            "relativity": 0.97,
                        },
                    ],
                    "preference_detail_list": [
                        {
                            "id": "pref_user_1",
                            "user_id": str(alice_id),
                            "preference": "Prefers Python 3.13",
                            "tags": ["memai:scope:user", "memai:epoch:2"],
                            "relativity": 0.96,
                        }
                    ],
                },
            },
        )

    provider, client = _make_memos_provider(session_factory=session_factory, handler=handler)
    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    try:
        # 1. User-scoped recall sends `tags` + `scene` filters, omits conversation_id,
        # caps limit <= 25, parses both facts and preferences, and filters low-rel/wrong-scope.
        user_items = await provider.recall(
            MemoryRecallQuery(
                user_id=alice_id,
                query=secret_query,
                scope="user",
                limit=40,
                min_relevance=0.5,
            )
        )
        assert [item.memory_id for item in user_items] == ["pref_user_1", "mem_user_ok"]
        assert user_items[0].content == "Prefers Python 3.13"
        assert user_items[0].relevance == pytest.approx(0.96)
        assert user_items[1].memory_epoch == 2
        assert user_items[1].relevance == pytest.approx(0.91)

        url_u, headers_u, payload_u = captured_requests[-1]
        assert url_u == f"{_BASE_URL}/search/memory"
        assert headers_u["authorization"] == f"Token {_SECRET_API_KEY}"
        assert "conversation_id" not in payload_u
        assert payload_u["memory_limit_number"] == 25
        assert payload_u["preference_limit_number"] == 25
        assert payload_u["relativity"] == pytest.approx(0.5)
        and_filters_u = payload_u["filter"]["and"]
        assert {"tags": {"contains": "memai:scope:user"}} in and_filters_u
        assert {"scene": "scope:user"} in and_filters_u

        # 2. Conversation-scoped recall sends `tags` + `scene` + `business_type` + conversation_id
        # and drops other conversations & foreign users returned by a buggy upstream.
        conv_items = await provider.recall(
            MemoryRecallQuery(
                user_id=alice_id,
                query=secret_query,
                scope="conversation",
                conversation_id=conv_a,
                limit=5,
                min_relevance=0.5,
            )
        )
        assert [item.memory_id for item in conv_items] == ["mem_conv_a_ok"]
        assert conv_items[0].conversation_id == conv_a
        assert conv_items[0].memory_epoch == 2

        _, _, payload_c = captured_requests[-1]
        assert payload_c["conversation_id"] == str(conv_a)
        and_filters_c = payload_c["filter"]["and"]
        assert {"tags": {"contains": "memai:scope:conversation"}} in and_filters_c
        assert {"tags": {"contains": f"memai:conv:{conv_a}"}} in and_filters_c
        assert {"scene": "scope:conversation"} in and_filters_c
        assert {"business_type": f"conv:{conv_a}"} in and_filters_c

        # 3. Conversation scope without conversation_id safely returns empty without HTTP call.
        calls_before = len(captured_requests)
        empty_items = await provider.recall(
            MemoryRecallQuery(
                user_id=alice_id,
                query=secret_query,
                scope="conversation",
                conversation_id=None,
                limit=5,
            )
        )
        assert empty_items == []
        assert len(captured_requests) == calls_before

        # 3b. Relevance sorting occurs BEFORE `limit` truncation (high-relevance preference
        #     beats lower-relevance fact even when fact appeared first in upstream response),
        #     and `min_relevance=0.0` retains unscored items consistently.
        top_one = await provider.recall(
            MemoryRecallQuery(
                user_id=alice_id,
                query=secret_query,
                scope="user",
                limit=1,
                min_relevance=0.5,
            )
        )
        assert [item.memory_id for item in top_one] == ["pref_user_1"]
        zero_thresh = await provider.recall(
            MemoryRecallQuery(
                user_id=alice_id,
                query=secret_query,
                scope="user",
                limit=10,
                min_relevance=0.0,
            )
        )
        assert [item.memory_id for item in zero_thresh] == [
            "pref_user_1",
            "mem_user_ok",
            "mem_user_low_rel",
            "mem_user_zero_rel",
        ]

        # 4. Upstream errors map cleanly and never leak API key or query text in logs.
        mode["behavior"] = "timeout"
        with pytest.raises(GatewayTimeoutError) as timeout_err:
            await provider.recall(
                MemoryRecallQuery(user_id=alice_id, query=secret_query, scope="user", limit=5)
            )
        assert timeout_err.value.code == "memory_provider_timeout"

        mode["behavior"] = "connect"
        with pytest.raises(ServiceUnavailableError) as conn_err:
            await provider.recall(
                MemoryRecallQuery(user_id=alice_id, query=secret_query, scope="user", limit=5)
            )
        assert conn_err.value.code == "memory_provider_unavailable"

        mode["behavior"] = "server_error"
        with pytest.raises(BadGatewayError) as bad_gw:
            await provider.recall(
                MemoryRecallQuery(user_id=alice_id, query=secret_query, scope="user", limit=5)
            )
        assert bad_gw.value.code == "memory_provider_error"

        all_logs = log_buffer.getvalue()
        assert _SECRET_API_KEY not in all_logs
        assert secret_query not in all_logs
    finally:
        logger.removeHandler(log_handler)
        await client.aclose()
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_memos_delete_clear_ownership_guard_and_verification_contract(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    alice_id = uuid4()
    bob_id = uuid4()
    store: dict[str, tuple[UUID, str]] = {
        "mem_alice_1": (alice_id, "Alice memory"),
        "mem_bob_1": (bob_id, "Bob secret"),
    }
    delete_calls: list[dict[str, Any]] = []
    mode = {"get_behavior": "ok", "delete_behavior": "ok", "clear_behavior": "ok"}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        if request.url.path.endswith("/get/memory"):
            if mode["get_behavior"] == "timeout":
                raise httpx.ReadTimeout("get timeout", request=request)
            uid = body["user_id"]
            page = int(body.get("page", 1))
            if page > 1:
                return httpx.Response(200, json={"code": 0, "data": {"memory_detail_list": []}})
            items = [
                {
                    "id": mid,
                    "user_id": str(owner),
                    "memory_value": text,
                    "tags": ["memai:scope:user"],
                }
                for mid, (owner, text) in store.items()
                if str(owner) == uid
            ]
            return httpx.Response(200, json={"code": 0, "data": {"memory_detail_list": items}})

        if request.url.path.endswith("/delete/memory"):
            delete_calls.append(body)
            if "memory_ids" in body:
                if mode["delete_behavior"] == "timeout":
                    raise httpx.ReadTimeout("delete timeout", request=request)
                for mid in body["memory_ids"]:
                    store.pop(mid, None)
                return httpx.Response(200, json={"code": 0, "data": {"success": "true"}})
            if "user_id" in body:
                if mode["clear_behavior"] == "timeout":
                    raise httpx.ReadTimeout("clear timeout", request=request)
                uid = body["user_id"]
                for mid in [k for k, (owner, _) in store.items() if str(owner) == uid]:
                    store.pop(mid, None)
                return httpx.Response(200, json={"code": 0, "data": {"success": "true"}})
        return httpx.Response(404)

    provider, client = _make_memos_provider(
        session_factory=session_factory, handler=handler, list_page_size=20
    )
    try:
        # 1. Bob cannot delete Alice's memory: ownership check blocks `/delete/memory` call!
        bob_del = await provider.delete_memory(user_id=bob_id, memory_id="mem_alice_1")
        assert bob_del.status == "failed"
        assert bob_del.error_code == "memory_not_found"
        assert delete_calls == []
        assert "mem_alice_1" in store

        # 1b. Pre-delete ownership read timeout fails closed (`failed`, never `reconciling`).
        mode["get_behavior"] = "timeout"
        pre_timeout = await provider.delete_memory(user_id=bob_id, memory_id="mem_alice_1")
        assert pre_timeout.status == "failed"
        assert pre_timeout.error_code == "memory_provider_timeout"
        assert delete_calls == []
        mode["get_behavior"] = "ok"

        # 1c. Bounded list truncation at `list_max_items` fails closed for both delete and verify.
        capped_provider, capped_client = _make_memos_provider(
            session_factory=session_factory,
            handler=handler,
            list_page_size=1,
            list_max_items=1,
        )
        try:
            capped_del = await capped_provider.delete_memory(
                user_id=bob_id, memory_id="mem_alice_1"
            )
            assert capped_del.status == "failed"
            assert capped_del.error_code == "memory_provider_error"
            assert delete_calls == []
            capped_verify = await capped_provider.verify_operation(
                user_id=bob_id, external_ref="delete_memory:mem_alice_1"
            )
            assert capped_verify.status == "reconciling"
            assert capped_verify.error_code == "memory_verification_truncated"
        finally:
            await capped_client.aclose()

        # 2. Alice delete times out -> reconciling; verify sees still present -> failed -> retry ok.
        mode["delete_behavior"] = "timeout"
        res_timeout = await provider.delete_memory(user_id=alice_id, memory_id="mem_alice_1")
        assert res_timeout.status == "reconciling"
        assert res_timeout.external_ref == "delete_memory:mem_alice_1"

        verify_present = await provider.verify_operation(
            user_id=alice_id, external_ref="delete_memory:mem_alice_1"
        )
        assert verify_present.status == "failed"
        assert verify_present.error_code == "memory_still_present"

        mode["delete_behavior"] = "ok"
        res_ok = await provider.delete_memory(user_id=alice_id, memory_id="mem_alice_1")
        assert res_ok.status == "succeeded"
        verify_gone = await provider.verify_operation(
            user_id=alice_id, external_ref="delete_memory:mem_alice_1"
        )
        assert verify_gone.status == "succeeded"

        # 3. Clear times out -> reconciling; verify sees Bob's memory -> retry clears only Bob.
        mode["clear_behavior"] = "timeout"
        clear_timeout = await provider.clear_memories(user_id=bob_id)
        assert clear_timeout.status == "reconciling"
        assert clear_timeout.external_ref == f"clear_memories:{bob_id}"
        verify_clear_fail = await provider.verify_operation(
            user_id=bob_id, external_ref=f"clear_memories:{bob_id}"
        )
        assert verify_clear_fail.status == "failed"

        mode["clear_behavior"] = "ok"
        clear_ok = await provider.clear_memories(user_id=bob_id)
        assert clear_ok.status == "succeeded"
        verify_clear_ok = await provider.verify_operation(
            user_id=bob_id, external_ref=f"clear_memories:{bob_id}"
        )
        assert verify_clear_ok.status == "succeeded"
        assert store == {}
    finally:
        await client.aclose()
        await engine.dispose()
