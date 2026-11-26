"""API tests for `/v1/memories` listing, explicit creation, `/v1/operations`, and MemOS flow."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from mem_ai.api.dependencies import ProviderRegistry, get_memory_provider
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import NotFoundError
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.memories.router import get_conversation_access_checker
from mem_ai.modules.users import service as users_service
from tests.conftest import BuiltinUsers, FakeMemoryProvider


class _InMemoryConversationAccessChecker:
    """Test seam simulating owned conversations before Ticket 05 lands."""

    def __init__(self, owned: dict[UUID, set[UUID]]) -> None:
        self._owned = owned

    async def ensure_conversation_accessible(
        self,
        *,
        owner_id: UUID,
        conversation_id: UUID,
    ) -> None:
        if conversation_id not in self._owned.get(owner_id, set()):
            raise NotFoundError(
                code="conversation_not_found",
                message="conversation not found",
            )

    async def ensure_conversation_accessible_in_session(
        self, session, *, owner_id: UUID, conversation_id: UUID
    ) -> None:
        await self.ensure_conversation_accessible(
            owner_id=owner_id, conversation_id=conversation_id
        )


class _ScriptedMemoryProvider:
    """Scripted MemoryProvider for testing 202 accepted, reconciling, and failure paths."""

    provider_id = "scripted-memory"

    def __init__(self, factory) -> None:  # type: ignore[no-untyped-def]
        self._factory = factory

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        return []

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return self._factory(item)

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self.create_memory(item)

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        return MemoryListPage(items=[], next_cursor=None, has_more=False)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        return MemoryDeleteResult(status="succeeded", external_ref=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        return MemoryDeleteResult(status="succeeded", external_ref=str(user_id))

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


@pytest.mark.integration
@pytest.mark.anyio
async def test_memories_crud_pagination_scope_ownership_and_user_isolation(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
    fake_memory_provider: FakeMemoryProvider,
) -> None:
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(memory_provider=fake_memory_provider),
    )
    alice_headers = builtin_users.alice.headers
    bob_headers = builtin_users.bob.headers

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated requests are rejected with 401.
            assert (await client.get("/v1/memories")).status_code == 401
            assert (await client.post("/v1/memories", json={"content": "hi"})).status_code == 401
            assert (await client.get(f"/v1/operations/{uuid4()}")).status_code == 401

            # Resolve Alice's user ID via `/v1/users/me` so we can register an owned conversation.
            alice_me = await client.get("/v1/users/me", headers=alice_headers)
            assert alice_me.status_code == 200
            alice_user_id = UUID(alice_me.json()["data"]["userId"])
            alice_conv_id = uuid4()
            other_conv_id = uuid4()

            checker = _InMemoryConversationAccessChecker({alice_user_id: {alice_conv_id}})
            app.dependency_overrides[get_conversation_access_checker] = lambda: checker

            # 2. Client cannot inject userId or pass blank content.
            tampered = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "hello", "userId": str(uuid4())},
            )
            assert tampered.status_code == 422

            blank = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "   "},
            )
            assert blank.status_code == 422

            # 3. Default explicit create uses scope='user' and returns 201.
            created_ids: list[str] = []
            for idx in range(3):
                resp = await client.post(
                    "/v1/memories",
                    headers=alice_headers,
                    json={"content": f"Alice preference #{idx}"},
                )
                assert resp.status_code == 201
                body = resp.json()
                assert body["success"] is True
                assert body["code"] == "ok"
                assert body["data"]["scope"] == "user"
                assert body["data"]["conversationId"] is None
                assert body["data"]["content"] == f"Alice preference #{idx}"
                created_ids.append(body["data"]["memoryId"])

            # 4. Conversation scope requires conversationId and checks ownership.
            missing_conv = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Needs conv id", "scope": "conversation"},
            )
            assert missing_conv.status_code == 422

            unowned_conv = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={
                    "content": "Foreign conv",
                    "scope": "conversation",
                    "conversationId": str(other_conv_id),
                },
            )
            assert unowned_conv.status_code == 404

            owned_conv = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={
                    "content": "Temporary task note",
                    "scope": "conversation",
                    "conversationId": str(alice_conv_id),
                },
            )
            assert owned_conv.status_code == 201
            assert owned_conv.json()["data"]["scope"] == "conversation"
            assert owned_conv.json()["data"]["conversationId"] == str(alice_conv_id)

            # Bob cannot create a conversation-scoped memory in Alice's conversation (404).
            bob_in_alice_conv = await client.post(
                "/v1/memories",
                headers=bob_headers,
                json={
                    "content": "Bob intrusion",
                    "scope": "conversation",
                    "conversationId": str(alice_conv_id),
                },
            )
            assert bob_in_alice_conv.status_code == 404

            # 5. Pagination limit validation (>100 rejected) and multi-page listing.
            assert (
                await client.get("/v1/memories", headers=alice_headers, params={"limit": 101})
            ).status_code == 422

            page1 = await client.get(
                "/v1/memories",
                headers=alice_headers,
                params={"scope": "user", "limit": 2},
            )
            assert page1.status_code == 200
            p1_data = page1.json()["data"]
            assert len(p1_data["items"]) == 2
            assert p1_data["has_more"] is True
            assert p1_data["next_cursor"] is not None

            page2 = await client.get(
                "/v1/memories",
                headers=alice_headers,
                params={"scope": "user", "limit": 2, "cursor": p1_data["next_cursor"]},
            )
            assert page2.status_code == 200
            p2_data = page2.json()["data"]
            assert len(p2_data["items"]) == 1
            assert p2_data["has_more"] is False
            assert p2_data["next_cursor"] is None

            all_listed_ids = [item["memoryId"] for item in p1_data["items"] + p2_data["items"]]
            assert set(all_listed_ids) == set(created_ids)

            # 6. User isolation: Bob sees zero of Alice's memories and cannot use Alice's cursor.
            bob_list = await client.get("/v1/memories", headers=bob_headers)
            assert bob_list.status_code == 200
            assert bob_list.json()["data"]["items"] == []

            bob_reuses_cursor = await client.get(
                "/v1/memories",
                headers=bob_headers,
                params={"scope": "user", "limit": 2, "cursor": p1_data["next_cursor"]},
            )
            assert bob_reuses_cursor.status_code == 422
            assert bob_reuses_cursor.json()["code"] == "invalid_cursor"

            # 7. Disabled user is blocked with 403.
            await users_service.set_user_status(
                app.state.session_factory,
                user_id=alice_user_id,
                status="disabled",
            )
            disabled_resp = await client.get("/v1/memories", headers=alice_headers)
            assert disabled_resp.status_code == 403
            assert disabled_resp.json()["code"] == "user_disabled"


@pytest.mark.integration
@pytest.mark.anyio
async def test_explicit_create_202_operations_reconciling_and_error_mapping(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    mode = {"kind": "accepted"}

    def result_factory(item: MemoryWriteInput) -> MemoryWriteResult:
        if mode["kind"] == "accepted":
            return MemoryWriteResult(
                status="accepted",
                provider_id="scripted-memory",
                memory_id=None,
                external_ref=f"batch_{item.write_id}",
            )
        if mode["kind"] == "reconciling":
            return MemoryWriteResult(
                status="reconciling",
                provider_id="scripted-memory",
                memory_id=None,
                external_ref=item.write_id,
                error_code="memory_provider_timeout",
            )
        return MemoryWriteResult(
            status="failed",
            provider_id="scripted-memory",
            external_ref=item.write_id,
            error_code="memory_provider_unavailable",
        )

    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(memory_provider=_ScriptedMemoryProvider(result_factory)),
    )
    alice_headers = builtin_users.alice.headers
    bob_headers = builtin_users.bob.headers

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Async acceptance returns 202 + Location + queryable operationId.
            resp_202 = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Remember I live in Shanghai."},
            )
            assert resp_202.status_code == 202
            body_202 = resp_202.json()
            op_id = body_202["data"]["operationId"]
            status_url = body_202["data"]["statusUrl"]
            assert status_url == f"/v1/operations/{op_id}"
            assert resp_202.headers["location"] == status_url
            assert body_202["data"]["status"] == "running"

            # Owner can poll operation status.
            op_resp = await client.get(status_url, headers=alice_headers)
            assert op_resp.status_code == 200
            op_data = op_resp.json()["data"]
            assert op_data["operationId"] == op_id
            assert op_data["operationType"] == "memory_explicit_create"
            assert op_data["status"] == "running"
            assert op_data["progress"]["phase"] == "accepted"

            # Other user receives uniform 404.
            bob_op = await client.get(status_url, headers=bob_headers)
            assert bob_op.status_code == 404

            # 2. Timeout / unknown outcome returns 202 with status='reconciling'.
            mode["kind"] = "reconciling"
            resp_rec = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "Remember my dentist is on Tuesday."},
            )
            assert resp_rec.status_code == 202
            rec_data = resp_rec.json()["data"]
            assert rec_data["status"] == "reconciling"
            assert resp_rec.headers["location"] == rec_data["statusUrl"]

            rec_op = await client.get(rec_data["statusUrl"], headers=alice_headers)
            assert rec_op.status_code == 200
            assert rec_op.json()["data"]["status"] == "reconciling"
            assert rec_op.json()["data"]["error"]["code"] == "memory_provider_timeout"

            # 3. Deterministic provider failure returns 503 stable error envelope.
            mode["kind"] = "failed"
            resp_fail = await client.post(
                "/v1/memories",
                headers=alice_headers,
                json={"content": "PRIVATE_SHOULD_NOT_LEAK"},
            )
            assert resp_fail.status_code == 503
            fail_body = resp_fail.json()
            assert fail_body["success"] is False
            assert fail_body["code"] == "memory_provider_unavailable"
            assert "PRIVATE_SHOULD_NOT_LEAK" not in json.dumps(fail_body)


@pytest.mark.integration
@pytest.mark.anyio
async def test_end_to_end_api_with_memos_adapter_redacts_secrets_and_content(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    secret_key = "memos-live-secret-never-log-xyz"
    secret_content = "USER_PRIVATE_MEDICAL_NOTE_98765"
    now_ref = [datetime(2026, 11, 21, 14, 0, 0, tzinfo=UTC)]
    upstream_mode = {"fail_add": False}

    def memos_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Token {secret_key}"
        if request.url.path.endswith("/add/message"):
            if upstream_mode["fail_add"]:
                return httpx.Response(
                    500,
                    json={"code": 500, "message": f"failed for {secret_key} {secret_content}"},
                )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {"memory_detail_list": [{"id": "memos_e2e_1"}]},
                },
            )
        if request.url.path.endswith("/get/memory"):
            ts_ms = int(now_ref[0].timestamp() * 1000)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "message": "ok",
                    "data": {
                        "memory_detail_list": [
                            {
                                "id": "memos_e2e_2",
                                "memory_value": "Second item",
                                "create_time": ts_ms,
                                "tags": ["memai:scope:user"],
                            },
                            {
                                "id": "memos_e2e_1",
                                "memory_value": "First item",
                                "create_time": ts_ms - 1000,
                                "tags": ["memai:scope:user"],
                            },
                        ]
                    },
                },
            )
        return httpx.Response(404)

    app = create_app(migrated_pg_settings)
    mock_http = httpx.AsyncClient(transport=httpx.MockTransport(memos_handler))

    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    try:
        async with app.router.lifespan_context(app):
            memos_provider = MemOSMemoryProvider(
                http_client=mock_http,
                session_factory=app.state.session_factory,
                api_key=SecretStr(secret_key),
                base_url="https://memos.test.local/api/openmem/v1",
                snapshot_ttl_seconds=30,
                clock=lambda: now_ref[0],
            )
            app.dependency_overrides[get_memory_provider] = lambda: memos_provider

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                headers = builtin_users.alice.headers
                created = await client.post(
                    "/v1/memories",
                    headers=headers,
                    json={"content": secret_content},
                )
                assert created.status_code == 201
                assert created.json()["data"]["memoryId"] == "memos_e2e_1"

                page1 = await client.get("/v1/memories", headers=headers, params={"limit": 1})
                assert page1.status_code == 200
                cursor = page1.json()["data"]["next_cursor"]
                assert cursor is not None

                # Advance clock beyond snapshot TTL -> 422 cursor_expired.
                now_ref[0] = now_ref[0] + timedelta(seconds=60)
                expired = await client.get(
                    "/v1/memories",
                    headers=headers,
                    params={"limit": 1, "cursor": cursor},
                )
                assert expired.status_code == 422
                assert expired.json()["code"] == "cursor_expired"

                # Upstream 500 -> 502 with neither API key nor user content in body or logs.
                upstream_mode["fail_add"] = True
                failed = await client.post(
                    "/v1/memories",
                    headers=headers,
                    json={"content": secret_content},
                )
                assert failed.status_code == 502
                assert failed.json()["code"] == "memory_provider_error"
                assert secret_key not in failed.text
                assert secret_content not in failed.text

        logs_text = log_buffer.getvalue()
        assert secret_key not in logs_text
        assert secret_content not in logs_text
    finally:
        logger.removeHandler(log_handler)
        await mock_http.aclose()
