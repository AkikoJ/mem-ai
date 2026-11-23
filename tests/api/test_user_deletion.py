"""HTTP contracts for accepting user deletion and operation-scoped deletion receipts."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from io import StringIO
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import func, select, text, update

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.core.logging import REDACTED_VALUE, JsonLogFormatter
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.jobs.models import Job
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.models import Operation
from mem_ai.modules.users.models import User, UserIdentity

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

_ALICE_HEADERS = {"Authorization": "Bearer token-alice"}
_BOB_HEADERS = {"Authorization": "Bearer token-bob"}
_RECEIPT_TTL_SECONDS = 3600


@pytest.fixture
def deletion_app(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> FastAPI:
    settings = migrated_pg_settings.model_copy(
        update={
            "deletion_receipt_secret": SecretStr("user-deletion-test-key-at-least-32-characters"),
            "deletion_receipt_ttl_seconds": _RECEIPT_TTL_SECONDS,
        }
    )
    return create_app(settings, credential_verifier=test_auth_adapter)


@pytest.fixture
async def deletion_client(deletion_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with (
        deletion_app.router.lifespan_context(deletion_app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=deletion_app), base_url="http://test"
        ) as client,
    ):
        yield client


async def test_delete_returns_202_with_one_time_receipt_location_and_expiry(
    deletion_client: httpx.AsyncClient,
) -> None:
    before = datetime.now(UTC)
    response = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    after = datetime.now(UTC)

    assert response.status_code == 202
    body = response.json()
    assert body["success"] is True
    assert body["code"] == "ok"
    data = body["data"]
    assert set(data) == {"operationId", "status", "statusUrl", "deletionReceipt", "expiresAt"}
    assert UUID(data["operationId"])
    assert data["status"] == "pending"
    assert data["statusUrl"] == f"/v1/operations/{data['operationId']}"
    assert response.headers["location"] == data["statusUrl"]
    assert response.headers["cache-control"] == "no-store"
    assert len(data["deletionReceipt"]) >= 43
    assert data["deletionReceipt"] not in data["statusUrl"]
    expires_at = datetime.fromisoformat(data["expiresAt"])
    ttl = timedelta(seconds=_RECEIPT_TTL_SECONDS)
    assert before + ttl <= expires_at <= after + ttl


async def test_deleting_user_credentials_remain_blocked_without_recreating_identity(
    deletion_app: FastAPI,
    deletion_client: httpx.AsyncClient,
) -> None:
    profile = await deletion_client.get("/v1/users/me", headers=_ALICE_HEADERS)
    assert profile.status_code == 200
    user_id = UUID(profile.json()["data"]["userId"])
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202

    for method, url in (
        ("GET", "/v1/users/me"),
        ("DELETE", "/v1/users/me"),
        ("GET", accepted.json()["data"]["statusUrl"]),
        ("GET", "/v1/agents"),
        ("GET", "/v1/conversations"),
        ("GET", "/v1/memories"),
    ):
        blocked = await deletion_client.request(method, url, headers=_ALICE_HEADERS)
        assert blocked.status_code == 403
        assert blocked.json()["code"] == "user_deleting"

    async with deletion_app.state.session_factory() as session:
        identity = (
            await session.execute(
                select(UserIdentity.user_id, User.status)
                .join(User, User.id == UserIdentity.user_id)
                .where(UserIdentity.subject == "alice")
            )
        ).one()
        assert identity == (user_id, "deleting")
        assert await session.scalar(select(func.count()).select_from(User)) == 1
        assert await session.scalar(select(func.count()).select_from(Operation)) == 1
        assert await session.scalar(select(func.count()).select_from(Job)) == 1


async def test_receipt_queries_its_deletion_operation_after_business_access_is_blocked(
    deletion_client: httpx.AsyncClient,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202
    data = accepted.json()["data"]

    status = await deletion_client.get(
        data["statusUrl"],
        headers={"Authorization": f"DeletionReceipt {data['deletionReceipt']}"},
    )

    assert status.status_code == 200
    operation = status.json()["data"]
    assert operation["operationId"] == data["operationId"]
    assert operation["operationType"] == "user_delete"
    assert operation["status"] == "pending"
    assert operation["progress"] == {"local_complete": False, "remote_complete": False}
    assert operation["expiresAt"] == data["expiresAt"]
    assert data["deletionReceipt"] not in status.text
    assert "deletionReceipt" not in operation
    assert "deletion_receipt_digest" not in operation

    other_user = await deletion_client.get(data["statusUrl"], headers=_BOB_HEADERS)
    assert other_user.status_code == 404
    assert other_user.json()["code"] == "operation_not_found"


async def test_receipt_rejects_another_users_deletion_operation_and_missing_operations(
    deletion_client: httpx.AsyncClient,
) -> None:
    alice = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    bob = await deletion_client.delete("/v1/users/me", headers=_BOB_HEADERS)
    assert alice.status_code == bob.status_code == 202
    alice_data = alice.json()["data"]
    bob_data = bob.json()["data"]
    assert alice_data["operationId"] != bob_data["operationId"]
    assert alice_data["deletionReceipt"] != bob_data["deletionReceipt"]
    headers = {"Authorization": f"DeletionReceipt {alice_data['deletionReceipt']}"}

    for url in (bob_data["statusUrl"], f"/v1/operations/{uuid4()}"):
        rejected = await deletion_client.get(url, headers=headers)
        assert rejected.status_code == 401
        assert rejected.json()["code"] == "invalid_deletion_receipt"
        assert rejected.json()["data"] is None


@pytest.mark.parametrize(
    "authorization",
    [
        "DeletionReceipt forged",
        f"DeletionReceipt {'A' * 43}",
        "DeletionReceipt",
        "DeletionReceipt invalid extra",
    ],
    ids=["malformed-token", "forged-token", "missing-token", "extra-token"],
)
async def test_forged_or_malformed_receipts_return_uniform_authentication_error(
    deletion_client: httpx.AsyncClient,
    authorization: str,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202

    rejected = await deletion_client.get(
        accepted.json()["data"]["statusUrl"], headers={"Authorization": authorization}
    )

    assert rejected.status_code == 401
    assert rejected.json()["code"] == "invalid_deletion_receipt"
    assert rejected.json()["message"] == "invalid deletion receipt"
    assert rejected.json()["data"] is None


@pytest.mark.parametrize("invalidation", ["expired", "revoked"])
async def test_expired_or_revoked_receipt_cannot_poll_its_operation(
    deletion_app: FastAPI,
    deletion_client: httpx.AsyncClient,
    invalidation: str,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202
    data = accepted.json()["data"]
    operation_id = UUID(data["operationId"])
    async with deletion_app.state.session_factory() as session, session.begin():
        if invalidation == "expired":
            await session.execute(
                update(Operation)
                .where(Operation.id == operation_id)
                .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        else:
            assert await operations_service.revoke_user_deletion_receipt_in_session(
                session, operation_id=operation_id
            )

    rejected = await deletion_client.get(
        data["statusUrl"],
        headers={"Authorization": f"DeletionReceipt {data['deletionReceipt']}"},
    )

    assert rejected.status_code == 401
    assert rejected.json()["code"] == "invalid_deletion_receipt"
    assert rejected.json()["message"] == "invalid deletion receipt"
    assert rejected.json()["data"] is None
    still_blocked = await deletion_client.get("/v1/users/me", headers=_ALICE_HEADERS)
    assert still_blocked.status_code == 403
    assert still_blocked.json()["code"] == "user_deleting"


async def test_receipt_cannot_authenticate_other_business_apis(
    deletion_client: httpx.AsyncClient,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202
    receipt = accepted.json()["data"]["deletionReceipt"]

    for method, url in (
        ("GET", "/v1/users/me"),
        ("DELETE", "/v1/users/me"),
        ("GET", "/v1/agents"),
        ("GET", "/v1/conversations"),
        ("GET", "/v1/memories"),
        ("GET", "/v1/models"),
    ):
        rejected = await deletion_client.request(
            method, url, headers={"Authorization": f"DeletionReceipt {receipt}"}
        )
        assert rejected.status_code == 401
        assert rejected.json()["code"] == "invalid_auth_scheme"


async def test_development_auth_cannot_bypass_receipt_scope_or_duplicate_header_rejection(
    migrated_pg_settings: Settings,
) -> None:
    settings = migrated_pg_settings.model_copy(update={"auth_mode": "development"})
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        profile = await client.get("/v1/users/me")
        assert profile.status_code == 200
        assert profile.json()["data"]["authMethod"] == "development"
        accepted = await client.delete("/v1/users/me")
        assert accepted.status_code == 202
        data = accepted.json()["data"]
        authorization = f"DeletionReceipt {data['deletionReceipt']}"
        valid = await client.get(data["statusUrl"], headers={"Authorization": authorization})
        assert valid.status_code == 200

        blocked = await client.get("/v1/users/me", headers={"Authorization": authorization})
        assert blocked.status_code == 401
        assert blocked.json()["code"] == "invalid_auth_scheme"
        forged = await client.get(
            data["statusUrl"], headers={"Authorization": f"DeletionReceipt {'A' * 43}"}
        )
        assert forged.status_code == 401
        assert forged.json()["code"] == "invalid_deletion_receipt"

        for first, second in (
            ("Bearer ignored-by-dev", authorization),
            (authorization, "Bearer ignored-by-dev"),
            (authorization, authorization),
        ):
            headers = [("Authorization", first), ("Authorization", second)]
            ambiguous = await client.get(data["statusUrl"], headers=headers)
            assert ambiguous.status_code == 401
            assert ambiguous.json()["code"] == "invalid_deletion_receipt"
            business = await client.get("/v1/users/me", headers=headers)
            assert business.status_code == 401
            assert business.json()["code"] == "invalid_auth_scheme"

        ordinary = await client.get("/v1/users/me")
        assert ordinary.status_code == 403
        assert ordinary.json()["code"] == "user_deleting"


async def test_receipt_is_persisted_only_as_a_digest_and_never_in_job_payloads(
    deletion_app: FastAPI,
    deletion_client: httpx.AsyncClient,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202
    data = accepted.json()["data"]
    receipt = data["deletionReceipt"]

    async with deletion_app.state.session_factory() as session:
        operation = await session.get(Operation, UUID(data["operationId"]))
        assert operation is not None
        digest = operation.deletion_receipt_digest
        assert digest is not None
        assert len(digest) == 64
        assert bytes.fromhex(digest)
        assert digest != receipt
        assert operation.expires_at == datetime.fromisoformat(data["expiresAt"])
        assert operation.job_id is not None
        assert await session.get(Job, operation.job_id) is not None
        persisted = (
            await session.scalars(
                text(
                    "SELECT row_to_json(o)::text FROM operations AS o "
                    "UNION ALL SELECT row_to_json(j)::text FROM jobs AS j"
                )
            )
        ).all()
        assert len(persisted) == 2
        assert all(receipt not in payload for payload in persisted)


async def test_receipt_survives_app_restart_and_requires_the_configured_protection_key(
    deletion_app: FastAPI,
    deletion_client: httpx.AsyncClient,
) -> None:
    accepted = await deletion_client.delete("/v1/users/me", headers=_ALICE_HEADERS)
    assert accepted.status_code == 202
    data = accepted.json()["data"]
    settings = deletion_app.state.settings
    rotated_settings = settings.model_copy(
        update={"deletion_receipt_secret": SecretStr("a-different-test-key-at-least-32-characters")}
    )

    for restart_settings, expected_status in ((settings, 200), (rotated_settings, 401)):
        restarted = create_app(
            restart_settings, credential_verifier=deletion_app.state.credential_verifier
        )
        async with (
            restarted.router.lifespan_context(restarted),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=restarted), base_url="http://test"
            ) as client,
        ):
            response = await client.get(
                data["statusUrl"],
                headers={"Authorization": f"DeletionReceipt {data['deletionReceipt']}"},
            )
            assert response.status_code == expected_status
            if expected_status == 200:
                assert response.json()["data"]["operationId"] == data["operationId"]
            else:
                assert response.json()["code"] == "invalid_deletion_receipt"


async def test_receipt_and_authorization_are_redacted_from_structured_logs(
    deletion_client: httpx.AsyncClient,
) -> None:
    buffer = StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        accepted = await deletion_client.delete(
            "/v1/users/me",
            headers={**_ALICE_HEADERS, "X-Request-Id": "req_delete_receipt_privacy"},
        )
        assert accepted.status_code == 202
        data = accepted.json()["data"]
        authorization = f"DeletionReceipt {data['deletionReceipt']}"
        status = await deletion_client.get(
            data["statusUrl"],
            headers={
                "Authorization": authorization,
                "X-Request-Id": "req_poll_receipt_privacy",
            },
        )
        assert status.status_code == 200
        logger.info(
            "deletion receipt log contract",
            extra={"extra_fields": {"Authorization": authorization, "response": data}},
        )
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)

    raw_logs = buffer.getvalue()
    assert data["deletionReceipt"] not in raw_logs
    assert "Bearer token-alice" not in raw_logs
    entries = [json.loads(line) for line in raw_logs.splitlines()]
    assert any(
        entry.get("request_id") == "req_delete_receipt_privacy" and entry.get("status_code") == 202
        for entry in entries
    )
    assert any(
        entry.get("request_id") == "req_poll_receipt_privacy" and entry.get("status_code") == 200
        for entry in entries
    )
    probe = next(entry for entry in entries if entry["message"] == "deletion receipt log contract")
    assert probe["Authorization"] == REDACTED_VALUE
    assert probe["response"]["deletionReceipt"] == REDACTED_VALUE
    assert probe["response"]["operationId"] == data["operationId"]
