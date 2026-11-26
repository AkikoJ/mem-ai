"""API/integration tests for built-in auth flows and login session retention."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.modules.auth.models import AuthSession
from mem_ai.modules.auth.tokens import hash_refresh_token
from mem_ai.modules.retention.schemas import RetentionPolicy
from mem_ai.modules.retention.service import run_retention_sweep
from mem_ai.modules.users import service as users_service
from mem_ai.modules.users.models import User

PASSWORD = "correct-horse-1"


async def _register(
    client: httpx.AsyncClient,
    email: str = "alice@example.com",
    password: str = PASSWORD,
) -> dict[str, Any]:
    response = await client.post("/v1/auth/register", json={"email": email, "password": password})
    assert response.status_code == 201, response.text
    data: dict[str, Any] = response.json()["data"]
    return data


async def _login(client: httpx.AsyncClient, email: str, password: str = PASSWORD) -> dict[str, Any]:
    response = await client.post("/v1/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    data: dict[str, Any] = response.json()["data"]
    return data


def _bearer(tokens: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['accessToken']}"}


async def _refresh(client: httpx.AsyncClient, refresh_token: str) -> httpx.Response:
    return await client.post("/v1/auth/refresh", json={"refreshToken": refresh_token})


@pytest.fixture
async def auth_app(
    migrated_pg_settings: Settings,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    app = create_app(migrated_pg_settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield app, client


@pytest.mark.integration
@pytest.mark.anyio
async def test_register_login_and_builtin_token_accesses_protected_endpoints(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    app, client = auth_app
    registered = await _register(client, "  Alice@Example.COM ")
    assert registered["email"] == "alice@example.com"
    assert set(registered["tokens"]) == {"accessToken", "refreshToken", "expiresIn"}

    # 凭据落库仅为哈希，永不保存明文。
    async with app.state.session_factory() as session:
        user = await session.scalar(select(User).where(User.email == "alice@example.com"))
        assert user is not None
        assert user.password_hash is not None and PASSWORD not in user.password_hash
        stored = await session.scalar(select(AuthSession.token_hash))
        assert stored == hash_refresh_token(registered["tokens"]["refreshToken"])

    login = await _login(client, "ALICE@example.com")
    for path in ("/v1/users/me", "/v1/agents", "/v1/conversations"):
        response = await client.get(path, headers=_bearer(login["tokens"]))
        assert response.status_code == 200, (path, response.text)
        if path == "/v1/users/me":
            profile = response.json()["data"]
            assert profile["userId"] == registered["userId"]
            assert profile["email"] == "alice@example.com"
            assert profile["status"] == "active"
            assert {"actorId", "authMethod", "permissions", "createdAt"} <= profile.keys()

    tampered = {"Authorization": f"Bearer {login['tokens']['accessToken']}x"}
    assert (await client.get("/v1/users/me", headers=tampered)).status_code == 401
    receipt = {"Authorization": "DeletionReceipt abc123"}
    assert (await client.get("/v1/users/me", headers=receipt)).status_code == 401


@pytest.mark.integration
@pytest.mark.anyio
async def test_user_deletion_revokes_all_sessions_and_reserves_normalized_email(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    app, client = auth_app
    registered = await _register(client, "Delete@Example.com")
    other = await _login(client, "delete@example.com")
    user_id = UUID(registered["userId"])

    deleted = await client.delete("/v1/users/me", headers=_bearer(registered["tokens"]))
    assert deleted.status_code == 202, deleted.text
    accepted = deleted.json()["data"]
    async with app.state.session_factory() as session:
        sessions = (
            await session.scalars(select(AuthSession).where(AuthSession.user_id == user_id))
        ).all()
        assert len(sessions) == 2
        assert all(row.revoked_at is not None for row in sessions)
        user = await session.get(User, user_id)
        assert user is not None and user.status == "deleting"

    for tokens in (registered["tokens"], other["tokens"]):
        denied = await client.get("/v1/users/me", headers=_bearer(tokens))
        assert denied.status_code == 403
        assert denied.json()["code"] == "user_deleting"
        assert (await _refresh(client, tokens["refreshToken"])).status_code == 401
    denied_login = await client.post(
        "/v1/auth/login", json={"email": "delete@example.com", "password": PASSWORD}
    )
    assert denied_login.status_code == 403
    conflict = await client.post(
        "/v1/auth/register", json={"email": "  DELETE@EXAMPLE.COM  ", "password": PASSWORD}
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "email_already_registered"

    receipt_headers = {"Authorization": f"DeletionReceipt {accepted['deletionReceipt']}"}
    progress = await client.get(accepted["statusUrl"], headers=receipt_headers)
    assert progress.status_code == 200
    assert progress.json()["data"]["operationId"] == accepted["operationId"]
    for path in ("/v1/users/me", "/v1/agents", "/v1/conversations", "/v1/models"):
        assert (await client.get(path, headers=receipt_headers)).status_code == 401


@pytest.mark.integration
@pytest.mark.anyio
async def test_register_validation_conflicts_and_disabled_registration(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
    migrated_pg_settings: Settings,
) -> None:
    _, client = auth_app
    await _register(client, "bob@example.com")

    dup = await client.post(
        "/v1/auth/register", json={"email": "BOB@example.com", "password": PASSWORD}
    )
    assert dup.status_code == 409
    assert dup.json()["code"] == "email_already_registered"

    weak = await client.post(
        "/v1/auth/register", json={"email": "c@example.com", "password": "12345678"}
    )
    assert weak.status_code == 422
    assert weak.json()["code"] == "weak_password"
    bad_email = await client.post(
        "/v1/auth/register", json={"email": "not-an-email", "password": PASSWORD}
    )
    assert bad_email.status_code == 422
    extra = await client.post(
        "/v1/auth/register",
        json={"email": "d@example.com", "password": PASSWORD, "role": "admin"},
    )
    assert extra.status_code == 422

    # 并发重复注册：一个 201，一个 409，绝不 500。
    results = await asyncio.gather(
        *(
            client.post(
                "/v1/auth/register", json={"email": "race@example.com", "password": PASSWORD}
            )
            for _ in range(2)
        )
    )
    assert sorted(r.status_code for r in results) == [201, 409]

    closed_app = create_app(
        migrated_pg_settings.model_copy(update={"auth_allow_self_registration": False})
    )
    async with closed_app.router.lifespan_context(closed_app):
        transport = httpx.ASGITransport(app=closed_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as closed:
            denied = await closed.post(
                "/v1/auth/register", json={"email": "e@example.com", "password": PASSWORD}
            )
            assert denied.status_code == 403
            assert denied.json()["code"] == "registration_disabled"


@pytest.mark.integration
@pytest.mark.anyio
async def test_login_is_enumeration_safe_and_rejects_non_active_users(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    app, client = auth_app
    registered = await _register(client, "carol@example.com")

    unknown = await client.post(
        "/v1/auth/login", json={"email": "nobody@example.com", "password": PASSWORD}
    )
    wrong = await client.post(
        "/v1/auth/login", json={"email": "carol@example.com", "password": "wrong-pass-9"}
    )
    assert unknown.status_code == wrong.status_code == 401
    assert unknown.json()["code"] == wrong.json()["code"] == "invalid_credentials"
    assert unknown.json()["message"] == wrong.json()["message"]

    user_id = UUID(registered["userId"])
    await users_service.set_user_status(
        app.state.session_factory, user_id=user_id, status="disabled"
    )
    disabled_login = await client.post(
        "/v1/auth/login", json={"email": "carol@example.com", "password": PASSWORD}
    )
    assert disabled_login.status_code == 403
    assert disabled_login.json()["code"] == "user_disabled"
    # 已签发的 Access Token 也立即失效。
    assert (
        await client.get("/v1/users/me", headers=_bearer(registered["tokens"]))
    ).status_code == 403

    await users_service.set_user_status(
        app.state.session_factory, user_id=user_id, status="deleting"
    )
    deleting_login = await client.post(
        "/v1/auth/login", json={"email": "carol@example.com", "password": PASSWORD}
    )
    assert deleting_login.status_code == 403
    assert deleting_login.json()["code"] == "user_deleting"
    reregister = await client.post(
        "/v1/auth/register", json={"email": "carol@example.com", "password": PASSWORD}
    )
    assert reregister.status_code == 409


@pytest.mark.integration
@pytest.mark.anyio
async def test_refresh_rotation_replay_revokes_family_and_keeps_other_devices(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    _, client = auth_app
    registered = await _register(client, "dave@example.com")
    r0 = registered["tokens"]["refreshToken"]
    device_b = await _login(client, "dave@example.com")

    first = await _refresh(client, r0)
    assert first.status_code == 200
    t1 = first.json()["data"]["tokens"]
    second = await _refresh(client, t1["refreshToken"])
    assert second.status_code == 200
    t2 = second.json()["data"]["tokens"]
    # 刷新后的 Access Token 保留身份并可访问业务接口。
    assert (await client.get("/v1/users/me", headers=_bearer(t2))).status_code == 200

    replay = await _refresh(client, r0)
    assert replay.status_code == 401
    assert replay.json()["code"] == "invalid_credentials"
    # 吊销已落库：整条轮换链上的活跃后继同样失效。
    assert (await _refresh(client, t2["refreshToken"])).status_code == 401
    # 其他设备的独立会话不受影响。
    assert (await _refresh(client, device_b["tokens"]["refreshToken"])).status_code == 200


@pytest.mark.integration
@pytest.mark.anyio
async def test_concurrent_refresh_allows_at_most_one_success(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    _, client = auth_app
    registered = await _register(client, "erin@example.com")
    token = registered["tokens"]["refreshToken"]
    results = await asyncio.gather(*(_refresh(client, token) for _ in range(3)))
    statuses = sorted(r.status_code for r in results)
    assert statuses.count(200) == 1
    assert all(code in (200, 401) for code in statuses)


@pytest.mark.integration
@pytest.mark.anyio
async def test_logout_single_all_and_validation(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    _, client = auth_app
    first = await _register(client, "frank@example.com")
    second = await _login(client, "frank@example.com")
    r_a = first["tokens"]["refreshToken"]
    r_b = second["tokens"]["refreshToken"]

    # 无 Authorization 头也可仅凭 refreshToken 注销，且只吊销该会话；重复注销幂等。
    assert (await client.post("/v1/auth/logout", json={"refreshToken": r_a})).status_code == 204
    assert (await client.post("/v1/auth/logout", json={"refreshToken": r_a})).status_code == 204
    assert (await _refresh(client, r_a)).status_code == 401
    refreshed_b = await _refresh(client, r_b)
    assert refreshed_b.status_code == 200

    assert (await client.post("/v1/auth/logout", json={})).status_code == 422
    assert (await client.post("/v1/auth/logout", json={"allSessions": True})).status_code == 401

    tokens_b = refreshed_b.json()["data"]["tokens"]
    all_out = await client.post(
        "/v1/auth/logout", json={"allSessions": True}, headers=_bearer(tokens_b)
    )
    assert all_out.status_code == 204
    assert (await _refresh(client, tokens_b["refreshToken"])).status_code == 401


@pytest.mark.integration
@pytest.mark.anyio
async def test_change_password_revokes_other_sessions_only(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    _, client = auth_app
    registered = await _register(client, "grace@example.com")
    other = await _login(client, "grace@example.com")
    headers = _bearer(registered["tokens"])

    wrong = await client.post(
        "/v1/auth/password",
        json={"currentPassword": "nope-nope-1", "newPassword": "brand-new-pass-2"},
        headers=headers,
    )
    assert wrong.status_code == 401
    weak = await client.post(
        "/v1/auth/password",
        json={"currentPassword": PASSWORD, "newPassword": "12345678"},
        headers=headers,
    )
    assert weak.status_code == 422

    ok = await client.post(
        "/v1/auth/password",
        json={"currentPassword": PASSWORD, "newPassword": "brand-new-pass-2"},
        headers=headers,
    )
    assert ok.status_code == 200
    assert ok.json()["data"]["revokedSessions"] == 1

    assert (await _refresh(client, other["tokens"]["refreshToken"])).status_code == 401
    assert (await _refresh(client, registered["tokens"]["refreshToken"])).status_code == 200
    old_login = await client.post(
        "/v1/auth/login", json={"email": "grace@example.com", "password": PASSWORD}
    )
    assert old_login.status_code == 401
    await _login(client, "grace@example.com", "brand-new-pass-2")


@pytest.mark.integration
@pytest.mark.anyio
async def test_retention_purges_expired_and_long_revoked_sessions_only(
    auth_app: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    app, client = auth_app
    registered = await _register(client, "heidi@example.com")
    user_id = UUID(registered["userId"])
    now = datetime.now(UTC)
    session_factory = app.state.session_factory

    async with session_factory.begin() as session:
        active = (await session.scalars(select(AuthSession.id))).one()
        rows = {
            "expired": AuthSession(
                user_id=user_id,
                token_hash="e" * 64,
                expires_at=now - timedelta(days=1),
            ),
            "expired2": AuthSession(
                user_id=user_id,
                token_hash="f" * 64,
                expires_at=now - timedelta(days=2),
            ),
            "long_revoked": AuthSession(
                user_id=user_id,
                token_hash="a" * 64,
                expires_at=now + timedelta(days=5),
                revoked_at=now - timedelta(days=200),
            ),
            "recent_revoked": AuthSession(
                user_id=user_id,
                token_hash="b" * 64,
                expires_at=now + timedelta(days=5),
                revoked_at=now - timedelta(days=1),
            ),
        }
        session.add_all(rows.values())
        await session.flush()
        keep = {active, rows["recent_revoked"].id}

    summary, _ = await run_retention_sweep(
        session_factory,
        policy=RetentionPolicy(
            batch_size=2, max_batches_per_run=50, auth_sessions_retention_days=90
        ),
    )
    assert summary.auth_sessions_deleted == 3

    async with session_factory() as session:
        remaining = set((await session.scalars(select(AuthSession.id))).all())
        total = await session.scalar(select(func.count(AuthSession.id)))
        # 记录最近吊销的会话保留，活跃会话仍可刷新。
        assert remaining == keep
        assert total == 2
    assert (await _refresh(client, registered["tokens"]["refreshToken"])).status_code == 200
