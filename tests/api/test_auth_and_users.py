"""API tests for built-in identity, current-user profiles, and forged identity isolation."""

from __future__ import annotations

import json
import logging
from io import StringIO
from typing import Any

import httpx
import pytest
from fastapi import Request

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.auth.tokens import create_access_token
from mem_ai.modules.users import service as users_service
from tests.conftest import BuiltinUsers, login_builtin_user, register_builtin_user


@pytest.mark.integration
@pytest.mark.anyio
async def test_users_me_returns_builtin_profile_correlates_logs_and_survives_restart(
    migrated_pg_settings: Settings,
) -> None:
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    original_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    app_1 = create_app(migrated_pg_settings)
    try:
        async with app_1.router.lifespan_context(app_1):
            transport = httpx.ASGITransport(app=app_1)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                registered = await register_builtin_user(client, email="Alice@Example.COM")
                alice = await login_builtin_user(client, email="alice@example.com")
                assert alice.user_id == registered.user_id

                unauth = await client.get("/v1/users/me", headers={"X-Request-Id": "req_missing"})
                assert unauth.status_code == 401
                assert unauth.json()["code"] == "missing_credentials"
                assert unauth.json()["success"] is False

                me_resp = await client.get(
                    "/v1/users/me",
                    headers={**alice.headers, "X-Request-Id": "req_me_alice"},
                )
                assert me_resp.status_code == 200
                body = me_resp.json()
                assert body["success"] is True
                assert body["code"] == "ok"
                assert body["request_id"] == "req_me_alice"
                data = body["data"]
                assert data["userId"] == str(alice.user_id)
                assert data["email"] == "alice@example.com"
                assert data["authMethod"] == "builtin_jwt"
                assert data["status"] == "active"
                assert data["createdAt"]
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)

    log_entries = [json.loads(line) for line in log_buffer.getvalue().splitlines() if line.strip()]
    assert any(
        entry.get("request_id") == "req_me_alice" and entry.get("user_id") == str(alice.user_id)
        for entry in log_entries
    )

    app_2 = create_app(migrated_pg_settings)
    async with app_2.router.lifespan_context(app_2):
        transport_2 = httpx.ASGITransport(app=app_2)
        async with httpx.AsyncClient(transport=transport_2, base_url="http://test") as client_2:
            me_again = await client_2.get("/v1/users/me", headers=alice.headers)
            assert me_again.status_code == 200
            assert me_again.json()["data"]["userId"] == str(alice.user_id)
            fresh_login = await login_builtin_user(client_2, email=alice.email)
            assert fresh_login.user_id == alice.user_id


@pytest.mark.integration
@pytest.mark.anyio
async def test_expired_malformed_tampered_and_duplicate_bearer_credentials_are_rejected(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    alice = builtin_users.alice
    expired = create_access_token(
        user_id=alice.user_id,
        email=alice.email,
        secret=migrated_pg_settings.auth_jwt_secret.get_secret_value(),
        expires_in_seconds=-120,
    )
    app = create_app(migrated_pg_settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        expired_response = await client.get(
            "/v1/users/me", headers={"Authorization": f"Bearer {expired}"}
        )
        assert expired_response.status_code == 401
        assert expired_response.json()["code"] == "expired_credentials"
        for invalid in ("not-a-jwt", "a.b.c", alice.access_token + "x"):
            response = await client.get(
                "/v1/users/me", headers={"Authorization": f"Bearer {invalid}"}
            )
            assert response.status_code == 401
            assert response.json()["code"] == "invalid_credentials"
        duplicate = await client.get(
            "/v1/users/me",
            headers=[
                ("Authorization", f"Bearer {alice.access_token}"),
                ("Authorization", f"Bearer {builtin_users.bob.access_token}"),
            ],
        )
        assert duplicate.status_code == 401
        assert duplicate.json()["code"] == "invalid_credentials"


@pytest.mark.integration
@pytest.mark.anyio
async def test_disabled_and_deleting_users_return_403_without_recreating_user(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
) -> None:
    app = create_app(migrated_pg_settings)
    alice = builtin_users.alice
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            ok_resp = await client.get("/v1/users/me", headers=alice.headers)
            assert ok_resp.status_code == 200
            assert ok_resp.json()["data"]["userId"] == str(alice.user_id)

            for status, error_code in (
                ("disabled", "user_disabled"),
                ("deleting", "user_deleting"),
            ):
                await users_service.set_user_status(
                    app.state.session_factory, user_id=alice.user_id, status=status
                )
                response = await client.get("/v1/users/me", headers=alice.headers)
                assert response.status_code == 403
                assert response.json()["code"] == error_code


@pytest.mark.integration
@pytest.mark.anyio
@pytest.mark.parametrize("client_host", ["10.2.3.4", "203.0.113.99"])
async def test_untrusted_headers_query_params_and_body_cannot_override_builtin_identity(
    migrated_pg_settings: Settings,
    builtin_users: BuiltinUsers,
    client_host: str,
) -> None:
    app = create_app(
        migrated_pg_settings.model_copy(update={"auth_trusted_proxies": ["10.0.0.0/8"]})
    )

    @app.post("/_test/identity-probe")
    async def probe_identity(request: Request, auth: AuthContextDep) -> dict[str, Any]:
        return {
            "resolved_user_id": str(auth.user_id),
            "resolved_email": auth.email,
            "saw_x_user_id": "x-user-id" in request.headers,
            "saw_x_auth_subject": "x-auth-subject" in request.headers,
            "saw_gateway_auth": "x-gateway-auth" in request.headers,
        }

    alice = builtin_users.alice
    bob = builtin_users.bob
    assert alice.user_id != bob.user_id
    forged_headers = {
        "X-Forwarded-For": "10.2.3.4",
        "X-User-Id": str(bob.user_id),
        "X-Auth-Subject": str(bob.user_id),
        "X-Auth-Issuer": "https://forged.example.com",
        "X-Auth-Actor-Id": bob.email,
        "X-Gateway-Auth": "forged-gateway-proof",
    }
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, client=(client_host, 443))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            unauthenticated = await client.get("/v1/users/me", headers=forged_headers)
            assert unauthenticated.status_code == 401
            assert unauthenticated.json()["code"] == "missing_credentials"

            spoofed_me = await client.get(
                "/v1/users/me",
                params={"user_id": str(bob.user_id), "userId": str(bob.user_id)},
                headers={**alice.headers, **forged_headers},
            )
            assert spoofed_me.status_code == 200
            assert spoofed_me.json()["data"]["userId"] == str(alice.user_id)
            assert spoofed_me.json()["data"]["email"] == alice.email

            probe = await client.post(
                "/_test/identity-probe",
                params={"user_id": str(bob.user_id)},
                json={"userId": str(bob.user_id), "user_id": str(bob.user_id)},
                headers={**alice.headers, **forged_headers},
            )
            assert probe.status_code == 200
            assert probe.json() == {
                "resolved_user_id": str(alice.user_id),
                "resolved_email": alice.email,
                "saw_x_user_id": False,
                "saw_x_auth_subject": False,
                "saw_gateway_auth": False,
            }
