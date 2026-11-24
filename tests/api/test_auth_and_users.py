"""API tests for authentication context, GET /v1/users/me, header sanitization, and isolation."""

from __future__ import annotations

import json
import logging
import time
from io import StringIO
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi import Request
from pydantic import SecretStr

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.auth.adapters import (
    DevelopmentAuthAdapter,
    StaticTokenAuthAdapter,
)
from mem_ai.modules.auth.dependencies import AuthContextDep, get_credential_verifier
from mem_ai.modules.users import service as users_service
from tests.conftest import make_test_principal, sign_hs256_jwt


@pytest.mark.integration
@pytest.mark.anyio
async def test_users_me_with_jwt_returns_profile_binds_log_user_id_and_persists_across_restart(
    migrated_pg_settings: Settings,
) -> None:
    secret = "api-test-jwt-signing-secret-at-least-32-chars"
    settings = migrated_pg_settings.model_copy(
        update={
            "auth_mode": "jwt_hs256",
            "auth_jwt_hs256_secret": SecretStr(secret),
            "auth_trusted_issuers": ["https://idp.example.com"],
            "auth_trusted_audience": "mem-ai-api",
        }
    )
    now = int(time.time())
    valid_jwt = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "alice-sub",
            "aud": "mem-ai-api",
            "exp": now + 300,
            "actor_id": "idp:alice-sub",
            "permissions": ["users:self", "chat:use"],
        },
        secret=secret,
    )
    expired_jwt = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "alice-sub",
            "aud": "mem-ai-api",
            "exp": now - 120,
        },
        secret=secret,
    )

    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    app_1 = create_app(settings)
    try:
        async with app_1.router.lifespan_context(app_1):
            transport = httpx.ASGITransport(app=app_1)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                # Missing auth -> 401
                unauth = await client.get("/v1/users/me", headers={"X-Request-Id": "req_missing"})
                assert unauth.status_code == 401
                assert unauth.json()["code"] == "missing_credentials"
                assert unauth.json()["success"] is False

                # Expired JWT -> 401
                exp_resp = await client.get(
                    "/v1/users/me",
                    headers={"Authorization": f"Bearer {expired_jwt}"},
                )
                assert exp_resp.status_code == 401
                assert exp_resp.json()["code"] == "expired_credentials"

                # Valid JWT -> 200
                me_resp = await client.get(
                    "/v1/users/me",
                    headers={
                        "Authorization": f"Bearer {valid_jwt}",
                        "X-Request-Id": "req_me_alice",
                    },
                )
                assert me_resp.status_code == 200
                body = me_resp.json()
                assert body["success"] is True
                assert body["code"] == "ok"
                assert body["request_id"] == "req_me_alice"
                data = body["data"]
                alice_user_id = UUID(data["userId"])
                assert data["actorId"] == "idp:alice-sub"
                assert data["authMethod"] == "jwt_hs256"
                assert data["permissions"] == ["chat:use", "users:self"]
                assert data["status"] == "active"
                assert data["createdAt"]
    finally:
        logger.removeHandler(handler)

    # Structured access log must carry correlated user_id
    log_entries = [json.loads(line) for line in log_buffer.getvalue().splitlines() if line.strip()]
    assert any(
        entry.get("request_id") == "req_me_alice" and entry.get("user_id") == str(alice_user_id)
        for entry in log_entries
    )

    # Simulate app restart with a fresh FastAPI instance and verify same local userId is returned
    app_2 = create_app(settings)
    async with app_2.router.lifespan_context(app_2):
        transport_2 = httpx.ASGITransport(app=app_2)
        async with httpx.AsyncClient(transport=transport_2, base_url="http://test") as client_2:
            me_again = await client_2.get(
                "/v1/users/me",
                headers={"Authorization": f"Bearer {valid_jwt}"},
            )
            assert me_again.status_code == 200
            assert me_again.json()["data"]["userId"] == str(alice_user_id)


@pytest.mark.integration
@pytest.mark.anyio
async def test_disabled_and_deleting_users_return_403_without_recreating_user(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            ok_resp = await client.get(
                "/v1/users/me",
                headers={"Authorization": "Bearer token-alice"},
            )
            assert ok_resp.status_code == 200
            user_id = UUID(ok_resp.json()["data"]["userId"])

            # Disable user -> 403 user_disabled
            await users_service.set_user_status(
                app.state.session_factory,
                user_id=user_id,
                status="disabled",
            )
            disabled_resp = await client.get(
                "/v1/users/me",
                headers={"Authorization": "Bearer token-alice"},
            )
            assert disabled_resp.status_code == 403
            assert disabled_resp.json()["code"] == "user_disabled"

            # Mark deleting -> 403 user_deleting (must not silently recreate a new user)
            await users_service.set_user_status(
                app.state.session_factory,
                user_id=user_id,
                status="deleting",
            )
            deleting_resp = await client.get(
                "/v1/users/me",
                headers={"Authorization": "Bearer token-alice"},
            )
            assert deleting_resp.status_code == 403
            assert deleting_resp.json()["code"] == "user_deleting"


@pytest.mark.integration
@pytest.mark.anyio
async def test_untrusted_headers_query_params_and_body_cannot_override_identity(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)

    @app.post("/_test/identity-probe")
    async def probe_identity(request: Request, auth: AuthContextDep) -> dict[str, Any]:
        return {
            "resolved_user_id": str(auth.user_id),
            "resolved_actor_id": auth.actor_id,
            "saw_x_user_id": "x-user-id" in request.headers,
            "saw_x_auth_subject": "x-auth-subject" in request.headers,
        }

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_id = (
                await client.get("/v1/users/me", headers={"Authorization": "Bearer token-alice"})
            ).json()["data"]["userId"]
            bob_id = (
                await client.get("/v1/users/me", headers={"Authorization": "Bearer token-bob"})
            ).json()["data"]["userId"]
            assert alice_id != bob_id

            # Alice sends Bob's IDs in query params and spoofed headers to /v1/users/me
            spoofed_me = await client.get(
                "/v1/users/me",
                params={"user_id": bob_id, "userId": bob_id, "actor_id": "bob"},
                headers={
                    "Authorization": "Bearer token-alice",
                    "X-User-Id": bob_id,
                    "X-Auth-Subject": "bob",
                    "X-Auth-Issuer": "https://test.mem-ai.local",
                    "X-Auth-Actor-Id": "bob",
                },
            )
            assert spoofed_me.status_code == 200
            assert spoofed_me.json()["data"]["userId"] == alice_id
            assert spoofed_me.json()["data"]["actorId"] == "https://test.mem-ai.local:alice"

            # Alice sends Bob's ID in JSON body + forged headers to a POST endpoint
            probe = await client.post(
                "/_test/identity-probe",
                params={"user_id": bob_id},
                json={"userId": bob_id, "user_id": bob_id},
                headers={
                    "Authorization": "Bearer token-alice",
                    "X-User-Id": bob_id,
                    "X-Auth-Subject": "bob",
                },
            )
            assert probe.status_code == 200
            assert probe.json() == {
                "resolved_user_id": alice_id,
                "resolved_actor_id": "https://test.mem-ai.local:alice",
                "saw_x_user_id": False,
                "saw_x_auth_subject": False,
            }


@pytest.mark.integration
@pytest.mark.anyio
async def test_trusted_gateway_mode_enforces_proxy_boundary_and_strips_forged_headers(
    migrated_pg_settings: Settings,
) -> None:
    gateway_secret = "gateway-secret-for-api-test-boundary"
    settings = migrated_pg_settings.model_copy(
        update={
            "auth_mode": "trusted_gateway",
            "auth_trusted_proxies": ["10.0.0.0/8"],
            "auth_trusted_issuers": ["https://gateway.example.com"],
            "auth_gateway_shared_secret": SecretStr(gateway_secret),
        }
    )
    app = create_app(settings)

    @app.get("/_test/headers")
    async def inspect_headers(request: Request) -> dict[str, bool]:
        return {
            "has_auth_subject": "x-auth-subject" in request.headers,
            "has_gateway_auth": "x-gateway-auth" in request.headers,
            "has_x_user_id": "x-user-id" in request.headers,
        }

    async with app.router.lifespan_context(app):
        # 1. Request from trusted proxy with valid proof succeeds
        trusted_transport = httpx.ASGITransport(app=app, client=("10.2.3.4", 443))
        async with httpx.AsyncClient(
            transport=trusted_transport, base_url="http://test"
        ) as trusted_client:
            ok_resp = await trusted_client.get(
                "/v1/users/me",
                headers={
                    "X-Gateway-Auth": gateway_secret,
                    "X-Auth-Issuer": "https://gateway.example.com",
                    "X-Auth-Subject": "gw-alice",
                    "X-Auth-Permissions": "chat:use users:self",
                    "X-User-Id": "spoofed-non-gateway-header",
                },
            )
            assert ok_resp.status_code == 200
            assert ok_resp.json()["data"]["authMethod"] == "trusted_gateway"
            assert ok_resp.json()["data"]["actorId"] == "https://gateway.example.com:gw-alice"

            # Duplicate X-Auth-Subject headers from proxy are rejected with 401
            dup_resp = await trusted_client.get(
                "/v1/users/me",
                headers=[
                    ("X-Gateway-Auth", gateway_secret),
                    ("X-Auth-Issuer", "https://gateway.example.com"),
                    ("X-Auth-Subject", "spoofed-victim"),
                    ("X-Auth-Subject", "gw-alice"),
                ],
            )
            assert dup_resp.status_code == 401
            assert dup_resp.json()["code"] == "invalid_credentials"

            # Wrong gateway proof from trusted proxy still returns 401
            bad_secret = await trusted_client.get(
                "/v1/users/me",
                headers={
                    "X-Gateway-Auth": "wrong-secret",
                    "X-Auth-Issuer": "https://gateway.example.com",
                    "X-Auth-Subject": "gw-alice",
                },
            )
            assert bad_secret.status_code == 401
            assert bad_secret.json()["code"] == "untrusted_gateway_source"

        # 2. Request from untrusted peer with forged headers is rejected and headers are stripped
        untrusted_transport = httpx.ASGITransport(app=app, client=("203.0.113.99", 54321))
        async with httpx.AsyncClient(
            transport=untrusted_transport, base_url="http://test"
        ) as untrusted_client:
            forged = await untrusted_client.get(
                "/v1/users/me",
                headers={
                    "X-Forwarded-For": "10.2.3.4",
                    "X-Gateway-Auth": gateway_secret,
                    "X-Auth-Issuer": "https://gateway.example.com",
                    "X-Auth-Subject": "gw-alice",
                },
            )
            assert forged.status_code == 401
            assert forged.json()["code"] == "untrusted_gateway_source"

            stripped = await untrusted_client.get(
                "/_test/headers",
                headers={
                    "X-Gateway-Auth": gateway_secret,
                    "X-Auth-Subject": "gw-alice",
                    "X-User-Id": "spoofed",
                },
            )
            assert stripped.status_code == 200
            assert stripped.json() == {
                "has_auth_subject": False,
                "has_gateway_auth": False,
                "has_x_user_id": False,
            }


@pytest.mark.integration
@pytest.mark.anyio
async def test_credential_verifier_dependency_override_and_production_guard(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app = create_app(migrated_pg_settings)
    custom_principal = make_test_principal(
        "override-user",
        issuer="https://override.example.com",
        actor_id="override:user",
        auth_method="custom_test",
        permissions=("users:self",),
    )
    app.dependency_overrides[get_credential_verifier] = lambda: StaticTokenAuthAdapter(
        {"override-tok": custom_principal}
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/v1/users/me",
                headers={"Authorization": "Bearer override-tok"},
            )
            assert resp.status_code == 200
            assert resp.json()["data"]["actorId"] == "override:user"
            assert resp.json()["data"]["authMethod"] == "custom_test"

    # Production create_app must refuse explicitly injected development or test adapters
    prod_settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_password="strong-prod-password-123",
        allowed_hosts=["api.example.com"],
        auth_mode="jwt_hs256",
        auth_jwt_hs256_secret="super-secret-jwt-signing-key-at-least-32-chars",
        auth_trusted_issuers=["https://idp.example.com"],
        auth_trusted_audience="mem-ai-api",
    )
    with pytest.raises(ValueError, match="development"):
        create_app(
            prod_settings,
            credential_verifier=DevelopmentAuthAdapter(
                issuer="https://dev.mem-ai.local",
                subject="dev-user",
            ),
        )
    with pytest.raises(ValueError, match="development or test"):
        create_app(prod_settings, credential_verifier=test_auth_adapter)
