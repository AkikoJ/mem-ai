"""Unit tests for authentication context and credential verification adapters."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.modules.auth.adapters import (
    AuthRequest,
    CredentialVerifier,
    DevelopmentAuthAdapter,
    JwtHs256AuthAdapter,
    StaticTokenAuthAdapter,
    TrustedGatewayAuthAdapter,
    build_credential_verifier,
)
from mem_ai.modules.auth.context import AuthenticationContext, VerifiedPrincipal
from tests.conftest import make_test_principal, sign_hs256_jwt


def test_authentication_context_exposes_required_fields_and_permissions() -> None:
    user_id = uuid4()
    ctx = AuthenticationContext(
        actor_id="https://idp.example.com:alice",
        user_id=user_id,
        auth_method="jwt_hs256",
        permissions=frozenset({"chat:use", "users:self"}),
    )

    assert ctx.actor_id == "https://idp.example.com:alice"
    assert ctx.user_id == user_id
    assert ctx.auth_method == "jwt_hs256"
    assert ctx.permissions == frozenset({"chat:use", "users:self"})
    assert ctx.has_permission("chat:use") is True
    assert ctx.has_permission("agents:manage") is False


@pytest.mark.anyio
async def test_development_adapter_returns_fixed_principal_and_ignores_spoofed_headers() -> None:
    adapter = DevelopmentAuthAdapter(
        issuer="https://dev.mem-ai.local",
        subject="dev-alice",
        permissions=("chat:use", "users:self"),
    )
    assert isinstance(adapter, CredentialVerifier)

    principal = await adapter.verify(
        AuthRequest(
            headers={
                "X-User-Id": str(uuid4()),
                "X-Auth-Subject": "victim-bob",
                "X-Auth-Issuer": "https://evil.example.com",
                "Authorization": "Bearer spoofed",
            },
            client_host="203.0.113.10",
        )
    )

    assert principal == VerifiedPrincipal(
        issuer="https://dev.mem-ai.local",
        subject="dev-alice",
        actor_id="https://dev.mem-ai.local:dev-alice",
        auth_method="development",
        permissions=frozenset({"chat:use", "users:self"}),
    )


@pytest.mark.anyio
async def test_static_token_adapter_verifies_known_tokens_and_rejects_unknown() -> None:
    alice = make_test_principal("alice", actor_id="actor:alice", permissions=("chat:use",))
    adapter = StaticTokenAuthAdapter({"token-alice": alice})

    resolved = await adapter.verify(AuthRequest(headers={"Authorization": "Bearer token-alice"}))
    assert resolved == alice

    with pytest.raises(UnauthorizedError) as missing_exc:
        await adapter.verify(AuthRequest(headers={}))
    assert missing_exc.value.code == "missing_credentials"

    with pytest.raises(UnauthorizedError) as invalid_exc:
        await adapter.verify(AuthRequest(headers={"Authorization": "Bearer wrong-token"}))
    assert invalid_exc.value.code == "invalid_credentials"


@pytest.mark.anyio
async def test_jwt_hs256_adapter_verifies_valid_claims_and_rejects_invalid_tokens() -> None:
    secret = "unit-test-jwt-signing-secret-at-least-32-chars"
    adapter = JwtHs256AuthAdapter(
        secret=secret,
        trusted_issuers=("https://idp.example.com",),
        trusted_audience="mem-ai-api",
        leeway_seconds=5,
    )
    now = int(time.time())

    valid_token = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-123",
            "aud": ["other-api", "mem-ai-api"],
            "exp": now + 300,
            "nbf": now - 10,
            "actor_id": "idp:user-123",
            "permissions": ["chat:use", "users:self"],
        },
        secret=secret,
    )
    principal = await adapter.verify(
        AuthRequest(headers={"Authorization": f"Bearer {valid_token}"})
    )
    assert principal == VerifiedPrincipal(
        issuer="https://idp.example.com",
        subject="user-123",
        actor_id="idp:user-123",
        auth_method="jwt_hs256",
        permissions=frozenset({"chat:use", "users:self"}),
    )

    # Explicit empty permissions list must NOT fall back to scope claim
    empty_perms_token = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-empty-perms",
            "aud": "mem-ai-api",
            "exp": now + 300,
            "permissions": [],
            "scope": "chat:use admin:all",
        },
        secret=secret,
    )
    empty_perms_principal = await adapter.verify(
        AuthRequest(headers={"Authorization": f"Bearer {empty_perms_token}"})
    )
    assert empty_perms_principal.permissions == frozenset()

    # Reject missing header
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={}))
    assert exc.value.code == "missing_credentials"

    # Reject duplicate Authorization headers
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(
            AuthRequest(
                headers=(
                    ("Authorization", f"Bearer {valid_token}"),
                    ("Authorization", f"Bearer {valid_token}"),
                )
            )
        )
    assert exc.value.code == "invalid_credentials"

    # Reject wrong signature
    wrong_sig = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-123",
            "aud": "mem-ai-api",
            "exp": now + 300,
        },
        secret="different-signing-secret-that-must-fail-check",
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {wrong_sig}"}))
    assert exc.value.code == "invalid_credentials"

    # Reject alg: none
    alg_none = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-123",
            "aud": "mem-ai-api",
            "exp": now + 300,
        },
        secret=secret,
        header={"alg": "none", "typ": "JWT"},
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {alg_none}"}))
    assert exc.value.code == "invalid_credentials"

    # Reject non-finite exp (NaN / Infinity)
    for bad_exp in (float("nan"), float("inf")):
        non_finite = sign_hs256_jwt(
            {
                "iss": "https://idp.example.com",
                "sub": "user-123",
                "aud": "mem-ai-api",
                "exp": bad_exp,
            },
            secret=secret,
        )
        with pytest.raises(UnauthorizedError) as exc:
            await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {non_finite}"}))
        assert exc.value.code == "invalid_credentials"

    # Reject expired token
    expired = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-123",
            "aud": "mem-ai-api",
            "exp": now - 60,
        },
        secret=secret,
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {expired}"}))
    assert exc.value.code == "expired_credentials"

    # Reject untrusted issuer
    bad_iss = sign_hs256_jwt(
        {
            "iss": "https://untrusted.example.com",
            "sub": "user-123",
            "aud": "mem-ai-api",
            "exp": now + 300,
        },
        secret=secret,
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {bad_iss}"}))
    assert exc.value.code == "invalid_issuer"

    # Reject wrong audience
    bad_aud = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "user-123",
            "aud": "wrong-api",
            "exp": now + 300,
        },
        secret=secret,
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {bad_aud}"}))
    assert exc.value.code == "invalid_audience"

    # Reject excessively long subject (> 255 chars) with 401 instead of DB 500
    long_sub = sign_hs256_jwt(
        {
            "iss": "https://idp.example.com",
            "sub": "x" * 256,
            "aud": "mem-ai-api",
            "exp": now + 300,
        },
        secret=secret,
    )
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(AuthRequest(headers={"Authorization": f"Bearer {long_sub}"}))
    assert exc.value.code == "invalid_credentials"


@pytest.mark.anyio
async def test_trusted_gateway_adapter_enforces_proxy_boundary_secret_and_issuer() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        auth_mode="trusted_gateway",
        auth_trusted_proxies=["10.0.0.0/8"],
        auth_trusted_issuers=["https://gateway.example.com"],
        auth_gateway_shared_secret="gateway-shared-secret-value-for-test",
    )
    adapter = build_credential_verifier(settings)
    assert isinstance(adapter, TrustedGatewayAuthAdapter)

    principal = await adapter.verify(
        AuthRequest(
            headers={
                "X-Gateway-Auth": "gateway-shared-secret-value-for-test",
                "X-Auth-Issuer": "https://gateway.example.com",
                "X-Auth-Subject": "gw-user-1",
                "X-Auth-Actor-Id": "gw:gw-user-1",
                "X-Auth-Permissions": "chat:use, users:self",
            },
            client_host="10.1.2.3",
        )
    )
    assert principal == VerifiedPrincipal(
        issuer="https://gateway.example.com",
        subject="gw-user-1",
        actor_id="gw:gw-user-1",
        auth_method="trusted_gateway",
        permissions=frozenset({"chat:use", "users:self"}),
    )

    # Duplicate gateway identity header must be rejected
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(
            AuthRequest(
                headers=(
                    ("X-Gateway-Auth", "gateway-shared-secret-value-for-test"),
                    ("X-Auth-Issuer", "https://gateway.example.com"),
                    ("X-Auth-Subject", "spoofed-victim"),
                    ("X-Auth-Subject", "gw-user-1"),
                ),
                client_host="10.1.2.3",
            )
        )
    assert exc.value.code == "invalid_credentials"

    # Untrusted direct peer is rejected even if X-Forwarded-For spoofs a trusted IP
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(
            AuthRequest(
                headers={
                    "X-Forwarded-For": "10.1.2.3",
                    "X-Gateway-Auth": "gateway-shared-secret-value-for-test",
                    "X-Auth-Issuer": "https://gateway.example.com",
                    "X-Auth-Subject": "gw-user-1",
                },
                client_host="203.0.113.55",
            )
        )
    assert exc.value.code == "untrusted_gateway_source"

    # Trusted proxy with wrong gateway secret is rejected
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(
            AuthRequest(
                headers={
                    "X-Gateway-Auth": "wrong-secret",
                    "X-Auth-Issuer": "https://gateway.example.com",
                    "X-Auth-Subject": "gw-user-1",
                },
                client_host="10.1.2.3",
            )
        )
    assert exc.value.code == "untrusted_gateway_source"

    # Trusted proxy with untrusted issuer is rejected
    with pytest.raises(UnauthorizedError) as exc:
        await adapter.verify(
            AuthRequest(
                headers={
                    "X-Gateway-Auth": "gateway-shared-secret-value-for-test",
                    "X-Auth-Issuer": "https://forged-issuer.example.com",
                    "X-Auth-Subject": "gw-user-1",
                },
                client_host="10.1.2.3",
            )
        )
    assert exc.value.code == "invalid_issuer"
