"""Unit tests for built-in auth primitives, settings validation and retention mapping."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import UnauthorizedError, UnprocessableEntityError
from mem_ai.modules.auth import tokens
from mem_ai.modules.retention.schemas import (
    RETENTION_STAGES,
    RetentionSweepSummary,
    retention_policy_from_settings,
)
from tests.conftest import sign_hs256_jwt

SECRET = "unit-test-builtin-jwt-secret-at-least-32-chars"


def test_normalize_email_trims_and_lowercases() -> None:
    assert tokens.normalize_email("  Alice@Example.COM ") == "alice@example.com"


@pytest.mark.parametrize(
    "password",
    ["short1!", "12345678", "aaaaaaaa", "        ", "a" * 129 + "1"],
)
def test_weak_passwords_raise_422(password: str) -> None:
    with pytest.raises(UnprocessableEntityError) as exc_info:
        tokens.validate_password_strength(password)
    assert exc_info.value.status_code == 422
    assert exc_info.value.code == "weak_password"


@pytest.mark.parametrize("password", ["password123", "Passw0rd!", "correct horse 1"])
def test_acceptable_passwords_pass(password: str) -> None:
    tokens.validate_password_strength(password)


def test_password_hash_is_salted_and_verifiable() -> None:
    first = tokens.hash_password("password123")
    second = tokens.hash_password("password123")
    assert first != second
    assert first.startswith("pbkdf2_sha256$")
    assert "password123" not in first
    assert tokens.verify_password("password123", first)
    assert not tokens.verify_password("password124", first)


@pytest.mark.parametrize("bad_hash", ["", "plain", "pbkdf2_sha256$x$y$z", "other$1$a$b"])
def test_verify_password_rejects_malformed_hash(bad_hash: str) -> None:
    assert tokens.verify_password("password123", bad_hash) is False


def test_refresh_token_is_high_entropy_and_hashed_with_sha256() -> None:
    first = tokens.generate_refresh_token()
    second = tokens.generate_refresh_token()
    assert first != second
    assert len(first) >= 43
    digest = tokens.hash_refresh_token(first)
    assert len(digest) == 64
    assert first not in digest


def test_access_token_round_trip_preserves_claims() -> None:
    user_id = uuid4()
    session_id = uuid4()
    token = tokens.create_access_token(
        user_id=user_id,
        email="a@example.com",
        secret=SECRET,
        expires_in_seconds=300,
        session_id=session_id,
    )
    claims = tokens.verify_access_token(token, secret=SECRET)
    assert claims.user_id == user_id
    assert claims.session_id == session_id
    assert claims.email == "a@example.com"
    assert claims.issuer == tokens.BUILTIN_JWT_ISSUER
    assert not hasattr(tokens, "try_peek_jwt_issuer")


def _payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "iss": tokens.BUILTIN_JWT_ISSUER,
        "typ": tokens.BUILTIN_JWT_TOKEN_TYPE,
        "sub": str(uuid4()),
        "email": "a@example.com",
        "exp": int(time.time()) + 300,
    }
    base.update(overrides)
    return base


def test_access_token_rejects_wrong_secret_alg_issuer_and_type() -> None:
    good = sign_hs256_jwt(_payload(), secret=SECRET)
    tokens.verify_access_token(good, secret=SECRET)

    with pytest.raises(UnauthorizedError):
        tokens.verify_access_token(good, secret="another-secret-another-secret-123456")
    with pytest.raises(UnauthorizedError):
        tokens.verify_access_token(
            sign_hs256_jwt(_payload(), secret=SECRET, header={"alg": "none", "typ": "JWT"}),
            secret=SECRET,
        )
    with pytest.raises(UnauthorizedError):
        tokens.verify_access_token(
            sign_hs256_jwt(_payload(iss="https://idp.example.com"), secret=SECRET),
            secret=SECRET,
        )
    with pytest.raises(UnauthorizedError):
        tokens.verify_access_token(
            sign_hs256_jwt(_payload(typ="refresh"), secret=SECRET),
            secret=SECRET,
        )


@pytest.mark.parametrize("exp", [None, True, "soon", float("nan"), float("inf")])
def test_access_token_rejects_invalid_exp(exp: object) -> None:
    payload = _payload()
    if exp is None:
        payload.pop("exp")
    else:
        payload["exp"] = exp
    with pytest.raises(UnauthorizedError):
        tokens.verify_access_token(sign_hs256_jwt(payload, secret=SECRET), secret=SECRET)


@pytest.mark.parametrize(
    "claim,value",
    [("sub", None), ("sub", "x" * 256), ("sid", "invalid-session"), ("email", 123)],
)
def test_access_token_rejects_malformed_identity_claims(claim: str, value: object) -> None:
    with pytest.raises(UnauthorizedError) as exc_info:
        tokens.verify_access_token(
            sign_hs256_jwt(_payload(**{claim: value}), secret=SECRET), secret=SECRET
        )
    assert exc_info.value.code == "invalid_credentials"


def test_access_token_expired_and_malformed() -> None:
    expired = sign_hs256_jwt(_payload(exp=int(time.time()) - 10), secret=SECRET)
    with pytest.raises(UnauthorizedError) as exc_info:
        tokens.verify_access_token(expired, secret=SECRET)
    assert exc_info.value.code == "expired_credentials"

    for bad in ("", "a.b", "a..c", "not-a-jwt"):
        with pytest.raises(UnauthorizedError):
            tokens.verify_access_token(bad, secret=SECRET)


def test_production_requires_strong_non_default_auth_jwt_secret() -> None:
    common = {
        "_env_file": None,
        "app_env": "production",
        "postgres_password": "strong-prod-password-123",
        "allowed_hosts": ["api.example.com"],
        "cors_allowed_origins": ["https://app.example.com"],
    }
    with pytest.raises(ValueError, match="auth_jwt_secret"):
        Settings(**common)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="auth_jwt_secret"):
        Settings(**common, auth_jwt_secret="too-short")  # type: ignore[arg-type]
    settings = Settings(
        **common,  # type: ignore[arg-type]
        auth_jwt_secret="a-sufficiently-long-builtin-jwt-secret-1234",
    )
    assert settings.auth_jwt_secret.get_secret_value().startswith("a-sufficiently")
    assert settings.deletion_receipt_signing_key() == (
        b"a-sufficiently-long-builtin-jwt-secret-1234"
    )
    assert not hasattr(settings, "auth_mode")
    assert not hasattr(settings, "auth_jwt_hs256_secret")
    assert not hasattr(settings, "auth_gateway_shared_secret")
    assert not hasattr(settings, "auth_trusted_issuers")
    assert not hasattr(settings, "auth_trusted_audience")


def test_auth_context_excludes_legacy_principal_actor_and_permissions() -> None:
    from mem_ai.modules.auth import context as auth_context

    assert not hasattr(auth_context, "VerifiedPrincipal")
    ctx = auth_context.AuthenticationContext(
        user_id=uuid4(),
        email="alice@example.com",
        auth_method="builtin_jwt",
    )
    assert not hasattr(ctx, "actor_id")
    assert not hasattr(ctx, "permissions")
    assert not hasattr(ctx, "has_permission")


def test_retention_policy_and_summary_include_auth_sessions() -> None:
    policy = retention_policy_from_settings(
        Settings(_env_file=None, app_env="test", auth_sessions_retention_days=3)
    )
    assert policy.auth_sessions_retention_days == 3
    assert RETENTION_STAGES[-1] == "auth_sessions"

    summary = RetentionSweepSummary(auth_sessions_deleted=4, job_attempts_deleted=1)
    payload = summary.to_result_summary()
    assert payload["auth_sessions_deleted"] == 4
    assert payload["total_deleted"] == 5
