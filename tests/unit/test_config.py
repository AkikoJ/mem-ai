"""Unit tests for application configuration and startup validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mem_ai.core.config import Settings


def test_default_settings_build_psycopg_url() -> None:
    settings = Settings(_env_file=None)
    url = settings.database_url()

    assert url.drivername == "postgresql+psycopg"
    assert url.host == "127.0.0.1"
    assert url.port == 5432
    assert url.database == "mem_ai"
    assert url.username == "mem_ai"
    assert settings.db_pool_size == 5
    assert settings.db_max_overflow == 5
    assert settings.db_pool_timeout_seconds == 10.0


def test_invalid_port_and_pool_limits_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, postgres_port=70000)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, db_pool_size=0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, db_statement_timeout_ms=0)


def test_production_rejects_default_postgres_password() -> None:
    with pytest.raises(ValidationError, match="postgres_password"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="change-this-local-password",
        )


def test_production_rejects_wildcard_cors_origin() -> None:
    with pytest.raises(ValidationError, match="cors_allowed_origins"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            cors_allowed_origins=["*"],
        )


def test_production_accepts_hardened_configuration() -> None:
    settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_host="postgres",
        postgres_password="strong-prod-password-123",
        cors_allowed_origins=["https://app.example.com"],
        auth_mode="jwt_hs256",
        auth_jwt_hs256_secret="super-secret-jwt-signing-key-at-least-32-chars",
        auth_trusted_issuers=["https://idp.example.com"],
        auth_trusted_audience="mem-ai-api",
    )

    assert settings.app_env == "production"
    assert settings.database_url().host == "postgres"
    assert settings.auth_mode == "jwt_hs256"


def test_production_rejects_development_auth_mode() -> None:
    with pytest.raises(ValidationError, match="auth_mode"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            auth_mode="development",
        )


def test_jwt_hs256_mode_requires_secret_issuer_and_audience() -> None:
    with pytest.raises(ValidationError, match="auth_jwt_hs256_secret"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="jwt_hs256",
            auth_trusted_issuers=["https://idp.example.com"],
            auth_trusted_audience="mem-ai-api",
        )

    with pytest.raises(ValidationError, match="auth_trusted_issuers"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="jwt_hs256",
            auth_jwt_hs256_secret="test-secret",
            auth_trusted_audience="mem-ai-api",
        )

    with pytest.raises(ValidationError, match="auth_trusted_audience"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="jwt_hs256",
            auth_jwt_hs256_secret="test-secret",
            auth_trusted_issuers=["https://idp.example.com"],
        )

    with pytest.raises(ValidationError, match="auth_jwt_hs256_secret"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            auth_mode="jwt_hs256",
            auth_jwt_hs256_secret="too-short",
            auth_trusted_issuers=["https://idp.example.com"],
            auth_trusted_audience="mem-ai-api",
        )


def test_trusted_gateway_mode_requires_explicit_proxies_issuers_and_secret() -> None:
    with pytest.raises(ValidationError, match="auth_trusted_proxies"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="trusted_gateway",
            auth_trusted_issuers=["https://gateway.example.com"],
            auth_gateway_shared_secret="gateway-secret",
        )

    with pytest.raises(ValidationError, match="auth_trusted_proxies"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="trusted_gateway",
            auth_trusted_proxies=["not-a-valid-ip"],
            auth_trusted_issuers=["https://gateway.example.com"],
            auth_gateway_shared_secret="gateway-secret",
        )

    with pytest.raises(ValidationError, match="auth_gateway_shared_secret"):
        Settings(
            _env_file=None,
            app_env="test",
            auth_mode="trusted_gateway",
            auth_trusted_proxies=["10.0.0.0/8"],
            auth_trusted_issuers=["https://gateway.example.com"],
        )

    for wildcard in ("0.0.0.0/0", "::/0"):
        with pytest.raises(ValidationError, match="wildcard CIDR"):
            Settings(
                _env_file=None,
                app_env="production",
                postgres_password="strong-prod-password-123",
                auth_mode="trusted_gateway",
                auth_trusted_proxies=[wildcard],
                auth_trusted_issuers=["https://gateway.example.com"],
                auth_gateway_shared_secret="gateway-shared-secret-value-at-least-32-chars",
            )

    settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_password="strong-prod-password-123",
        auth_mode="trusted_gateway",
        auth_trusted_proxies=["10.0.0.0/8", "127.0.0.1"],
        auth_trusted_issuers=["https://gateway.example.com"],
        auth_gateway_shared_secret="gateway-shared-secret-value-at-least-32-chars",
    )
    assert settings.auth_mode == "trusted_gateway"


def test_memos_memory_provider_mode_requires_api_key_and_valid_base_url() -> None:
    with pytest.raises(ValidationError, match="memos_api_key"):
        Settings(
            _env_file=None,
            app_env="test",
            memory_provider_mode="memos",
        )

    with pytest.raises(ValidationError, match="memos_base_url"):
        Settings(
            _env_file=None,
            app_env="test",
            memory_provider_mode="memos",
            memos_api_key="server-memos-key",
            memos_base_url="ftp://invalid.example.com",
        )

    valid = Settings(
        _env_file=None,
        app_env="test",
        memory_provider_mode="memos",
        memos_api_key="server-memos-key",
        memos_base_url="https://memos.memtensor.cn/api/openmem/v1",
    )
    assert valid.memory_provider_mode == "memos"
    assert valid.memos_api_key is not None
    assert valid.memos_api_key.get_secret_value() == "server-memos-key"


def test_openai_compatible_model_provider_mode_requires_api_key_and_valid_base_url() -> None:
    with pytest.raises(ValidationError, match="openai_compat_api_key"):
        Settings(
            _env_file=None,
            app_env="test",
            model_provider_mode="openai_compatible",
        )

    with pytest.raises(ValidationError, match="openai_compat_base_url"):
        Settings(
            _env_file=None,
            app_env="test",
            model_provider_mode="openai_compatible",
            openai_compat_api_key="server-model-key",
            openai_compat_base_url="ftp://invalid.example.com",
        )

    valid = Settings(
        _env_file=None,
        app_env="test",
        model_provider_mode="openai_compatible",
        openai_compat_api_key="server-model-key",
        openai_compat_base_url="https://llm.example.com/v1",
        model_registry_entries=[
            {"model_id": "gpt-4o-mini", "upstream_model": "vendor-gpt-4o-mini"}
        ],
    )
    assert valid.model_provider_mode == "openai_compatible"
    assert valid.openai_compat_api_key is not None
    assert valid.openai_compat_api_key.get_secret_value() == "server-model-key"

    with pytest.raises(ValidationError, match="model_registry_entries"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[{"model_id": "   "}],
        )

    with pytest.raises(ValidationError, match="duplicate model_id"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[
                {"model_id": "dup-model"},
                {"model_id": "dup-model"},
            ],
        )
