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
    )

    assert settings.app_env == "production"
    assert settings.database_url().host == "postgres"
