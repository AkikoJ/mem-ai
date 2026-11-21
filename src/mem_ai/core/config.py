"""Typed application settings and startup safety boundary validation."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

AppEnv = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

_INSECURE_DEFAULT_PASSWORDS = frozenset({"", "change-this-local-password", "postgres", "password"})


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables and `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    app_env: AppEnv = "development"
    app_name: str = "mem-ai"
    app_version: str = "0.1.0"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_log_level: LogLevel = "INFO"

    # PostgreSQL
    postgres_host: str = Field(default="127.0.0.1", min_length=1)
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str = Field(default="mem_ai", min_length=1)
    postgres_user: str = Field(default="mem_ai", min_length=1)
    postgres_password: SecretStr = Field(default=SecretStr("change-this-local-password"))

    # Connection pool and timeouts
    db_pool_size: int = Field(default=5, ge=1, le=100)
    db_max_overflow: int = Field(default=5, ge=0, le=100)
    db_pool_timeout_seconds: float = Field(default=10.0, gt=0.0)
    db_connect_timeout_seconds: float = Field(default=5.0, gt=0.0)
    db_statement_timeout_ms: int = Field(default=10000, ge=100)
    db_health_check_timeout_seconds: float = Field(default=2.0, gt=0.0)
    expected_alembic_revision: str | None = None

    # Outbound HTTP client
    http_connect_timeout_seconds: float = Field(default=5.0, gt=0.0)
    http_read_timeout_seconds: float = Field(default=30.0, gt=0.0)
    http_write_timeout_seconds: float = Field(default=10.0, gt=0.0)
    http_pool_timeout_seconds: float = Field(default=5.0, gt=0.0)
    http_max_connections: int = Field(default=100, ge=1)
    http_max_keepalive_connections: int = Field(default=20, ge=0)

    # Security / HTTP middleware
    cors_allowed_origins: list[str] = Field(default_factory=list)
    trusted_request_id_header: str = Field(default="X-Request-Id", min_length=1)

    @model_validator(mode="after")
    def validate_production_boundaries(self) -> Settings:
        password = self.postgres_password.get_secret_value().strip()
        if not password:
            raise ValueError("postgres_password must not be empty")
        if self.app_env == "production":
            if password in _INSECURE_DEFAULT_PASSWORDS:
                raise ValueError(
                    "postgres_password must be set to a non-default secret in production"
                )
            if any(origin.strip() == "*" for origin in self.cors_allowed_origins):
                raise ValueError("cors_allowed_origins must not contain '*' in production")
        return self

    def database_url(self) -> URL:
        """Build the PostgreSQL URL using the psycopg 3 driver."""
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )
