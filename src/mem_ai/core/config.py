"""Typed application settings and startup safety boundary validation."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

AppEnv = Literal["development", "test", "production"]
AuthMode = Literal["development", "jwt_hs256", "trusted_gateway"]
MemoryProviderMode = Literal["disabled", "memos"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
ProxyNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network

_INSECURE_DEFAULT_PASSWORDS = frozenset({"", "change-this-local-password", "postgres", "password"})
_MIN_PRODUCTION_SECRET_LENGTH = 32


@dataclass(frozen=True, slots=True)
class GatewayHeaders:
    """Configured header names accepted only from verified trusted gateway proxies."""

    secret: str = "X-Gateway-Auth"
    issuer: str = "X-Auth-Issuer"
    subject: str = "X-Auth-Subject"
    actor: str = "X-Auth-Actor-Id"
    permissions: str = "X-Auth-Permissions"

    def all_names(self) -> tuple[str, ...]:
        return (self.secret, self.issuer, self.subject, self.actor, self.permissions)


def is_ip_in_networks(client_host: str | None, networks: tuple[ProxyNetwork, ...]) -> bool:
    """Return True when `client_host` parses as an IP inside any configured CIDR."""
    if not client_host:
        return False
    try:
        addr = ipaddress.ip_address(client_host.strip())
    except ValueError:
        return False
    return any(addr in net for net in networks)


def _validate_required_secret(
    secret_value: SecretStr | None,
    *,
    field_name: str,
    mode_name: str,
    is_production: bool,
) -> None:
    secret = secret_value.get_secret_value().strip() if secret_value is not None else ""
    if not secret:
        raise ValueError(f"{field_name} must be set when auth_mode={mode_name!r}")
    if is_production and len(secret) < _MIN_PRODUCTION_SECRET_LENGTH:
        raise ValueError(
            f"{field_name} must be at least "
            f"{_MIN_PRODUCTION_SECRET_LENGTH} characters in production"
        )


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

    # Authentication
    auth_mode: AuthMode = "development"
    auth_dev_issuer: str = Field(default="https://dev.mem-ai.local", min_length=1)
    auth_dev_subject: str = Field(default="dev-user", min_length=1)
    auth_dev_actor_id: str | None = None
    auth_dev_permissions: list[str] = Field(
        default_factory=lambda: ["chat:use", "agents:manage", "memories:manage", "users:self"]
    )
    auth_trusted_issuers: list[str] = Field(default_factory=list)
    auth_trusted_audience: str | None = None
    auth_jwt_hs256_secret: SecretStr | None = None
    auth_jwt_leeway_seconds: int = Field(default=30, ge=0, le=300)
    auth_trusted_proxies: list[str] = Field(default_factory=list)
    auth_gateway_shared_secret: SecretStr | None = None
    auth_gateway_secret_header: str = Field(default="X-Gateway-Auth", min_length=1)
    auth_gateway_issuer_header: str = Field(default="X-Auth-Issuer", min_length=1)
    auth_gateway_subject_header: str = Field(default="X-Auth-Subject", min_length=1)
    auth_gateway_actor_header: str = Field(default="X-Auth-Actor-Id", min_length=1)
    auth_gateway_permissions_header: str = Field(default="X-Auth-Permissions", min_length=1)

    # Memory provider (MemOS Cloud)
    memory_provider_mode: MemoryProviderMode = "disabled"
    memos_base_url: str = Field(
        default="https://memos.memtensor.cn/api/openmem/v1",
        min_length=1,
    )
    memos_api_key: SecretStr | None = None
    memos_request_timeout_seconds: float = Field(default=15.0, gt=0.0)
    memos_list_page_size: int = Field(default=100, ge=1, le=200)
    memos_list_max_items: int = Field(default=1000, ge=1, le=5000)
    memory_list_snapshot_ttl_seconds: int = Field(default=600, ge=10, le=86400)

    @model_validator(mode="after")
    def validate_production_boundaries(self) -> Settings:
        password = self.postgres_password.get_secret_value().strip()
        if not password:
            raise ValueError("postgres_password must not be empty")

        is_production = self.app_env == "production"

        # Validate CIDR / IP syntax for any configured trusted proxy entries.
        parsed_proxies: list[ProxyNetwork] = []
        for raw_proxy in self.auth_trusted_proxies:
            try:
                network = ipaddress.ip_network(raw_proxy.strip(), strict=False)
            except ValueError as exc:
                raise ValueError(
                    f"auth_trusted_proxies contains invalid IP/CIDR entry: {raw_proxy!r}"
                ) from exc
            if is_production and network.prefixlen == 0:
                raise ValueError(
                    f"auth_trusted_proxies must not contain wildcard CIDR {raw_proxy!r} "
                    "in production"
                )
            parsed_proxies.append(network)

        trusted_issuers = [issuer.strip() for issuer in self.auth_trusted_issuers if issuer.strip()]

        if is_production:
            if password in _INSECURE_DEFAULT_PASSWORDS:
                raise ValueError(
                    "postgres_password must be set to a non-default secret in production"
                )
            if any(origin.strip() == "*" for origin in self.cors_allowed_origins):
                raise ValueError("cors_allowed_origins must not contain '*' in production")
            if self.auth_mode == "development":
                raise ValueError("auth_mode='development' is forbidden in production")

        if self.auth_mode == "jwt_hs256":
            _validate_required_secret(
                self.auth_jwt_hs256_secret,
                field_name="auth_jwt_hs256_secret",
                mode_name="jwt_hs256",
                is_production=is_production,
            )
            if not trusted_issuers:
                raise ValueError(
                    "auth_trusted_issuers must not be empty when auth_mode='jwt_hs256'"
                )
            if not (self.auth_trusted_audience and self.auth_trusted_audience.strip()):
                raise ValueError("auth_trusted_audience must be set when auth_mode='jwt_hs256'")

        if self.auth_mode == "trusted_gateway":
            if not parsed_proxies:
                raise ValueError(
                    "auth_trusted_proxies must not be empty when auth_mode='trusted_gateway'"
                )
            if not trusted_issuers:
                raise ValueError(
                    "auth_trusted_issuers must not be empty when auth_mode='trusted_gateway'"
                )
            _validate_required_secret(
                self.auth_gateway_shared_secret,
                field_name="auth_gateway_shared_secret",
                mode_name="trusted_gateway",
                is_production=is_production,
            )

        if self.memory_provider_mode == "memos":
            api_key = self.memos_api_key.get_secret_value().strip() if self.memos_api_key else ""
            if not api_key:
                raise ValueError("memos_api_key must be set when memory_provider_mode='memos'")
            if not self.memos_base_url.startswith(("http://", "https://")):
                raise ValueError("memos_base_url must start with 'http://' or 'https://'")

        return self

    def trusted_proxy_networks(self) -> tuple[ProxyNetwork, ...]:
        """Parse configured trusted proxy IPs and CIDRs."""
        return tuple(
            ipaddress.ip_network(entry.strip(), strict=False) for entry in self.auth_trusted_proxies
        )

    def gateway_headers(self) -> GatewayHeaders:
        """Return the configured trusted-gateway header bundle."""
        return GatewayHeaders(
            secret=self.auth_gateway_secret_header,
            issuer=self.auth_gateway_issuer_header,
            subject=self.auth_gateway_subject_header,
            actor=self.auth_gateway_actor_header,
            permissions=self.auth_gateway_permissions_header,
        )

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
