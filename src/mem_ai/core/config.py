"""Typed application settings and startup safety boundary validation."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

AppEnv = Literal["development", "test", "production"]
AuthMode = Literal["development", "jwt_hs256", "trusted_gateway"]
MemoryProviderMode = Literal["disabled", "memos"]
ModelProviderMode = Literal["disabled", "openai_compatible"]
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


def _validate_http_provider_settings(
    *,
    api_key: SecretStr | None,
    base_url: str,
    api_key_field: str,
    base_url_field: str,
    mode_field: str,
    mode_value: str,
) -> None:
    secret = api_key.get_secret_value().strip() if api_key is not None else ""
    if not secret:
        raise ValueError(f"{api_key_field} must be set when {mode_field}={mode_value!r}")
    if not base_url.startswith(("http://", "https://")):
        raise ValueError(f"{base_url_field} must start with 'http://' or 'https://'")


_ALLOWED_MODEL_REGISTRY_KEYS = frozenset(
    {
        "model_id",
        "provider_id",
        "upstream_model",
        "display_name",
        "enabled",
        "supports_streaming",
        "max_context_tokens",
        "max_output_tokens",
        "required_permission",
        "credential_ref",
        "capabilities_json",
    }
)


def _validate_model_registry_entries(entries: list[dict[str, Any]]) -> None:
    seen_ids: set[str] = set()
    for idx, entry in enumerate(entries):
        unknown_keys = sorted(set(entry.keys()) - _ALLOWED_MODEL_REGISTRY_KEYS)
        if unknown_keys:
            raise ValueError(
                f"model_registry_entries[{idx}] contains unsupported keys: {unknown_keys}"
            )

        model_id = entry.get("model_id")
        if not isinstance(model_id, str) or not model_id.strip():
            raise ValueError(f"model_registry_entries[{idx}].model_id must be a non-empty string")
        normalized_id = model_id.strip()
        if normalized_id in seen_ids:
            raise ValueError(
                f"model_registry_entries contains duplicate model_id {normalized_id!r}"
            )
        seen_ids.add(normalized_id)

        for optional_str in (
            "provider_id",
            "upstream_model",
            "display_name",
            "required_permission",
            "credential_ref",
        ):
            value = entry.get(optional_str)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(
                    f"model_registry_entries[{idx}].{optional_str} must be a non-empty string"
                )

        for optional_bool in ("enabled", "supports_streaming"):
            value = entry.get(optional_bool)
            if value is not None and not isinstance(value, bool):
                raise ValueError(f"model_registry_entries[{idx}].{optional_bool} must be a boolean")

        for optional_int in ("max_context_tokens", "max_output_tokens"):
            value = entry.get(optional_int)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(
                    f"model_registry_entries[{idx}].{optional_int} must be a positive integer"
                )

        capabilities = entry.get("capabilities_json")
        if capabilities is not None and not isinstance(capabilities, dict):
            raise ValueError(
                f"model_registry_entries[{idx}].capabilities_json must be a JSON object"
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
    compatible_alembic_revisions: list[str] = Field(default_factory=list)

    # Outbound HTTP client
    http_connect_timeout_seconds: float = Field(default=5.0, gt=0.0)
    http_read_timeout_seconds: float = Field(default=30.0, gt=0.0)
    http_write_timeout_seconds: float = Field(default=10.0, gt=0.0)
    http_pool_timeout_seconds: float = Field(default=5.0, gt=0.0)
    http_max_connections: int = Field(default=100, ge=1)
    http_max_keepalive_connections: int = Field(default=20, ge=0)

    # Security / HTTP middleware & process resource budgets
    cors_allowed_origins: list[str] = Field(default_factory=list)
    allowed_hosts: list[str] = Field(default_factory=list)
    trusted_request_id_header: str = Field(default="X-Request-Id", min_length=1)
    trust_request_id_from_proxies_only: bool | None = None
    max_request_body_bytes: int = Field(default=1_048_576, ge=1024, le=100_000_000)
    max_concurrent_requests: int = Field(default=100, ge=1, le=10000)
    max_concurrent_generations: int = Field(default=20, ge=1, le=1000)
    rate_limit_retry_after_seconds: int = Field(default=5, ge=1, le=3600)
    proxy_read_timeout_seconds: float = Field(default=60.0, gt=0.0, le=3600.0)

    # Observability & controlled metrics endpoint
    metrics_enabled: bool = True
    metrics_allowed_networks: list[str] = Field(default_factory=lambda: ["127.0.0.1/32", "::1/128"])
    metrics_bearer_token: SecretStr | None = None

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

    # Account deletion receipts; HMAC keys must remain stable throughout receipt retention.
    deletion_receipt_secret: SecretStr | None = None
    deletion_receipt_ttl_seconds: int = Field(default=2592000, ge=60, le=31536000)

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
    memory_inflight_settle_timeout: float = Field(default=60.0, gt=0.0, le=3600.0)
    memory_clear_compensation_delay_seconds: float = Field(default=30.0, gt=0.0, le=3600.0)
    deletion_audit_archive_path: str | None = None

    # Model provider (OpenAI-compatible) and chat generation budgets
    model_provider_mode: ModelProviderMode = "disabled"
    openai_compat_base_url: str = Field(default="https://api.openai.com/v1", min_length=1)
    openai_compat_api_key: SecretStr | None = None
    openai_compat_request_timeout_seconds: float = Field(default=60.0, gt=0.0)
    model_default_id: str | None = None
    model_forced_id: str | None = None
    model_allow_request_override: bool = False
    model_sync_on_startup: bool = True
    model_registry_entries: list[dict[str, Any]] = Field(default_factory=list)
    chat_generation_timeout_seconds: float = Field(default=60.0, gt=0.0)
    chat_stream_timeout_seconds: float = Field(default=120.0, gt=0.0, le=3600.0)
    chat_stream_heartbeat_interval_seconds: float = Field(default=15.0, gt=0.0, le=300.0)
    chat_history_message_limit: int = Field(default=20, ge=0, le=200)
    chat_lease_ttl_seconds: int = Field(default=60, ge=5, le=600)
    chat_lease_renew_interval_seconds: float = Field(default=20.0, gt=0.0, le=300.0)
    chat_memory_recall_timeout_seconds: float = Field(default=5.0, gt=0.0, le=60.0)
    chat_memory_min_relevance: float = Field(default=0.5, ge=0.0, le=1.0)
    chat_memory_max_items: int = Field(default=10, ge=0, le=50)
    chat_memory_context_max_chars: int = Field(default=2000, ge=0, le=32000)
    chat_prompt_max_chars: int = Field(default=24000, ge=256, le=200000)
    chat_message_max_chars: int = Field(default=32000, ge=1, le=200000)

    # Background worker and durable job budgets
    worker_loop_enabled: bool = True
    worker_concurrency: int = Field(default=4, ge=1, le=100)
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0.0, le=60.0)
    worker_claim_batch_size: int = Field(default=10, ge=1, le=200)
    worker_lease_ttl_seconds: int = Field(default=60, ge=5, le=600)
    worker_lease_renew_interval_seconds: float = Field(default=20.0, gt=0.0, le=300.0)
    worker_max_attempts: int = Field(default=5, ge=1, le=50)
    worker_backoff_base_seconds: float = Field(default=2.0, gt=0.0, le=300.0)
    worker_backoff_max_seconds: float = Field(default=300.0, gt=0.0, le=86400.0)
    worker_backoff_jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)
    worker_reconcile_delay_seconds: float = Field(default=10.0, gt=0.0, le=3600.0)
    worker_shutdown_grace_seconds: float = Field(default=15.0, gt=0.0, le=300.0)
    worker_heartbeat_interval_seconds: float = Field(default=30.0, gt=0.0, le=300.0)
    worker_heartbeat_file: str | None = None
    worker_metrics_host: str = Field(default="127.0.0.1", min_length=1)
    worker_metrics_port: int | None = Field(default=None, ge=1, le=65535)
    conversation_retention_policy: Literal["indefinite"] = "indefinite"
    job_attempt_retention_days: int = Field(default=30, ge=1, le=365)
    idempotency_retention_hours: int = Field(default=24, ge=1, le=720)
    operation_summary_retention_days: int = Field(default=30, ge=1, le=365)
    retention_cleanup_interval_seconds: int = Field(default=900, ge=10, le=86400)
    retention_batch_size: int = Field(default=100, ge=1, le=500)
    retention_max_batches_per_run: int = Field(default=50, ge=1, le=1000)

    @model_validator(mode="after")
    def validate_production_boundaries(self) -> Settings:
        password = self.postgres_password.get_secret_value().strip()
        if not password:
            raise ValueError("postgres_password must not be empty")

        is_production = self.app_env == "production"

        if self.deletion_receipt_secret is not None:
            receipt_secret = self.deletion_receipt_secret.get_secret_value().strip()
            if not receipt_secret:
                raise ValueError("deletion_receipt_secret must not be blank when set")
            if is_production and len(receipt_secret) < _MIN_PRODUCTION_SECRET_LENGTH:
                raise ValueError(
                    "deletion_receipt_secret must be at least 32 characters in production"
                )

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

        # Validate CIDR / IP syntax for any configured metrics network allowlist entries.
        parsed_metrics_nets: list[ProxyNetwork] = []
        for raw_net in self.metrics_allowed_networks:
            try:
                net = ipaddress.ip_network(raw_net.strip(), strict=False)
            except ValueError as exc:
                raise ValueError(
                    f"metrics_allowed_networks contains invalid IP/CIDR entry: {raw_net!r}"
                ) from exc
            if is_production and net.prefixlen == 0:
                raise ValueError(
                    f"metrics_allowed_networks must not contain wildcard CIDR {raw_net!r} "
                    "in production"
                )
            parsed_metrics_nets.append(net)

        if self.metrics_bearer_token is not None:
            metrics_token = self.metrics_bearer_token.get_secret_value().strip()
            if not metrics_token:
                raise ValueError("metrics_bearer_token must not be blank when set")
            if is_production and len(metrics_token) < _MIN_PRODUCTION_SECRET_LENGTH:
                raise ValueError(
                    f"metrics_bearer_token must be at least "
                    f"{_MIN_PRODUCTION_SECRET_LENGTH} characters in production"
                )

        normalized_hosts = [host.strip().lower() for host in self.allowed_hosts]
        for host in normalized_hosts:
            if not host or any(ch.isspace() for ch in host) or "/" in host:
                raise ValueError(f"allowed_hosts contains invalid host entry: {host!r}")

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
            if not normalized_hosts:
                raise ValueError("allowed_hosts must not be empty in production")
            if any(host == "*" for host in normalized_hosts):
                raise ValueError("allowed_hosts must not contain '*' in production")
            if (
                self.metrics_enabled
                and not parsed_metrics_nets
                and self.metrics_bearer_token is None
            ):
                raise ValueError(
                    "metrics_allowed_networks or metrics_bearer_token must be configured "
                    "when metrics_enabled=True in production"
                )

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
            _validate_http_provider_settings(
                api_key=self.memos_api_key,
                base_url=self.memos_base_url,
                api_key_field="memos_api_key",
                base_url_field="memos_base_url",
                mode_field="memory_provider_mode",
                mode_value="memos",
            )

        if self.model_provider_mode == "openai_compatible":
            _validate_http_provider_settings(
                api_key=self.openai_compat_api_key,
                base_url=self.openai_compat_base_url,
                api_key_field="openai_compat_api_key",
                base_url_field="openai_compat_base_url",
                mode_field="model_provider_mode",
                mode_value="openai_compatible",
            )

        if self.model_default_id is not None and not self.model_default_id.strip():
            raise ValueError("model_default_id must not be blank when set")
        if self.model_forced_id is not None and not self.model_forced_id.strip():
            raise ValueError("model_forced_id must not be blank when set")
        if (
            self.deletion_audit_archive_path is not None
            and not self.deletion_audit_archive_path.strip()
        ):
            raise ValueError("deletion_audit_archive_path must not be blank when set")
        if self.worker_heartbeat_file is not None and not self.worker_heartbeat_file.strip():
            raise ValueError("worker_heartbeat_file must not be blank when set")

        if self.chat_lease_renew_interval_seconds >= self.chat_lease_ttl_seconds:
            raise ValueError(
                "chat_lease_renew_interval_seconds must be strictly less than "
                "chat_lease_ttl_seconds"
            )

        if self.chat_stream_heartbeat_interval_seconds >= self.chat_stream_timeout_seconds:
            raise ValueError(
                "chat_stream_heartbeat_interval_seconds must be strictly less than "
                "chat_stream_timeout_seconds"
            )

        if self.chat_stream_heartbeat_interval_seconds >= self.proxy_read_timeout_seconds:
            raise ValueError(
                "chat_stream_heartbeat_interval_seconds must be strictly less than "
                "proxy_read_timeout_seconds"
            )

        if self.worker_lease_renew_interval_seconds >= self.worker_lease_ttl_seconds:
            raise ValueError(
                "worker_lease_renew_interval_seconds must be strictly less than "
                "worker_lease_ttl_seconds"
            )

        if (
            self.worker_loop_enabled
            and self.db_pool_size + self.db_max_overflow < self.worker_concurrency + 1
        ):
            raise ValueError(
                "db_pool_size + db_max_overflow must be at least "
                "worker_concurrency + 1 when worker_loop_enabled is true to reserve a "
                "database connection for online requests"
            )

        if self.worker_backoff_base_seconds > self.worker_backoff_max_seconds:
            raise ValueError(
                "worker_backoff_base_seconds must be less than or equal to "
                "worker_backoff_max_seconds"
            )

        _validate_model_registry_entries(self.model_registry_entries)

        return self

    def trusted_proxy_networks(self) -> tuple[ProxyNetwork, ...]:
        """Parse configured trusted proxy IPs and CIDRs."""
        return tuple(
            ipaddress.ip_network(entry.strip(), strict=False) for entry in self.auth_trusted_proxies
        )

    def metrics_allowed_proxy_networks(self) -> tuple[ProxyNetwork, ...]:
        """Parse configured IP/CIDR allowlist guarding `/metrics` scraping."""
        return tuple(
            ipaddress.ip_network(entry.strip(), strict=False)
            for entry in self.metrics_allowed_networks
        )

    def effective_trust_request_id_from_proxies_only(self) -> bool:
        """Trust inbound `X-Request-Id` only from configured proxies in production by default."""
        if self.trust_request_id_from_proxies_only is not None:
            return self.trust_request_id_from_proxies_only
        return self.app_env == "production"

    def deletion_receipt_signing_key(self) -> bytes:
        """Use an independent key when configured, otherwise the active stable server secret."""
        secret = self.deletion_receipt_secret
        if secret is None and self.auth_mode == "jwt_hs256":
            secret = self.auth_jwt_hs256_secret
        if secret is None and self.auth_mode == "trusted_gateway":
            secret = self.auth_gateway_shared_secret
        if secret is None:
            secret = self.postgres_password
        return secret.get_secret_value().encode("utf-8")

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
