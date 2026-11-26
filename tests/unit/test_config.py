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
            auth_jwt_secret="strong-secret-key-at-least-32-chars",
        )


def test_production_rejects_wildcard_cors_origin() -> None:
    with pytest.raises(ValidationError, match="cors_allowed_origins"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            auth_jwt_secret="strong-secret-key-at-least-32-chars",
            cors_allowed_origins=["*"],
        )


def test_production_accepts_hardened_configuration() -> None:
    settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_host="postgres",
        postgres_password="strong-prod-password-123",
        allowed_hosts=["api.example.com"],
        cors_allowed_origins=["https://app.example.com"],
        auth_jwt_secret="super-secret-jwt-signing-key-at-least-32-chars",
    )

    assert settings.app_env == "production"
    assert settings.database_url().host == "postgres"


def test_trusted_proxy_networks_reject_invalid_and_production_wildcard_entries() -> None:
    with pytest.raises(ValidationError, match="auth_trusted_proxies"):
        Settings(_env_file=None, app_env="test", auth_trusted_proxies=["not-an-ip"])

    for wildcard in ("0.0.0.0/0", "::/0"):
        with pytest.raises(ValidationError, match="wildcard CIDR"):
            Settings(
                _env_file=None,
                app_env="production",
                postgres_password="strong-prod-password-123",
                auth_jwt_secret="strong-secret-key-at-least-32-chars",
                auth_trusted_proxies=[wildcard],
            )

    settings = Settings(
        _env_file=None, app_env="test", auth_trusted_proxies=["10.0.0.0/8", "127.0.0.1"]
    )
    assert [str(network) for network in settings.trusted_proxy_networks()] == [
        "10.0.0.0/8",
        "127.0.0.1/32",
    ]


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


def test_model_routing_policy_and_registry_entry_validation() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.model_forced_id is None
    assert defaults.model_allow_request_override is False
    assert defaults.model_sync_on_startup is True

    configured = Settings(
        _env_file=None,
        app_env="test",
        model_default_id="gpt-4o-mini",
        model_forced_id="gpt-4o-forced",
        model_allow_request_override=True,
        model_sync_on_startup=False,
        model_registry_entries=[
            {
                "model_id": "gpt-4o-mini",
                "provider_id": "openai_compatible",
                "upstream_model": "vendor-gpt-4o-mini",
                "display_name": "GPT-4o Mini",
                "enabled": True,
                "supports_streaming": True,
                "max_context_tokens": 128000,
                "max_output_tokens": 4096,
                "credential_ref": "OPENAI_COMPAT_API_KEY",
                "capabilities_json": {"json_mode": True},
            }
        ],
    )
    assert configured.model_forced_id == "gpt-4o-forced"
    assert configured.model_allow_request_override is True
    assert configured.model_sync_on_startup is False

    with pytest.raises(ValidationError, match="model_default_id"):
        Settings(_env_file=None, app_env="test", model_default_id="   ")

    with pytest.raises(ValidationError, match="model_forced_id"):
        Settings(_env_file=None, app_env="test", model_forced_id="   ")

    with pytest.raises(ValidationError, match="unsupported keys"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[
                {"model_id": "bad-entry", "api_key": "sk-never-here", "base_url": "https://x"}
            ],
        )

    with pytest.raises(ValidationError, match="unsupported keys"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[
                {"model_id": "legacy-perm", "required_permission": "models:pro"}
            ],
        )

    with pytest.raises(ValidationError, match="supports_streaming"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[
                {"model_id": "bad-bool", "supports_streaming": "yes"},
            ],
        )

    with pytest.raises(ValidationError, match="max_output_tokens"):
        Settings(
            _env_file=None,
            app_env="test",
            model_registry_entries=[
                {"model_id": "bad-int", "max_output_tokens": 0},
            ],
        )


def test_chat_lease_renew_interval_must_be_strictly_less_than_ttl() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.chat_lease_ttl_seconds == 60
    assert defaults.chat_lease_renew_interval_seconds == 20.0
    assert defaults.chat_lease_renew_interval_seconds < defaults.chat_lease_ttl_seconds

    with pytest.raises(ValidationError, match="chat_lease_renew_interval_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            chat_lease_ttl_seconds=10,
            chat_lease_renew_interval_seconds=10.0,
        )

    with pytest.raises(ValidationError, match="chat_lease_renew_interval_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            chat_lease_ttl_seconds=10,
            chat_lease_renew_interval_seconds=15.0,
        )


def test_chat_memory_recall_budget_settings_validation() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.chat_memory_recall_timeout_seconds == 5.0
    assert defaults.chat_memory_min_relevance == 0.5
    assert defaults.chat_memory_max_items == 10
    assert defaults.chat_memory_context_max_chars == 2000
    assert defaults.chat_prompt_max_chars == 24000

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", chat_memory_min_relevance=1.5)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", chat_memory_recall_timeout_seconds=0.0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", chat_prompt_max_chars=100)


def test_worker_and_job_retention_settings_validation() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.worker_loop_enabled is True
    assert defaults.worker_concurrency == 4
    assert defaults.worker_poll_interval_seconds == 1.0
    assert defaults.worker_claim_batch_size == 10
    assert defaults.worker_lease_ttl_seconds == 60
    assert defaults.worker_lease_renew_interval_seconds == 20.0
    assert defaults.worker_max_attempts == 5
    assert defaults.worker_backoff_base_seconds == 2.0
    assert defaults.worker_backoff_max_seconds == 300.0
    assert defaults.worker_backoff_jitter_ratio == 0.2
    assert defaults.worker_reconcile_delay_seconds == 10.0
    assert defaults.worker_shutdown_grace_seconds == 15.0
    assert defaults.job_attempt_retention_days == 30
    assert defaults.idempotency_retention_hours == 24

    with pytest.raises(ValidationError, match="worker_lease_renew_interval_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            worker_lease_ttl_seconds=10,
            worker_lease_renew_interval_seconds=10.0,
        )

    with pytest.raises(ValidationError, match="worker_backoff_base_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            worker_backoff_base_seconds=60.0,
            worker_backoff_max_seconds=30.0,
        )

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", job_attempt_retention_days=0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", idempotency_retention_hours=0)


def test_chat_stream_timeout_and_heartbeat_settings_validation() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.chat_stream_timeout_seconds == 120.0
    assert defaults.chat_stream_heartbeat_interval_seconds == 15.0

    custom = Settings(
        _env_file=None,
        app_env="test",
        chat_stream_timeout_seconds=0.5,
        chat_stream_heartbeat_interval_seconds=0.05,
    )
    assert custom.chat_stream_timeout_seconds == 0.5
    assert custom.chat_stream_heartbeat_interval_seconds == 0.05

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", chat_stream_timeout_seconds=0.0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", chat_stream_heartbeat_interval_seconds=0.0)

    with pytest.raises(ValidationError, match="chat_stream_heartbeat_interval_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            chat_stream_timeout_seconds=10.0,
            chat_stream_heartbeat_interval_seconds=10.0,
        )

    with pytest.raises(ValidationError, match="chat_stream_heartbeat_interval_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            chat_stream_timeout_seconds=10.0,
            chat_stream_heartbeat_interval_seconds=15.0,
        )


def test_memory_deletion_barrier_and_audit_archive_settings_validation() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.memory_inflight_settle_timeout == 60.0
    assert defaults.memory_clear_compensation_delay_seconds == 30.0
    assert defaults.deletion_audit_archive_path is None

    configured = Settings(
        _env_file=None,
        app_env="test",
        memory_inflight_settle_timeout=15.0,
        memory_clear_compensation_delay_seconds=5.0,
        deletion_audit_archive_path="/tmp/mem-ai-audit.jsonl",
    )
    assert configured.memory_inflight_settle_timeout == 15.0
    assert configured.memory_clear_compensation_delay_seconds == 5.0
    assert configured.deletion_audit_archive_path == "/tmp/mem-ai-audit.jsonl"

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", memory_inflight_settle_timeout=0.0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", memory_clear_compensation_delay_seconds=0.0)

    with pytest.raises(ValidationError, match="deletion_audit_archive_path"):
        Settings(_env_file=None, app_env="test", deletion_audit_archive_path="   ")


def test_data_retention_policy_settings_defaults_and_bounds() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.conversation_retention_policy == "indefinite"
    assert defaults.job_attempt_retention_days == 30
    assert defaults.idempotency_retention_hours == 24
    assert defaults.memory_list_snapshot_ttl_seconds == 600
    assert defaults.deletion_receipt_ttl_seconds == 2592000
    assert defaults.operation_summary_retention_days == 30
    assert defaults.retention_cleanup_interval_seconds == 900
    assert defaults.retention_batch_size == 100
    assert defaults.retention_max_batches_per_run == 50

    custom = Settings(
        _env_file=None,
        app_env="test",
        conversation_retention_policy="indefinite",
        operation_summary_retention_days=14,
        retention_cleanup_interval_seconds=60,
        retention_batch_size=25,
        retention_max_batches_per_run=10,
    )
    assert custom.conversation_retention_policy == "indefinite"
    assert custom.operation_summary_retention_days == 14
    assert custom.retention_cleanup_interval_seconds == 60
    assert custom.retention_batch_size == 25
    assert custom.retention_max_batches_per_run == 10

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", conversation_retention_policy="30d")  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", operation_summary_retention_days=0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", retention_cleanup_interval_seconds=5)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", retention_batch_size=0)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_env="test", retention_max_batches_per_run=0)


def test_worker_requires_an_online_connection_reserve_when_polling_is_enabled() -> None:
    with pytest.raises(ValidationError, match="worker_concurrency"):
        Settings(
            _env_file=None,
            db_pool_size=2,
            db_max_overflow=1,
            worker_concurrency=3,
            worker_loop_enabled=True,
        )

    enabled = Settings(
        _env_file=None,
        db_pool_size=3,
        db_max_overflow=1,
        worker_concurrency=3,
        worker_loop_enabled=True,
    )
    assert enabled.worker_loop_enabled is True

    paused = Settings(
        _env_file=None,
        db_pool_size=2,
        db_max_overflow=1,
        worker_concurrency=3,
        worker_loop_enabled=False,
    )
    assert paused.worker_loop_enabled is False


def test_resource_limits_observability_and_proxy_heartbeat_coordination_settings() -> None:
    defaults = Settings(_env_file=None, app_env="test")
    assert defaults.allowed_hosts == []
    assert defaults.max_request_body_bytes == 1_048_576
    assert defaults.max_concurrent_requests == 100
    assert defaults.max_concurrent_generations == 20
    assert defaults.rate_limit_retry_after_seconds == 5
    assert defaults.chat_message_max_chars == 32000
    assert defaults.proxy_read_timeout_seconds == 60.0
    assert defaults.metrics_enabled is True
    assert defaults.metrics_allowed_networks == ["127.0.0.1/32", "::1/128"]
    assert defaults.metrics_bearer_token is None
    assert defaults.worker_heartbeat_interval_seconds == 30.0
    assert defaults.worker_heartbeat_file is None
    assert defaults.effective_trust_request_id_from_proxies_only() is False

    # Heartbeat must be strictly shorter than proxy idle read timeout.
    with pytest.raises(ValidationError, match="proxy_read_timeout_seconds"):
        Settings(
            _env_file=None,
            app_env="test",
            chat_stream_heartbeat_interval_seconds=30.0,
            proxy_read_timeout_seconds=30.0,
        )

    # Invalid CIDR in metrics_allowed_networks rejected.
    with pytest.raises(ValidationError, match="metrics_allowed_networks"):
        Settings(
            _env_file=None,
            app_env="test",
            metrics_allowed_networks=["not-a-cidr"],
        )

    # Production requires explicit non-wildcard allowed_hosts and non-wildcard metrics networks.
    with pytest.raises(ValidationError, match="allowed_hosts"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            auth_jwt_secret="super-secret-jwt-signing-key-at-least-32-chars",
        )

    with pytest.raises(ValidationError, match="allowed_hosts"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            allowed_hosts=["*"],
            auth_jwt_secret="super-secret-jwt-signing-key-at-least-32-chars",
        )

    with pytest.raises(ValidationError, match="metrics_allowed_networks"):
        Settings(
            _env_file=None,
            app_env="production",
            postgres_password="strong-prod-password-123",
            allowed_hosts=["api.example.com"],
            metrics_allowed_networks=["0.0.0.0/0"],
            auth_jwt_secret="super-secret-jwt-signing-key-at-least-32-chars",
        )

    prod = Settings(
        _env_file=None,
        app_env="production",
        postgres_password="strong-prod-password-123",
        allowed_hosts=["api.example.com", "*.internal.example.com"],
        cors_allowed_origins=["https://app.example.com"],
        auth_jwt_secret="super-secret-jwt-signing-key-at-least-32-chars",
    )
    assert prod.effective_trust_request_id_from_proxies_only() is True
