"""Unit tests for model seed validation, public projection, sync loader, and request schemas."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ServiceUnavailableError
from mem_ai.modules.conversations.schemas import ChatRequest
from mem_ai.modules.models.schemas import (
    ModelConfigRecord,
    ModelConfigSeed,
    ModelItemResponse,
)
from mem_ai.modules.models.sync import (
    load_model_seeds_from_file,
    load_model_seeds_from_settings,
)


def test_model_config_seed_strips_fields_and_rejects_blank_or_secret_keys() -> None:
    seed = ModelConfigSeed.model_validate(
        {
            "model_id": "  gpt-4o-mini  ",
            "provider_id": " openai_compatible ",
            "upstream_model": " vendor-gpt-4o-mini ",
            "display_name": " GPT-4o Mini ",
            "required_permission": " models:standard ",
            "credential_ref": " OPENAI_COMPAT_API_KEY ",
            "capabilities_json": {"json_mode": True},
        }
    )
    assert seed.model_id == "gpt-4o-mini"
    assert seed.provider_id == "openai_compatible"
    assert seed.resolved_upstream_model() == "vendor-gpt-4o-mini"
    assert seed.resolved_display_name() == "GPT-4o Mini"
    assert seed.required_permission == "models:standard"
    assert seed.credential_ref == "OPENAI_COMPAT_API_KEY"

    with pytest.raises(ValidationError):
        ModelConfigSeed(model_id="   ")

    with pytest.raises(ValidationError):
        ModelConfigSeed.model_validate({"model_id": "m1", "api_key": "sk-secret"})

    with pytest.raises(ValidationError):
        ModelConfigSeed.model_validate({"model_id": "m1", "base_url": "https://evil.example"})

    with pytest.raises(ValidationError):
        ModelConfigSeed.model_validate({"model_id": "m1", "supports_streaming": "yes"})


def test_model_item_response_exposes_only_public_identifiers_and_capabilities() -> None:
    now = datetime.now(UTC)
    record = ModelConfigRecord(
        id=uuid4(),
        model_id="gpt-4o-pro",
        provider_id="openai_compatible",
        upstream_model="internal-vendor-gpt-4o-2026",
        display_name="GPT-4o Pro",
        enabled=True,
        supports_streaming=True,
        max_context_tokens=128000,
        max_output_tokens=8192,
        required_permission="models:pro",
        credential_ref="SECRET_ENV_OPENAI_PRO_KEY",
        capabilities_json={"json_mode": True, "reasoning": True},
        created_at=now,
        updated_at=now,
    )

    payload = ModelItemResponse.from_record(record).model_dump(mode="json", by_alias=True)
    assert payload == {
        "modelId": "gpt-4o-pro",
        "displayName": "GPT-4o Pro",
        "supportsStreaming": True,
        "maxContextTokens": 128000,
        "maxOutputTokens": 8192,
        "capabilities": {"json_mode": True, "reasoning": True},
    }
    serialized = json.dumps(payload)
    for forbidden in (
        "provider_id",
        "providerId",
        "openai_compatible",
        "upstream_model",
        "upstreamModel",
        "internal-vendor-gpt-4o-2026",
        "credential_ref",
        "credentialRef",
        "SECRET_ENV_OPENAI_PRO_KEY",
        "required_permission",
        "requiredPermission",
        "models:pro",
    ):
        assert forbidden not in serialized


def test_chat_request_schema_rejects_client_provider_credentials_and_arbitrary_config() -> None:
    base = {"agentId": str(uuid4()), "message": "hello"}
    forbidden_fields: list[dict[str, object]] = [
        {"apiKey": "sk-user-key"},
        {"api_key": "sk-user-key"},
        {"baseUrl": "https://attacker.example/v1"},
        {"base_url": "https://attacker.example/v1"},
        {"provider": "openai"},
        {"providerId": "openai_compatible"},
        {"providerConfig": {"endpoint": "https://attacker.example"}},
        {"upstreamModel": "gpt-4o"},
        {"credentialRef": "OPENAI_COMPAT_API_KEY"},
    ]
    for extra in forbidden_fields:
        with pytest.raises(ValidationError):
            ChatRequest.model_validate({**base, **extra})


def test_sync_loader_reads_settings_and_json_file_and_errors_when_missing(tmp_path: Path) -> None:
    empty_settings = Settings(_env_file=None, app_env="test", model_registry_entries=[])
    with pytest.raises(ServiceUnavailableError) as missing_err:
        load_model_seeds_from_settings(empty_settings)
    assert missing_err.value.code == "model_configuration_missing"

    config_path = tmp_path / "models.json"
    config_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "gpt-4o-mini",
                        "upstream_model": "vendor-mini",
                        "display_name": "Mini",
                        "supports_streaming": True,
                    },
                    {
                        "model_id": "batch-only",
                        "supports_streaming": False,
                        "required_permission": "models:batch",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    seeds = load_model_seeds_from_file(config_path)
    assert [seed.model_id for seed in seeds] == ["gpt-4o-mini", "batch-only"]
    assert seeds[1].supports_streaming is False
    assert seeds[1].required_permission == "models:batch"

    empty_file = tmp_path / "empty.json"
    empty_file.write_text("[]", encoding="utf-8")
    with pytest.raises(ServiceUnavailableError) as empty_file_err:
        load_model_seeds_from_file(empty_file)
    assert empty_file_err.value.code == "model_configuration_missing"
