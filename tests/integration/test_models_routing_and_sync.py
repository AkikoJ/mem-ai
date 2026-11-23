"""PostgreSQL integration tests for model config sync, discovery, and routing policy."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from mem_ai.contracts.persona_provider import MemoryPolicy, PersonaSnapshot
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    ConflictError,
    ForbiddenError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.conversations import generation as generation_store
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.service import ChatOptions
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelRoutingPolicy
from mem_ai.modules.models.sync import main as sync_cli_main
from mem_ai.modules.models.sync import run_sync_models
from mem_ai.modules.users import service as users_service


@pytest.mark.integration
def test_explicit_model_sync_cli_command_handles_missing_and_valid_config(
    migrated_pg_settings: Settings,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    empty_settings = migrated_pg_settings.model_copy(update={"model_registry_entries": []})
    assert sync_cli_main([], settings=empty_settings) == 1
    captured = capsys.readouterr()
    assert "model_configuration_missing" in captured.err

    config_file = tmp_path / "cli_models.json"
    config_file.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "cli-synced-model",
                        "upstream_model": "vendor-cli",
                        "display_name": "CLI Synced",
                        "enabled": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert sync_cli_main(["--config-file", str(config_file)], settings=migrated_pg_settings) == 0
    # Running the CLI command a second time is idempotent and exits 0.
    assert sync_cli_main(["--config-file", str(config_file)], settings=migrated_pg_settings) == 0


@pytest.mark.integration
@pytest.mark.anyio
async def test_explicit_model_sync_command_is_idempotent_and_rejects_missing_config(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    # Missing configuration fails cleanly with stable error code.
    empty_settings = migrated_pg_settings.model_copy(update={"model_registry_entries": []})
    with pytest.raises(ServiceUnavailableError) as missing_err:
        await run_sync_models(empty_settings)
    assert missing_err.value.code == "model_configuration_missing"

    # Sync from JSON file twice (second run updates capabilities and disables one model).
    config_file = tmp_path / "models.json"
    config_file.write_text(
        json.dumps(
            [
                {
                    "model_id": "gpt-4o-mini",
                    "provider_id": "openai_compatible",
                    "upstream_model": "vendor-mini-v1",
                    "display_name": "Mini v1",
                    "enabled": True,
                    "supports_streaming": True,
                    "max_context_tokens": 64000,
                    "max_output_tokens": 4096,
                    "credential_ref": "OPENAI_COMPAT_API_KEY",
                    "capabilities_json": {"json_mode": True},
                },
                {
                    "model_id": "legacy-model",
                    "upstream_model": "vendor-legacy",
                    "display_name": "Legacy",
                    "enabled": True,
                    "supports_streaming": False,
                },
            ]
        ),
        encoding="utf-8",
    )

    first = await run_sync_models(migrated_pg_settings, config_file=config_file)
    assert [row.model_id for row in first] == ["gpt-4o-mini", "legacy-model"]
    assert first[0].upstream_model == "vendor-mini-v1"
    assert first[1].enabled is True

    # Idempotent re-sync updates existing rows in place without duplicating model_id.
    config_file.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "model_id": "gpt-4o-mini",
                        "provider_id": "openai_compatible",
                        "upstream_model": "vendor-mini-v2",
                        "display_name": "Mini v2",
                        "enabled": True,
                        "supports_streaming": True,
                        "max_context_tokens": 128000,
                        "max_output_tokens": 8192,
                        "credential_ref": "OPENAI_COMPAT_API_KEY_V2",
                        "capabilities_json": {"json_mode": True, "reasoning": True},
                    },
                    {
                        "model_id": "legacy-model",
                        "upstream_model": "vendor-legacy",
                        "display_name": "Legacy",
                        "enabled": False,
                        "supports_streaming": False,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    second = await run_sync_models(migrated_pg_settings, config_file=config_file)
    assert [row.model_id for row in second] == ["gpt-4o-mini", "legacy-model"]
    assert second[0].upstream_model == "vendor-mini-v2"
    assert second[1].enabled is False

    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        available = await models_service.list_available_models(session_factory)
        assert [row.model_id for row in available] == ["gpt-4o-mini"]
        assert available[0].upstream_model == "vendor-mini-v2"
        assert available[0].display_name == "Mini v2"
        assert available[0].max_context_tokens == 128000
        assert available[0].capabilities_json == {"json_mode": True, "reasoning": True}
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_model_routing_precedence_permissions_capabilities_and_shared_streaming_gate(
    migrated_pg_settings: Settings,
) -> None:
    seeded_settings = migrated_pg_settings.model_copy(
        update={
            "model_registry_entries": [
                {
                    "model_id": "model-standard",
                    "upstream_model": "vendor-standard",
                    "display_name": "Standard",
                    "enabled": True,
                    "supports_streaming": True,
                    "max_output_tokens": 2048,
                    "capabilities_json": {"json_mode": True},
                },
                {
                    "model_id": "model-pro",
                    "upstream_model": "vendor-pro",
                    "display_name": "Pro",
                    "enabled": True,
                    "supports_streaming": True,
                    "required_permission": "models:pro",
                    "capabilities_json": {"json_mode": True, "reasoning": True},
                },
                {
                    "model_id": "model-batch",
                    "upstream_model": "vendor-batch",
                    "display_name": "Batch Non-Streaming",
                    "enabled": True,
                    "supports_streaming": False,
                },
                {
                    "model_id": "model-disabled",
                    "upstream_model": "vendor-disabled",
                    "display_name": "Disabled",
                    "enabled": False,
                    "supports_streaming": True,
                },
            ]
        }
    )
    await run_sync_models(seeded_settings)

    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        # 1. Discovery shares exact enabled/permission/capability/forced rules.
        standard_visible = await models_service.list_available_models(
            session_factory,
            user_permissions=frozenset({"chat:use"}),
        )
        assert [m.model_id for m in standard_visible] == ["model-batch", "model-standard"]

        pro_visible = await models_service.list_available_models(
            session_factory,
            user_permissions=frozenset({"chat:use", "models:pro"}),
        )
        assert [m.model_id for m in pro_visible] == ["model-batch", "model-pro", "model-standard"]

        streaming_only = await models_service.list_available_models(
            session_factory,
            user_permissions=frozenset({"chat:use", "models:pro"}),
            supports_streaming=True,
        )
        assert [m.model_id for m in streaming_only] == ["model-pro", "model-standard"]

        forced_visible = await models_service.list_available_models(
            session_factory,
            user_permissions=frozenset({"chat:use", "models:pro"}),
            policy=ModelRoutingPolicy(forced_model_id="model-pro"),
        )
        assert [m.model_id for m in forced_visible] == ["model-pro"]

        # 2. Routing priority: server forced > allowed request > agent default > server default.
        forced = await models_service.resolve_model_for_chat(
            session_factory,
            agent_default_model_id="model-standard",
            requested_model_id="model-batch",
            user_permissions=frozenset({"chat:use", "models:pro"}),
            policy=ModelRoutingPolicy(
                forced_model_id="model-pro",
                allow_request_override=True,
                default_model_id="model-standard",
            ),
        )
        assert forced.model_id == "model-pro"
        assert forced.upstream_model == "vendor-pro"

        overridden = await models_service.resolve_model_for_chat(
            session_factory,
            agent_default_model_id="model-standard",
            requested_model_id="model-pro",
            user_permissions=frozenset({"chat:use", "models:pro"}),
            policy=ModelRoutingPolicy(
                allow_request_override=True,
                default_model_id="model-batch",
            ),
        )
        assert overridden.model_id == "model-pro"

        agent_default = await models_service.resolve_model_for_chat(
            session_factory,
            agent_default_model_id="model-standard",
            requested_model_id=None,
            user_permissions=frozenset({"chat:use"}),
            policy=ModelRoutingPolicy(
                allow_request_override=True,
                default_model_id="model-batch",
            ),
        )
        assert agent_default.model_id == "model-standard"

        server_default = await models_service.resolve_model_for_chat(
            session_factory,
            agent_default_model_id=None,
            requested_model_id=None,
            user_permissions=frozenset({"chat:use"}),
            policy=ModelRoutingPolicy(default_model_id="model-standard"),
        )
        assert server_default.model_id == "model-standard"

        # 3. Missing configuration errors (no candidate, or unconfigured server default/forced).
        with pytest.raises(ServiceUnavailableError) as no_cfg:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id=None,
                requested_model_id=None,
                policy=ModelRoutingPolicy(),
            )
        assert no_cfg.value.code == "model_configuration_missing"

        with pytest.raises(ServiceUnavailableError) as missing_forced:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-standard",
                policy=ModelRoutingPolicy(forced_model_id="ghost-forced"),
            )
        assert missing_forced.value.code == "model_configuration_missing"

        with pytest.raises(ServiceUnavailableError) as missing_server_default:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id=None,
                policy=ModelRoutingPolicy(default_model_id="ghost-default"),
            )
        assert missing_server_default.value.code == "model_configuration_missing"

        # 4. Disallowed override, disabled/unregistered, unauthorized, unsupported capability.
        with pytest.raises(ForbiddenError) as no_override:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-standard",
                requested_model_id="model-batch",
                policy=ModelRoutingPolicy(allow_request_override=False),
            )
        assert no_override.value.code == "model_override_not_allowed"

        with pytest.raises(ConflictError) as disabled_agent_default:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-disabled",
                requested_model_id=None,
                policy=ModelRoutingPolicy(
                    allow_request_override=True,
                    default_model_id="model-standard",
                ),
            )
        assert disabled_agent_default.value.code == "model_unavailable"

        with pytest.raises(ConflictError) as unregistered_req:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-standard",
                requested_model_id="unregistered-model",
                policy=ModelRoutingPolicy(allow_request_override=True),
            )
        assert unregistered_req.value.code == "model_unavailable"

        with pytest.raises(ForbiddenError) as no_perm:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-standard",
                requested_model_id="model-pro",
                user_permissions=frozenset({"chat:use"}),
                policy=ModelRoutingPolicy(allow_request_override=True),
            )
        assert no_perm.value.code == "model_permission_denied"

        with pytest.raises(UnprocessableEntityError) as no_stream:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-batch",
                require_streaming=True,
                policy=ModelRoutingPolicy(),
            )
        assert no_stream.value.code == "model_capability_unsupported"
        assert no_stream.value.details["missing_capabilities"] == ["streaming"]

        with pytest.raises(UnprocessableEntityError) as no_reasoning:
            await models_service.resolve_model_for_chat(
                session_factory,
                agent_default_model_id="model-standard",
                required_capabilities=frozenset({"reasoning"}),
                policy=ModelRoutingPolicy(),
            )
        assert no_reasoning.value.code == "model_capability_unsupported"
        assert no_reasoning.value.details["missing_capabilities"] == ["reasoning"]

        # 5. Shared `prepare_generation` enforces streaming capability BEFORE creating a generation.
        alice = await users_service.resolve_or_create_user(
            session_factory,
            issuer="https://test.mem-ai.local",
            subject="alice-stream-cap",
        )
        agent_id = uuid4()
        snapshot = PersonaSnapshot(
            agent_id=agent_id,
            version_id=uuid4(),
            version=1,
            system_prompt="Batch only persona",
            behavior_rules=(),
            default_model_id="model-batch",
            memory_policy=MemoryPolicy(),
            published_at=datetime.now(UTC),
        )

        class _SinglePersonaProvider:
            async def get_latest_published(
                self,
                *,
                owner_id: UUID,
                agent_id: UUID,
            ) -> PersonaSnapshot | None:
                return snapshot if owner_id == alice.id and agent_id == snapshot.agent_id else None

            async def get_version(
                self,
                *,
                owner_id: UUID,
                agent_id: UUID,
                version_id: UUID,
            ) -> PersonaSnapshot | None:
                if (
                    owner_id == alice.id
                    and agent_id == snapshot.agent_id
                    and version_id == snapshot.version_id
                ):
                    return snapshot
                return None

        with pytest.raises(UnprocessableEntityError) as prep_err:
            await conversations_service.prepare_generation(
                session_factory,
                persona_provider=_SinglePersonaProvider(),
                owner_id=alice.id,
                agent_id=agent_id,
                conversation_id=None,
                user_message="Try streaming on batch model",
                options=ChatOptions(
                    user_permissions=frozenset({"chat:use"}),
                    require_streaming=True,
                ),
            )
        assert prep_err.value.code == "model_capability_unsupported"

        # Confirm zero conversations/messages were created on pre-call rejection.
        prepared_sync = await conversations_service.prepare_generation(
            session_factory,
            persona_provider=_SinglePersonaProvider(),
            owner_id=alice.id,
            agent_id=agent_id,
            conversation_id=None,
            user_message="Sync works on batch model",
            options=ChatOptions(
                user_permissions=frozenset({"chat:use"}),
                require_streaming=False,
            ),
        )
        assert prepared_sync.model_id == "model-batch"
        gen = await generation_store.get_generation_for_owner(
            session_factory,
            owner_id=alice.id,
            conversation_id=prepared_sync.conversation_id,
            generation_id=prepared_sync.generation_id,
        )
        assert gen.model_id == "model-batch"
    finally:
        await engine.dispose()
