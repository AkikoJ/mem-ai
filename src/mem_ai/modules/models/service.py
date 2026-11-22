"""Public model registration sync, discovery, and chat routing policy use cases."""

from __future__ import annotations

from collections.abc import Sequence
from collections.abc import Set as AbstractSet

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import (
    ConflictError,
    ForbiddenError,
    ServiceUnavailableError,
    UnprocessableEntityError,
)
from mem_ai.modules.models import repository
from mem_ai.modules.models.schemas import (
    ModelConfigRecord,
    ModelConfigSeed,
    ModelRoutingPolicy,
    ResolvedModelSelection,
)


def _normalize_optional_model_id(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def model_routing_policy_from_settings(settings: Settings) -> ModelRoutingPolicy:
    """Build the shared server-controlled `ModelRoutingPolicy` from runtime settings."""
    return ModelRoutingPolicy(
        forced_model_id=_normalize_optional_model_id(settings.model_forced_id),
        allow_request_override=settings.model_allow_request_override,
        default_model_id=_normalize_optional_model_id(settings.model_default_id),
    )


def _is_model_permitted(
    config: ModelConfigRecord,
    *,
    user_permissions: AbstractSet[str],
) -> bool:
    if config.required_permission is None:
        return True
    return config.required_permission in user_permissions


def _missing_capabilities(
    config: ModelConfigRecord,
    *,
    require_streaming: bool,
    required_capabilities: AbstractSet[str],
) -> list[str]:
    missing: list[str] = []
    needs_streaming = require_streaming or ("streaming" in required_capabilities)
    if needs_streaming and not config.supports_streaming:
        missing.append("streaming")

    for capability in sorted(required_capabilities):
        if capability == "streaming":
            continue
        if config.capabilities_json.get(capability) is not True:
            missing.append(capability)
    return missing


async def sync_model_configs(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    seeds: Sequence[ModelConfigSeed],
) -> list[ModelConfigRecord]:
    """Idempotently sync server-controlled model config entries into PostgreSQL."""
    if not seeds:
        return []
    records: list[ModelConfigRecord] = []
    async with session_factory.begin() as session:
        for seed in seeds:
            row = await repository.upsert_model_config(session, seed=seed)
            records.append(ModelConfigRecord.model_validate(row))
    return records


async def list_available_models(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_permissions: AbstractSet[str] = frozenset(),
    policy: ModelRoutingPolicy | None = None,
    supports_streaming: bool | None = None,
    required_capabilities: AbstractSet[str] = frozenset(),
) -> list[ModelConfigRecord]:
    """List enabled, permitted models matching the same policy and capability rules as chat."""
    async with session_factory() as session:
        rows = await repository.list_enabled_model_configs(session)

    records = [ModelConfigRecord.model_validate(row) for row in rows]
    forced_model_id = (
        _normalize_optional_model_id(policy.forced_model_id) if policy is not None else None
    )

    visible: list[ModelConfigRecord] = []
    for record in records:
        if forced_model_id is not None and record.model_id != forced_model_id:
            continue
        if not _is_model_permitted(record, user_permissions=user_permissions):
            continue
        if supports_streaming is not None and record.supports_streaming != supports_streaming:
            continue
        if _missing_capabilities(
            record,
            require_streaming=supports_streaming is True,
            required_capabilities=required_capabilities,
        ):
            continue
        visible.append(record)
    return visible


async def resolve_model_for_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    agent_default_model_id: str | None,
    requested_model_id: str | None = None,
    user_permissions: AbstractSet[str] = frozenset(),
    require_streaming: bool = False,
    required_capabilities: AbstractSet[str] = frozenset(),
    policy: ModelRoutingPolicy | None = None,
) -> ResolvedModelSelection:
    """Resolve and validate the effective model for sync or streaming chat before invocation.

    Routing priority:
    1. Server forced policy (`policy.forced_model_id`)
    2. Allowed requested `modelId` (`policy.allow_request_override=True`)
    3. Agent pinned/default model (`agent_default_model_id`) — never silently failed over
    4. Server default fallback (`policy.default_model_id`) only when Agent omitted default
    """
    effective_forced = (
        _normalize_optional_model_id(policy.forced_model_id) if policy is not None else None
    )
    allow_override = policy.allow_request_override if policy is not None else False
    effective_server_default = (
        _normalize_optional_model_id(policy.default_model_id) if policy is not None else None
    )

    requested = _normalize_optional_model_id(requested_model_id)
    agent_default = _normalize_optional_model_id(agent_default_model_id)

    if effective_forced is not None:
        candidate_model_id = effective_forced
        source = "server_forced"
    elif requested is not None:
        if not allow_override:
            raise ForbiddenError(
                code="model_override_not_allowed",
                message="request model override is not enabled by server policy",
            )
        candidate_model_id = requested
        source = "request_override"
    elif agent_default is not None:
        candidate_model_id = agent_default
        source = "agent_default"
    elif effective_server_default is not None:
        candidate_model_id = effective_server_default
        source = "server_default"
    else:
        raise ServiceUnavailableError(
            code="model_configuration_missing",
            message="no model configuration is available",
        )

    async with session_factory() as session:
        row = await repository.get_model_config_by_model_id(
            session,
            model_id=candidate_model_id,
        )
        registry_empty = row is None and not await repository.has_any_model_config(session)

    if registry_empty:
        raise ServiceUnavailableError(
            code="model_configuration_missing",
            message="no server model configurations have been synced",
            details={"model_id": candidate_model_id},
        )

    config = ModelConfigRecord.model_validate(row) if row is not None else None
    if config is None or not config.enabled:
        if source in {"server_forced", "server_default"}:
            raise ServiceUnavailableError(
                code="model_configuration_missing",
                message="configured server model is not registered or enabled",
                details={"model_id": candidate_model_id},
            )
        raise ConflictError(
            code="model_unavailable",
            message="selected model is not available",
            details={"model_id": candidate_model_id},
        )

    if not _is_model_permitted(config, user_permissions=user_permissions):
        raise ForbiddenError(
            code="model_permission_denied",
            message="current user is not permitted to use the selected model",
            details={"model_id": config.model_id},
        )

    missing = _missing_capabilities(
        config,
        require_streaming=require_streaming,
        required_capabilities=required_capabilities,
    )
    if missing:
        raise UnprocessableEntityError(
            code="model_capability_unsupported",
            message="selected model does not support required capabilities",
            details={
                "model_id": config.model_id,
                "missing_capabilities": missing,
            },
        )

    return ResolvedModelSelection(
        model_id=config.model_id,
        provider_id=config.provider_id,
        upstream_model=config.upstream_model,
        supports_streaming=config.supports_streaming,
        max_context_tokens=config.max_context_tokens,
        max_output_tokens=config.max_output_tokens,
        capabilities=dict(config.capabilities_json),
    )
