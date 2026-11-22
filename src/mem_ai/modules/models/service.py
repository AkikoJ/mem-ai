"""Public model registration sync and chat model selection use cases."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ConflictError, ForbiddenError
from mem_ai.modules.models import repository
from mem_ai.modules.models.schemas import (
    ModelConfigRecord,
    ModelConfigSeed,
    ResolvedModelSelection,
)


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


async def resolve_model_for_chat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    agent_default_model_id: str | None,
    requested_model_id: str | None = None,
    server_fallback_model_id: str | None = None,
) -> ResolvedModelSelection:
    """Resolve and validate the effective server-registered default model before invocation.

    Ticket 06 extends this shared selection entry with `/v1/models` discovery, server forced
    policy, request-override controls, permission checks, and capability validation.
    """
    if requested_model_id is not None and requested_model_id.strip():
        raise ForbiddenError(
            code="model_override_not_allowed",
            message="request model override is not enabled by server policy",
        )

    if agent_default_model_id is not None and agent_default_model_id.strip():
        candidate_model_id: str | None = agent_default_model_id.strip()
    elif server_fallback_model_id is not None and server_fallback_model_id.strip():
        candidate_model_id = server_fallback_model_id.strip()
    else:
        candidate_model_id = None

    if candidate_model_id is None:
        raise ConflictError(
            code="model_unavailable",
            message="no default model is configured for this agent",
        )

    async with session_factory() as session:
        config = await repository.get_model_config_by_model_id(
            session,
            model_id=candidate_model_id,
        )

    if config is None or not config.enabled:
        raise ConflictError(
            code="model_unavailable",
            message="selected model is not available",
            details={"model_id": candidate_model_id},
        )

    return ResolvedModelSelection(
        model_id=config.model_id,
        provider_id=config.provider_id,
        upstream_model=config.upstream_model,
        supports_streaming=config.supports_streaming,
        max_output_tokens=config.max_output_tokens,
    )
