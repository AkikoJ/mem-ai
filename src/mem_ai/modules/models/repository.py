"""PostgreSQL repository for `model_configs` accepting explicit `AsyncSession`."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.models.models import ModelConfig
from mem_ai.modules.models.schemas import ModelConfigSeed


async def get_model_config_by_model_id(
    session: AsyncSession,
    *,
    model_id: str,
) -> ModelConfig | None:
    """Fetch a single `model_configs` row by its registered `model_id`."""
    stmt = select(ModelConfig).where(ModelConfig.model_id == model_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_enabled_model_configs(session: AsyncSession) -> list[ModelConfig]:
    """Return all enabled `model_configs` rows ordered deterministically by `model_id`."""
    stmt = (
        select(ModelConfig)
        .where(ModelConfig.enabled.is_(True))
        .order_by(ModelConfig.model_id.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def has_any_model_config(session: AsyncSession) -> bool:
    """Return True when at least one model configuration row has been synced."""
    stmt = select(ModelConfig.id).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def upsert_model_config(
    session: AsyncSession,
    *,
    seed: ModelConfigSeed,
) -> ModelConfig:
    """Idempotently insert or update a `model_configs` row by `model_id`."""
    values = {
        "model_id": seed.model_id,
        "provider_id": seed.provider_id,
        "upstream_model": seed.resolved_upstream_model(),
        "display_name": seed.resolved_display_name(),
        "enabled": seed.enabled,
        "supports_streaming": seed.supports_streaming,
        "max_context_tokens": seed.max_context_tokens,
        "max_output_tokens": seed.max_output_tokens,
        "required_permission": seed.required_permission,
        "credential_ref": seed.credential_ref,
        "capabilities_json": dict(seed.capabilities_json),
    }
    stmt = (
        pg_insert(ModelConfig)
        .values(**values)
        .on_conflict_do_update(
            index_elements=[ModelConfig.model_id],
            set_={
                "provider_id": values["provider_id"],
                "upstream_model": values["upstream_model"],
                "display_name": values["display_name"],
                "enabled": values["enabled"],
                "supports_streaming": values["supports_streaming"],
                "max_context_tokens": values["max_context_tokens"],
                "max_output_tokens": values["max_output_tokens"],
                "required_permission": values["required_permission"],
                "credential_ref": values["credential_ref"],
                "capabilities_json": values["capabilities_json"],
                "updated_at": func.now(),
            },
        )
        .returning(ModelConfig)
    )
    result = await session.execute(stmt)
    row = result.scalar_one()
    await session.flush()
    return row
