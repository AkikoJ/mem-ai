"""Local read-only `PersonaProvider` implementation delegating queries to `agents.service`."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.persona_provider import PersonaSnapshot
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import PersonaVersionRecord


def _to_persona_snapshot(record: PersonaVersionRecord) -> PersonaSnapshot | None:
    if record.published_at is None:
        return None
    return PersonaSnapshot(
        agent_id=record.agent_id,
        version_id=record.version_id,
        version=record.version,
        system_prompt=record.system_prompt,
        behavior_rules=record.behavior_rules,
        tone=record.tone,
        language=record.language,
        default_model_id=record.default_model_id,
        memory_policy=record.memory_policy,
        published_at=record.published_at,
    )


class LocalPersonaProvider:
    """Local PostgreSQL-backed read-only `PersonaProvider` delegating to `agents.service`."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_latest_published(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
    ) -> PersonaSnapshot | None:
        """Return the latest published persona snapshot for an active owned Agent."""
        record = await agents_service.get_latest_published_version_for_owner(
            self._session_factory,
            owner_id=owner_id,
            agent_id=agent_id,
        )
        return _to_persona_snapshot(record) if record is not None else None

    async def get_version(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
        version_id: UUID,
    ) -> PersonaSnapshot | None:
        """Return a specific published immutable persona version snapshot owned by `owner_id`."""
        record = await agents_service.get_persona_version_for_owner(
            self._session_factory,
            owner_id=owner_id,
            agent_id=agent_id,
            version_id=version_id,
        )
        return _to_persona_snapshot(record) if record is not None else None
