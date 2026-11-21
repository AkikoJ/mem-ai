"""PostgreSQL repository for `agents` and immutable `persona_versions`."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, func, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.agents.models import Agent, PersonaVersion
from mem_ai.modules.agents.schemas import AgentDraftRecord


async def create_agent(
    session: AsyncSession,
    *,
    owner_id: UUID,
    name: str,
    draft: AgentDraftRecord,
) -> Agent:
    """Insert a new active private Agent with initial draft fields."""
    agent = Agent(
        owner_id=owner_id,
        name=name,
        status="active",
        draft_system_prompt=draft.system_prompt,
        draft_behavior_rules=list(draft.behavior_rules),
        draft_tone=draft.tone,
        draft_language=draft.language,
        draft_default_model_id=draft.default_model_id,
        draft_memory_policy=draft.memory_policy.model_dump(mode="json"),
    )
    session.add(agent)
    await session.flush()
    await session.refresh(agent)
    return agent


async def get_agent_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> Agent | None:
    """Fetch an owned Agent row by `(owner_id, agent_id)`."""
    stmt = select(Agent).where(Agent.id == agent_id, Agent.owner_id == owner_id)
    return await session.scalar(stmt)


async def get_agent_with_published_version(
    session: AsyncSession,
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> tuple[Agent, PersonaVersion | None] | None:
    """Fetch an owned Agent and its currently published PersonaVersion (if any)."""
    stmt = (
        select(Agent, PersonaVersion)
        .outerjoin(
            PersonaVersion,
            and_(
                Agent.current_published_version_id == PersonaVersion.id,
                PersonaVersion.agent_id == Agent.id,
            ),
        )
        .where(Agent.id == agent_id, Agent.owner_id == owner_id)
    )
    row = (await session.execute(stmt)).first()
    if row is None:
        return None
    return (row[0], row[1])


async def list_agents_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    limit: int,
    cursor_created_at: datetime | None = None,
    cursor_id: UUID | None = None,
) -> list[tuple[Agent, int | None]]:
    """List owned Agents ordered by `(created_at DESC, id DESC)` with keyset cursor."""
    stmt = (
        select(Agent, PersonaVersion.version)
        .outerjoin(
            PersonaVersion,
            and_(
                Agent.current_published_version_id == PersonaVersion.id,
                PersonaVersion.agent_id == Agent.id,
            ),
        )
        .where(Agent.owner_id == owner_id)
    )
    if cursor_created_at is not None and cursor_id is not None:
        stmt = stmt.where(tuple_(Agent.created_at, Agent.id) < (cursor_created_at, cursor_id))

    stmt = stmt.order_by(Agent.created_at.desc(), Agent.id.desc()).limit(limit)
    rows = (await session.execute(stmt)).all()
    return [(row[0], row[1]) for row in rows]


async def get_persona_version(
    session: AsyncSession,
    *,
    agent_id: UUID,
    version_id: UUID,
) -> PersonaVersion | None:
    """Fetch a persona version belonging to a specific Agent."""
    stmt = select(PersonaVersion).where(
        PersonaVersion.id == version_id,
        PersonaVersion.agent_id == agent_id,
    )
    return await session.scalar(stmt)


async def get_published_persona_version_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    agent_id: UUID,
    version_id: UUID,
) -> PersonaVersion | None:
    """Fetch a published immutable persona version owned by `owner_id` (active or retired)."""
    stmt = (
        select(PersonaVersion)
        .join(Agent, Agent.id == PersonaVersion.agent_id)
        .where(
            Agent.id == agent_id,
            Agent.owner_id == owner_id,
            PersonaVersion.id == version_id,
            PersonaVersion.published_at.is_not(None),
        )
    )
    return await session.scalar(stmt)


async def get_latest_published_version_for_owner(
    session: AsyncSession,
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> PersonaVersion | None:
    """Fetch the current published version for an active owned Agent."""
    stmt = (
        select(PersonaVersion)
        .join(
            Agent,
            and_(
                Agent.current_published_version_id == PersonaVersion.id,
                PersonaVersion.agent_id == Agent.id,
            ),
        )
        .where(
            Agent.id == agent_id,
            Agent.owner_id == owner_id,
            Agent.status == "active",
            PersonaVersion.published_at.is_not(None),
        )
    )
    return await session.scalar(stmt)


async def insert_persona_version(
    session: AsyncSession,
    *,
    agent_id: UUID,
    version: int,
    draft: AgentDraftRecord,
    published_at: datetime | None,
) -> PersonaVersion:
    """Insert an immutable `persona_versions` snapshot row."""
    row = PersonaVersion(
        agent_id=agent_id,
        version=version,
        system_prompt=draft.system_prompt,
        behavior_rules=list(draft.behavior_rules),
        tone=draft.tone,
        language=draft.language,
        default_model_id=draft.default_model_id,
        memory_policy=draft.memory_policy.model_dump(mode="json"),
        published_at=published_at,
    )
    session.add(row)
    await session.flush()
    await session.refresh(row)
    return row


async def mark_persona_version_published(
    session: AsyncSession,
    *,
    agent_id: UUID,
    version_id: UUID,
    published_at: datetime,
) -> PersonaVersion | None:
    """Stamp `published_at` once (preserving any earlier timestamp) when publishing a snapshot."""
    stmt = (
        update(PersonaVersion)
        .where(
            PersonaVersion.id == version_id,
            PersonaVersion.agent_id == agent_id,
        )
        .values(published_at=func.coalesce(PersonaVersion.published_at, published_at))
        .returning(PersonaVersion)
    )
    return await session.scalar(stmt, execution_options={"populate_existing": True})


async def conditional_update_draft(
    session: AsyncSession,
    *,
    agent_id: UUID,
    owner_id: UUID,
    expected_revision: int,
    values: dict[str, Any],
) -> Agent | None:
    """Conditionally update draft columns on an active Agent guarded by `revision`."""
    stmt = (
        update(Agent)
        .where(
            Agent.id == agent_id,
            Agent.owner_id == owner_id,
            Agent.status == "active",
            Agent.revision == expected_revision,
        )
        .values(
            **values,
            revision=Agent.revision + 1,
            updated_at=func.now(),
        )
        .returning(Agent)
    )
    return await session.scalar(stmt, execution_options={"populate_existing": True})


async def conditional_advance_latest_version(
    session: AsyncSession,
    *,
    agent_id: UUID,
    owner_id: UUID,
    expected_revision: int,
    new_latest_version: int,
) -> Agent | None:
    """Conditionally advance `latest_version_number` and `revision` when snapshotting a version."""
    stmt = (
        update(Agent)
        .where(
            Agent.id == agent_id,
            Agent.owner_id == owner_id,
            Agent.status == "active",
            Agent.revision == expected_revision,
        )
        .values(
            latest_version_number=new_latest_version,
            revision=Agent.revision + 1,
            updated_at=func.now(),
        )
        .returning(Agent)
    )
    return await session.scalar(stmt, execution_options={"populate_existing": True})


async def conditional_publish_agent(
    session: AsyncSession,
    *,
    agent_id: UUID,
    owner_id: UUID,
    expected_revision: int,
    expected_current_version_id: UUID | None,
    new_current_version_id: UUID,
    new_latest_version: int,
) -> Agent | None:
    """Conditionally switch `current_published_version_id` on an active Agent."""
    current_version_predicate = (
        Agent.current_published_version_id.is_(None)
        if expected_current_version_id is None
        else Agent.current_published_version_id == expected_current_version_id
    )
    stmt = (
        update(Agent)
        .where(
            Agent.id == agent_id,
            Agent.owner_id == owner_id,
            Agent.status == "active",
            Agent.revision == expected_revision,
            current_version_predicate,
        )
        .values(
            current_published_version_id=new_current_version_id,
            latest_version_number=new_latest_version,
            revision=Agent.revision + 1,
            updated_at=func.now(),
        )
        .returning(Agent)
    )
    return await session.scalar(stmt, execution_options={"populate_existing": True})
