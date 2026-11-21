"""Public use cases, atomic publish transactions, and read-only lookups for `agents`."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.api.pagination import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    CursorPayload,
    decode_cursor,
    encode_cursor,
)
from mem_ai.contracts.persona_provider import MemoryPolicy
from mem_ai.core.exceptions import ConflictError, NotFoundError, UnprocessableEntityError
from mem_ai.db.base import is_unique_violation
from mem_ai.modules.agents import repository
from mem_ai.modules.agents.models import PERSONA_VERSION_UNIQUE_CONSTRAINT, Agent, PersonaVersion
from mem_ai.modules.agents.schemas import (
    AgentCreateRequest,
    AgentDraftRecord,
    AgentPublishRequest,
    AgentRecord,
    AgentStatus,
    AgentSummaryRecord,
    AgentUpdateDraftRequest,
    PersonaVersionRecord,
)
from mem_ai.modules.users import service as users_service


def _draft_from_agent(agent: Agent) -> AgentDraftRecord:
    return AgentDraftRecord(
        system_prompt=agent.draft_system_prompt,
        behavior_rules=tuple(agent.draft_behavior_rules),
        tone=agent.draft_tone,
        language=agent.draft_language,
        default_model_id=agent.draft_default_model_id,
        memory_policy=MemoryPolicy.model_validate(agent.draft_memory_policy),
    )


def _to_persona_version_record(row: PersonaVersion) -> PersonaVersionRecord:
    return PersonaVersionRecord.model_validate(row)


def _summary_kwargs(
    agent: Agent,
    *,
    current_published_version: int | None,
) -> dict[str, Any]:
    return {
        "agent_id": agent.id,
        "owner_id": agent.owner_id,
        "name": agent.name,
        "status": cast(AgentStatus, agent.status),
        "current_published_version_id": agent.current_published_version_id,
        "current_published_version": current_published_version,
        "latest_version": agent.latest_version_number,
        "created_at": agent.created_at,
        "updated_at": agent.updated_at,
        "retired_at": agent.retired_at,
    }


def _to_agent_summary_record(
    agent: Agent,
    *,
    current_published_version: int | None,
) -> AgentSummaryRecord:
    return AgentSummaryRecord(
        **_summary_kwargs(agent, current_published_version=current_published_version)
    )


def _to_agent_record(
    agent: Agent,
    *,
    published_version: PersonaVersionRecord | None,
) -> AgentRecord:
    return AgentRecord(
        **_summary_kwargs(
            agent,
            current_published_version=(
                published_version.version if published_version is not None else None
            ),
        ),
        draft=_draft_from_agent(agent),
        published_version=published_version,
    )


async def _insert_draft_snapshot(
    session: AsyncSession,
    *,
    agent: Agent,
    published_at: datetime | None,
) -> tuple[PersonaVersion, int]:
    next_version = agent.latest_version_number + 1
    row = await repository.insert_persona_version(
        session,
        agent_id=agent.id,
        version=next_version,
        draft=_draft_from_agent(agent),
        published_at=published_at,
    )
    return row, next_version


async def create_agent(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    data: AgentCreateRequest,
) -> AgentRecord:
    """Create a private Agent with its initial draft stored on the `agents` row."""
    draft = AgentDraftRecord(
        system_prompt=data.system_prompt,
        behavior_rules=tuple(data.behavior_rules),
        tone=data.tone,
        language=data.language,
        default_model_id=data.default_model_id,
        memory_policy=data.memory_policy.to_contract(),
    )
    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        agent = await repository.create_agent(
            session,
            owner_id=owner_id,
            name=data.name,
            draft=draft,
        )
        return _to_agent_record(agent, published_version=None)


async def get_agent(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> AgentRecord:
    """Fetch a single private Agent owned by `owner_id`, hiding cross-user existence with 404."""
    async with session_factory() as session:
        result = await repository.get_agent_with_published_version(
            session,
            owner_id=owner_id,
            agent_id=agent_id,
        )
        if result is None:
            raise NotFoundError(code="agent_not_found", message="agent not found")
        agent, published_row = result
        published_record = (
            _to_persona_version_record(published_row) if published_row is not None else None
        )
        return _to_agent_record(agent, published_version=published_record)


async def list_agents(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    limit: int = DEFAULT_PAGE_LIMIT,
    cursor: str | None = None,
) -> tuple[list[AgentSummaryRecord], str | None, bool]:
    """List owned Agents with stable `(created_at DESC, id DESC)` keyset cursor pagination."""
    if limit < 1 or limit > MAX_PAGE_LIMIT:
        raise UnprocessableEntityError(
            code="validation_error",
            message=f"limit must be between 1 and {MAX_PAGE_LIMIT}",
        )

    cursor_created_at: datetime | None = None
    cursor_id: UUID | None = None
    if cursor is not None:
        payload = decode_cursor(cursor, expected_direction="desc")
        cursor_created_at = payload.created_at
        try:
            cursor_id = payload.id if isinstance(payload.id, UUID) else UUID(str(payload.id))
        except ValueError as exc:
            raise UnprocessableEntityError(
                code="invalid_cursor",
                message="pagination cursor is invalid",
            ) from exc

    async with session_factory() as session:
        rows = await repository.list_agents_for_owner(
            session,
            owner_id=owner_id,
            limit=limit + 1,
            cursor_created_at=cursor_created_at,
            cursor_id=cursor_id,
        )

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    items = [
        _to_agent_summary_record(agent, current_published_version=published_version)
        for agent, published_version in page_rows
    ]

    next_cursor: str | None = None
    if has_more and items:
        last = items[-1]
        next_cursor = encode_cursor(
            CursorPayload(
                created_at=last.created_at,
                id=last.agent_id,
                direction="desc",
            )
        )
    return items, next_cursor, has_more


async def update_agent_draft(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
    data: AgentUpdateDraftRequest,
) -> AgentRecord:
    """Update only the draft fields on `agents` without mutating any published persona snapshot."""
    provided = data.model_fields_set
    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        found = await repository.get_agent_with_published_version(
            session,
            owner_id=owner_id,
            agent_id=agent_id,
        )
        if found is None:
            raise NotFoundError(code="agent_not_found", message="agent not found")

        agent, published_row = found
        if agent.status != "active":
            raise ConflictError(
                code="agent_retired",
                message="retired agent draft cannot be edited",
            )

        updates: dict[str, Any] = {}
        if "name" in provided and data.name is not None:
            updates["name"] = data.name
        if "system_prompt" in provided and data.system_prompt is not None:
            updates["draft_system_prompt"] = data.system_prompt
        if "behavior_rules" in provided and data.behavior_rules is not None:
            updates["draft_behavior_rules"] = list(data.behavior_rules)
        if "tone" in provided:
            updates["draft_tone"] = data.tone
        if "language" in provided:
            updates["draft_language"] = data.language
        if "default_model_id" in provided:
            updates["draft_default_model_id"] = data.default_model_id
        if "memory_policy" in provided and data.memory_policy is not None:
            current_policy = MemoryPolicy.model_validate(agent.draft_memory_policy)
            patch_fields = data.memory_policy.model_dump(exclude_unset=True)
            merged_policy = current_policy.model_copy(update=patch_fields)
            updates["draft_memory_policy"] = merged_policy.model_dump(mode="json")

        updated_agent = await repository.conditional_update_draft(
            session,
            agent_id=agent.id,
            owner_id=owner_id,
            expected_revision=agent.revision,
            values=updates,
        )
        if updated_agent is None:
            raise ConflictError(
                code="agent_revision_conflict",
                message="agent was modified concurrently while updating draft",
            )

        published_record = (
            _to_persona_version_record(published_row) if published_row is not None else None
        )
        return _to_agent_record(updated_agent, published_version=published_record)


async def create_persona_version(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> PersonaVersionRecord:
    """Snapshot the current Agent draft into a new immutable version without publishing it."""
    try:
        async with session_factory.begin() as session:
            await users_service.ensure_user_active_in_session(session, user_id=owner_id)
            agent = await repository.get_agent_for_owner(
                session,
                owner_id=owner_id,
                agent_id=agent_id,
            )
            if agent is None:
                raise NotFoundError(code="agent_not_found", message="agent not found")
            if agent.status != "active":
                raise ConflictError(
                    code="agent_retired",
                    message="retired agent cannot create persona versions",
                )

            version_row, next_version = await _insert_draft_snapshot(
                session,
                agent=agent,
                published_at=None,
            )
            updated_agent = await repository.conditional_advance_latest_version(
                session,
                agent_id=agent.id,
                owner_id=owner_id,
                expected_revision=agent.revision,
                new_latest_version=next_version,
            )
            if updated_agent is None:
                raise ConflictError(
                    code="agent_version_conflict",
                    message="agent was modified concurrently while allocating version",
                )
            return _to_persona_version_record(version_row)
    except IntegrityError as exc:
        if is_unique_violation(exc, constraint_name=PERSONA_VERSION_UNIQUE_CONSTRAINT):
            raise ConflictError(
                code="agent_version_conflict",
                message="concurrent persona version allocation conflict",
            ) from exc
        raise


async def publish_agent(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
    data: AgentPublishRequest | None = None,
) -> AgentRecord:
    """Atomically publish an existing version or snapshot the current draft and activate it."""
    request = data if data is not None else AgentPublishRequest()
    try:
        async with session_factory.begin() as session:
            await users_service.ensure_user_active_in_session(session, user_id=owner_id)
            agent = await repository.get_agent_for_owner(
                session,
                owner_id=owner_id,
                agent_id=agent_id,
            )
            if agent is None:
                raise NotFoundError(code="agent_not_found", message="agent not found")
            if agent.status != "active":
                raise ConflictError(
                    code="agent_retired",
                    message="retired agent cannot be published",
                )

            expected_current_id = agent.current_published_version_id
            expected_revision = agent.revision
            now = datetime.now(UTC)

            if request.version_id is not None:
                target_version = await repository.mark_persona_version_published(
                    session,
                    agent_id=agent.id,
                    version_id=request.version_id,
                    published_at=now,
                )
                if target_version is None:
                    raise NotFoundError(
                        code="persona_version_not_found",
                        message="persona version not found",
                    )
                new_latest_version = agent.latest_version_number
            else:
                target_version, new_latest_version = await _insert_draft_snapshot(
                    session,
                    agent=agent,
                    published_at=now,
                )

            updated_agent = await repository.conditional_publish_agent(
                session,
                agent_id=agent.id,
                owner_id=owner_id,
                expected_revision=expected_revision,
                expected_current_version_id=expected_current_id,
                new_current_version_id=target_version.id,
                new_latest_version=new_latest_version,
            )
            if updated_agent is None:
                raise ConflictError(
                    code="agent_publish_conflict",
                    message="agent was modified concurrently during publish",
                )

            version_record = _to_persona_version_record(target_version)
            return _to_agent_record(updated_agent, published_version=version_record)
    except IntegrityError as exc:
        if is_unique_violation(exc, constraint_name=PERSONA_VERSION_UNIQUE_CONSTRAINT):
            raise ConflictError(
                code="agent_publish_conflict",
                message="concurrent persona version publish conflict",
            ) from exc
        raise


async def get_latest_published_version_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
) -> PersonaVersionRecord | None:
    """Read-only lookup used by `personas.provider` for new conversation snapshot resolution."""
    async with session_factory() as session:
        row = await repository.get_latest_published_version_for_owner(
            session,
            owner_id=owner_id,
            agent_id=agent_id,
        )
        return _to_persona_version_record(row) if row is not None else None


async def get_persona_version_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    agent_id: UUID,
    version_id: UUID,
) -> PersonaVersionRecord | None:
    """Read-only lookup used by `personas.provider` for pinned published conversation versions."""
    async with session_factory() as session:
        row = await repository.get_published_persona_version_for_owner(
            session,
            owner_id=owner_id,
            agent_id=agent_id,
            version_id=version_id,
        )
        return _to_persona_version_record(row) if row is not None else None
