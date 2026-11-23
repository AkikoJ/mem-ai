"""HTTP routes for private Agent lifecycle and immutable persona version publishing."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Path, Query, status

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.api.pagination import DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, PaginatedResponse
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import (
    AgentCreateRequest,
    AgentDetailResponse,
    AgentPublishRequest,
    AgentSummaryResponse,
    AgentUpdateDraftRequest,
    PersonaVersionResponse,
)
from mem_ai.modules.auth.dependencies import AuthContextDep

router = APIRouter(prefix="/agents", tags=["agents"])

AgentIdPath = Annotated[UUID, Path(alias="agentId")]


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiResponse[AgentDetailResponse],
)
async def create_agent(
    payload: AgentCreateRequest,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[AgentDetailResponse]:
    """Create a private Agent owned by the authenticated user with its initial draft."""
    record = await agents_service.create_agent(
        session_factory,
        owner_id=auth.user_id,
        data=payload,
    )
    return ok_response(AgentDetailResponse.from_detail_record(record))


@router.get(
    "",
    response_model=ApiResponse[PaginatedResponse[AgentSummaryResponse]],
)
async def list_agents(
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
) -> ApiResponse[PaginatedResponse[AgentSummaryResponse]]:
    """List the authenticated user's private Agents using stable cursor pagination."""
    items, next_cursor, has_more = await agents_service.list_agents(
        session_factory,
        owner_id=auth.user_id,
        limit=limit,
        cursor=cursor,
    )
    page = PaginatedResponse[AgentSummaryResponse](
        items=[AgentSummaryResponse.from_summary_record(item) for item in items],
        next_cursor=next_cursor,
        has_more=has_more,
    )
    return ok_response(page)


@router.get(
    "/{agentId}",
    response_model=ApiResponse[AgentDetailResponse],
)
async def get_agent(
    agent_id: AgentIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[AgentDetailResponse]:
    """Return detail for a single private Agent owned by the authenticated user."""
    record = await agents_service.get_agent(
        session_factory,
        owner_id=auth.user_id,
        agent_id=agent_id,
    )
    return ok_response(AgentDetailResponse.from_detail_record(record))


@router.patch(
    "/{agentId}",
    response_model=ApiResponse[AgentDetailResponse],
)
async def update_agent_draft(
    agent_id: AgentIdPath,
    payload: AgentUpdateDraftRequest,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[AgentDetailResponse]:
    """Update only the draft fields on an owned Agent without mutating published versions."""
    record = await agents_service.update_agent_draft(
        session_factory,
        owner_id=auth.user_id,
        agent_id=agent_id,
        data=payload,
    )
    return ok_response(AgentDetailResponse.from_detail_record(record))


@router.post(
    "/{agentId}/versions",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiResponse[PersonaVersionResponse],
)
async def create_persona_version(
    agent_id: AgentIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[PersonaVersionResponse]:
    """Snapshot the current draft into a new immutable persona version without publishing it."""
    record = await agents_service.create_persona_version(
        session_factory,
        owner_id=auth.user_id,
        agent_id=agent_id,
    )
    return ok_response(PersonaVersionResponse.from_record(record))


@router.post(
    "/{agentId}/publish",
    response_model=ApiResponse[AgentDetailResponse],
)
async def publish_agent(
    agent_id: AgentIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    payload: Annotated[AgentPublishRequest | None, Body()] = None,
) -> ApiResponse[AgentDetailResponse]:
    """Atomically publish a persona version and switch the Agent's current published pointer."""
    record = await agents_service.publish_agent(
        session_factory,
        owner_id=auth.user_id,
        agent_id=agent_id,
        data=payload,
    )
    return ok_response(AgentDetailResponse.from_detail_record(record))


@router.delete(
    "/{agentId}",
    response_model=ApiResponse[AgentDetailResponse],
)
async def retire_agent(
    agent_id: AgentIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[AgentDetailResponse]:
    """Idempotently retire an owned Agent, blocking new chats while keeping existing sessions."""
    record = await agents_service.retire_agent(
        session_factory,
        owner_id=auth.user_id,
        agent_id=agent_id,
    )
    return ok_response(AgentDetailResponse.from_detail_record(record))
