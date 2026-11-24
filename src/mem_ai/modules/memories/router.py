"""HTTP routes for listing, creating, deleting, and clearing long-term memories."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse

from mem_ai.api.dependencies import (
    DeletionAuditArchiverDep,
    MemoryProviderDep,
    SessionFactoryDep,
    SettingsDep,
)
from mem_ai.api.pagination import DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, PaginatedResponse
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.contracts.memory_provider import MemoryScope
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.schemas import (
    CompletedMemoryDeletionOutcome,
    CreatedMemoryOutcome,
    MemoryCreateRequest,
    MemoryDeletionOutcome,
    MemoryItemResponse,
)
from mem_ai.modules.memories.service import (
    ConversationAccessChecker,
    DefaultConversationAccessChecker,
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
)
from mem_ai.modules.operations.schemas import OperationAcceptedResponse

router = APIRouter(prefix="/memories", tags=["memories"])

_DEFAULT_CONVERSATION_ACCESS_CHECKER = DefaultConversationAccessChecker()
_DEFAULT_RECALLED_MEMORY_MARKER = NoopRecalledMemoryReferenceMarker()


def get_conversation_access_checker(request: Request) -> ConversationAccessChecker:
    """Return the assembled conversation ownership checker (overridable in tests)."""
    configured = getattr(request.app.state, "conversation_access_checker", None)
    if isinstance(configured, ConversationAccessChecker):
        return configured
    return _DEFAULT_CONVERSATION_ACCESS_CHECKER


def get_recalled_memory_marker(request: Request) -> RecalledMemoryReferenceMarker:
    """Return the assembled recalled-memory reference marker (overridable in tests)."""
    configured = getattr(request.app.state, "recalled_memory_marker", None)
    if isinstance(configured, RecalledMemoryReferenceMarker):
        return configured
    return _DEFAULT_RECALLED_MEMORY_MARKER


ConversationAccessCheckerDep = Annotated[
    ConversationAccessChecker,
    Depends(get_conversation_access_checker),
]
RecalledMemoryMarkerDep = Annotated[
    RecalledMemoryReferenceMarker,
    Depends(get_recalled_memory_marker),
]
MemoryIdPath = Annotated[str, Path(alias="memoryId", min_length=1, max_length=255)]


@router.get(
    "",
    response_model=ApiResponse[PaginatedResponse[MemoryItemResponse]],
)
async def list_memories(
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    memory_provider: MemoryProviderDep,
    conversation_checker: ConversationAccessCheckerDep,
    scope: Annotated[MemoryScope | None, Query()] = None,
    conversation_id: Annotated[UUID | None, Query(alias="conversationId")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
) -> ApiResponse[PaginatedResponse[MemoryItemResponse]]:
    """List the authenticated user's memories using stable cursor pagination."""
    page = await memories_service.list_memories(
        session_factory,
        memory_provider=memory_provider,
        user_id=auth.user_id,
        scope=scope,
        conversation_id=conversation_id,
        limit=limit,
        cursor=cursor,
        conversation_access_checker=conversation_checker,
    )
    payload = PaginatedResponse[MemoryItemResponse](
        items=[MemoryItemResponse.from_record(item) for item in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )
    return ok_response(payload)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiResponse[MemoryItemResponse],
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": ApiResponse[OperationAcceptedResponse],
            "description": "Accepted for asynchronous extraction or pending verification.",
        }
    },
)
async def create_memory(
    payload: MemoryCreateRequest,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    memory_provider: MemoryProviderDep,
    conversation_checker: ConversationAccessCheckerDep,
) -> ApiResponse[MemoryItemResponse] | Response:
    """Explicitly create a user- or conversation-scoped long-term memory."""
    outcome = await memories_service.create_explicit_memory(
        session_factory,
        memory_provider=memory_provider,
        user_id=auth.user_id,
        content=payload.content,
        scope=payload.scope,
        conversation_id=payload.conversation_id,
        conversation_access_checker=conversation_checker,
    )
    if isinstance(outcome, CreatedMemoryOutcome):
        return ok_response(MemoryItemResponse.from_record(outcome.record))

    accepted = ok_response(OperationAcceptedResponse.from_record(outcome.operation))
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=accepted.model_dump(mode="json", by_alias=True),
        headers={"Location": outcome.operation.status_url},
    )


def _build_deletion_response(outcome: MemoryDeletionOutcome) -> Response:
    if isinstance(outcome, CompletedMemoryDeletionOutcome):
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    accepted = ok_response(OperationAcceptedResponse.from_record(outcome.operation))
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=accepted.model_dump(mode="json", by_alias=True),
        headers={"Location": outcome.operation.status_url},
    )


@router.delete(
    "/{memoryId}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": ApiResponse[OperationAcceptedResponse],
            "description": "Deletion accepted asynchronously or awaiting external verification.",
        }
    },
)
async def delete_memory(
    memory_id: MemoryIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    memory_provider: MemoryProviderDep,
    recalled_marker: RecalledMemoryMarkerDep,
    audit_archiver: DeletionAuditArchiverDep,
    settings: SettingsDep,
) -> Response:
    """Delete a single owned long-term memory, creating a tombstone before upstream removal."""
    outcome = await memories_service.delete_single_memory(
        session_factory,
        memory_provider=memory_provider,
        user_id=auth.user_id,
        memory_id=memory_id,
        recalled_memory_marker=recalled_marker,
        audit_archiver=audit_archiver,
        max_attempts=settings.worker_max_attempts,
        reconcile_delay_seconds=settings.worker_reconcile_delay_seconds,
    )
    return _build_deletion_response(outcome)


@router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": ApiResponse[OperationAcceptedResponse],
            "description": "Clearing barrier active; converging writes or remote clear.",
        }
    },
)
async def clear_memories(
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    memory_provider: MemoryProviderDep,
    recalled_marker: RecalledMemoryMarkerDep,
    audit_archiver: DeletionAuditArchiverDep,
    settings: SettingsDep,
    confirm: Annotated[bool, Query()] = False,
) -> Response:
    """Clear all long-term memories for the authenticated user with `confirm=true`."""
    outcome = await memories_service.clear_user_memories(
        session_factory,
        memory_provider=memory_provider,
        user_id=auth.user_id,
        confirm=confirm,
        recalled_memory_marker=recalled_marker,
        audit_archiver=audit_archiver,
        max_attempts=settings.worker_max_attempts,
        reconcile_delay_seconds=settings.worker_reconcile_delay_seconds,
    )
    return _build_deletion_response(outcome)
