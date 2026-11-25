"""HTTP routes for chat orchestration (`POST /v1/chat`) and generation status queries."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse

from mem_ai.api.dependencies import (
    DeletionAuditArchiverDep,
    GenerationLimiterDep,
    ModelProviderDep,
    OptionalMemoryProviderDep,
    SessionFactoryDep,
    SettingsDep,
)
from mem_ai.api.pagination import DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, PaginatedResponse
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.core.exceptions import UnprocessableEntityError
from mem_ai.core.limits import ConcurrencyPermit
from mem_ai.core.logging import bind_log_context
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.conversations import cleanup as conversation_cleanup
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations import stream as conversations_stream
from mem_ai.modules.conversations.execution import SyncGenerationTaskRegistry
from mem_ai.modules.conversations.schemas import (
    ChatAcceptedResponse,
    ChatAcceptedResult,
    ChatRequest,
    ChatResponse,
    ConversationResponse,
    GenerationResponse,
    HistoryMessageResponse,
    PreparedGeneration,
)
from mem_ai.modules.conversations.service import chat_options_from_settings
from mem_ai.modules.operations.schemas import OperationAcceptedResponse

router = APIRouter(tags=["chat"])

ConversationIdPath = Annotated[UUID, Path(alias="conversationId")]
GenerationIdPath = Annotated[UUID, Path(alias="generationId")]


@router.delete(
    "/conversations/{conversationId}",
    tags=["conversations"],
    response_model=ApiResponse[OperationAcceptedResponse],
    status_code=202,
)
async def delete_conversation(
    conversation_id: ConversationIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    memory_provider: OptionalMemoryProviderDep,
    audit_archiver: DeletionAuditArchiverDep,
    response: Response,
) -> ApiResponse[OperationAcceptedResponse]:
    """Hide an owned conversation immediately and return its durable cleanup operation."""
    operation = await conversation_cleanup.delete_conversation_for_owner(
        session_factory,
        owner_id=auth.user_id,
        conversation_id=conversation_id,
        memory_provider=memory_provider,
        audit_archiver=audit_archiver,
    )
    response.headers["Location"] = operation.status_url
    return ok_response(OperationAcceptedResponse.from_record(operation))


_IDEMPOTENCY_OPENAPI_EXTRA: dict[str, object] = {
    "parameters": [
        {
            "name": "Idempotency-Key",
            "in": "header",
            "required": True,
            "schema": {"type": "string", "minLength": 1, "maxLength": 255},
            "description": "Required idempotency key for safe chat retries.",
        }
    ]
}


def get_sync_generation_task_registry(request: Request) -> SyncGenerationTaskRegistry | None:
    """Return the application-managed registry tracking shielded sync generation tasks."""
    registry = getattr(request.app.state, "sync_generation_tasks", None)
    if isinstance(registry, SyncGenerationTaskRegistry):
        return registry
    return None


SyncTaskRegistryDep = Annotated[
    SyncGenerationTaskRegistry | None,
    Depends(get_sync_generation_task_registry),
]


@router.get("/conversations", response_model=ApiResponse[PaginatedResponse[ConversationResponse]])
async def list_conversations(
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
) -> ApiResponse[PaginatedResponse[ConversationResponse]]:
    """List active conversations belonging to the authenticated user."""
    return ok_response(
        await conversation_queries.list_conversations_for_owner(
            session_factory,
            owner_id=auth.user_id,
            limit=limit,
            cursor=cursor,
        )
    )


@router.get("/conversations/{conversationId}", response_model=ApiResponse[ConversationResponse])
async def get_conversation(
    conversation_id: ConversationIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[ConversationResponse]:
    """Get an owned conversation and its latest turn status."""
    return ok_response(
        await conversation_queries.get_conversation_for_owner(
            session_factory,
            owner_id=auth.user_id,
            conversation_id=conversation_id,
        )
    )


@router.get(
    "/conversations/{conversationId}/messages",
    response_model=ApiResponse[PaginatedResponse[HistoryMessageResponse]],
)
async def list_messages(
    conversation_id: ConversationIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
) -> ApiResponse[PaginatedResponse[HistoryMessageResponse]]:
    """Get a page of actual messages and the status of each message's generation."""
    return ok_response(
        await conversation_queries.page_messages_for_owner(
            session_factory,
            owner_id=auth.user_id,
            conversation_id=conversation_id,
            limit=limit,
            cursor=cursor,
        )
    )


@router.post(
    "/chat",
    response_model=ApiResponse[ChatResponse],
    openapi_extra=_IDEMPOTENCY_OPENAPI_EXTRA,
    responses={
        status.HTTP_200_OK: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            },
            "description": (
                "Committed JSON response for sync chat or idempotent replay, "
                "or Server-Sent Events stream for new `stream=true` turns."
            ),
        },
        status.HTTP_202_ACCEPTED: {
            "model": ApiResponse[ChatAcceptedResponse],
            "description": "Idempotent retry matched a generation that is still running.",
        },
    },
)
async def post_chat(
    request: Request,
    payload: ChatRequest,
    auth: AuthContextDep,
    settings: SettingsDep,
    session_factory: SessionFactoryDep,
    model_provider: ModelProviderDep,
    memory_provider: OptionalMemoryProviderDep,
    task_registry: SyncTaskRegistryDep,
    generation_limiter: GenerationLimiterDep,
) -> ApiResponse[ChatResponse] | Response:
    """Start, continue, stream, or idempotently replay a conversation turn."""
    idempotency_key = request.headers.get("Idempotency-Key")
    if idempotency_key is None:
        raise UnprocessableEntityError(
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
    options = chat_options_from_settings(
        settings,
        user_permissions=auth.permissions,
        require_streaming=payload.stream,
        task_registry=task_registry,
        generation_limiter=generation_limiter,
    )
    acquired_permit: list[ConcurrencyPermit] = []
    prepared_or_replay = await conversations_service.prepare_chat_turn(
        session_factory,
        memory_provider=memory_provider,
        owner_id=auth.user_id,
        agent_id=payload.agent_id,
        conversation_id=payload.conversation_id,
        user_message=payload.message,
        idempotency_key=idempotency_key,
        requested_model_id=payload.model_id,
        stream=payload.stream,
        allow_streaming=True,
        options=options,
        on_generation_permit=acquired_permit.append,
    )
    permit = acquired_permit[0] if acquired_permit else None

    if isinstance(prepared_or_replay, ChatAcceptedResult):
        bind_log_context(
            conversation_id=str(prepared_or_replay.conversation_id),
            generation_id=str(prepared_or_replay.generation_id),
        )
        if permit is not None:
            permit.release()
        accepted = ok_response(ChatAcceptedResponse.from_result(prepared_or_replay))
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=accepted.model_dump(mode="json", by_alias=True),
            headers={"Location": prepared_or_replay.status_url},
        )
    if not isinstance(prepared_or_replay, PreparedGeneration):
        bind_log_context(
            conversation_id=str(prepared_or_replay.conversation_id),
            generation_id=str(prepared_or_replay.generation_id),
        )
        if permit is not None:
            permit.release()
        return ok_response(ChatResponse.from_result(prepared_or_replay))

    bind_log_context(
        conversation_id=str(prepared_or_replay.conversation_id),
        generation_id=str(prepared_or_replay.generation_id),
    )

    if payload.stream:
        return conversations_stream.build_chat_sse_response(
            session_factory,
            model_provider=model_provider,
            prepared=prepared_or_replay,
            memory_provider=memory_provider,
            options=options,
            on_finished=permit.release if permit is not None else None,
        )

    outcome = await conversations_service.execute_prepared_sync_chat(
        session_factory,
        model_provider=model_provider,
        memory_provider=memory_provider,
        prepared=prepared_or_replay,
        options=options,
        on_finished=permit.release if permit is not None else None,
    )
    return ok_response(ChatResponse.from_result(outcome))


@router.get(
    "/conversations/{conversationId}/generations/{generationId}",
    tags=["conversations"],
    response_model=ApiResponse[GenerationResponse],
)
async def get_generation(
    conversation_id: ConversationIdPath,
    generation_id: GenerationIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[GenerationResponse]:
    """Query status, committed result, or terminal error for an owned generation."""
    bind_log_context(conversation_id=str(conversation_id), generation_id=str(generation_id))
    detail = await conversation_queries.get_generation_detail_for_owner(
        session_factory,
        owner_id=auth.user_id,
        conversation_id=conversation_id,
        generation_id=generation_id,
    )
    return ok_response(GenerationResponse.from_detail(detail))
