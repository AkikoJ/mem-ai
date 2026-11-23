"""HTTP routes for chat orchestration (`POST /v1/chat`) and generation status queries."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Request, Response, status
from fastapi.responses import JSONResponse

from mem_ai.api.dependencies import (
    ModelProviderDep,
    OptionalMemoryProviderDep,
    PersonaProviderDep,
    SessionFactoryDep,
    SettingsDep,
)
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.core.exceptions import UnprocessableEntityError
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.schemas import (
    ChatAcceptedResponse,
    ChatAcceptedResult,
    ChatRequest,
    ChatResponse,
    GenerationResponse,
)
from mem_ai.modules.conversations.service import (
    SyncGenerationTaskRegistry,
    sync_chat_options_from_settings,
)

router = APIRouter(tags=["chat"])

ConversationIdPath = Annotated[UUID, Path(alias="conversationId")]
GenerationIdPath = Annotated[UUID, Path(alias="generationId")]

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


@router.post(
    "/chat",
    response_model=ApiResponse[ChatResponse],
    openapi_extra=_IDEMPOTENCY_OPENAPI_EXTRA,
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": ApiResponse[ChatAcceptedResponse],
            "description": "Idempotent retry matched a generation that is still running.",
        }
    },
)
async def post_chat(
    request: Request,
    payload: ChatRequest,
    auth: AuthContextDep,
    settings: SettingsDep,
    session_factory: SessionFactoryDep,
    persona_provider: PersonaProviderDep,
    model_provider: ModelProviderDep,
    memory_provider: OptionalMemoryProviderDep,
    task_registry: SyncTaskRegistryDep,
) -> ApiResponse[ChatResponse] | Response:
    """Start, continue, or idempotently replay a conversation turn."""
    idempotency_key = request.headers.get("Idempotency-Key")
    if idempotency_key is None:
        raise UnprocessableEntityError(
            code="idempotency_key_required",
            message="Idempotency-Key header is required",
        )
    outcome = await conversations_service.run_sync_chat(
        session_factory,
        persona_provider=persona_provider,
        model_provider=model_provider,
        memory_provider=memory_provider,
        owner_id=auth.user_id,
        agent_id=payload.agent_id,
        conversation_id=payload.conversation_id,
        user_message=payload.message,
        idempotency_key=idempotency_key,
        requested_model_id=payload.model_id,
        stream=payload.stream,
        options=sync_chat_options_from_settings(
            settings,
            user_permissions=auth.permissions,
            task_registry=task_registry,
        ),
    )
    if isinstance(outcome, ChatAcceptedResult):
        accepted = ok_response(ChatAcceptedResponse.from_result(outcome))
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=accepted.model_dump(mode="json", by_alias=True),
            headers={"Location": outcome.status_url},
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
    detail = await conversation_queries.get_generation_detail_for_owner(
        session_factory,
        owner_id=auth.user_id,
        conversation_id=conversation_id,
        generation_id=generation_id,
    )
    return ok_response(GenerationResponse.from_detail(detail))
