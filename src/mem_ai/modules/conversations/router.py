"""HTTP routes for synchronous chat orchestration (`POST /v1/chat`)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request

from mem_ai.api.dependencies import (
    ModelProviderDep,
    PersonaProviderDep,
    SessionFactoryDep,
    SettingsDep,
)
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.core.exceptions import UnprocessableEntityError
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.conversations import service as conversations_service
from mem_ai.modules.conversations.schemas import ChatRequest, ChatResponse
from mem_ai.modules.conversations.service import SyncChatOptions, SyncGenerationTaskRegistry

router = APIRouter(tags=["chat"])


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


@router.post("/chat", response_model=ApiResponse[ChatResponse])
async def post_chat(
    payload: ChatRequest,
    auth: AuthContextDep,
    settings: SettingsDep,
    session_factory: SessionFactoryDep,
    persona_provider: PersonaProviderDep,
    model_provider: ModelProviderDep,
    task_registry: SyncTaskRegistryDep,
) -> ApiResponse[ChatResponse]:
    """Start or continue a synchronous conversation turn."""
    if payload.stream:
        raise UnprocessableEntityError(
            code="streaming_not_enabled",
            message="streaming responses are not enabled on this endpoint yet",
        )

    result = await conversations_service.run_sync_chat(
        session_factory,
        persona_provider=persona_provider,
        model_provider=model_provider,
        owner_id=auth.user_id,
        agent_id=payload.agent_id,
        conversation_id=payload.conversation_id,
        user_message=payload.message,
        requested_model_id=payload.model_id,
        options=SyncChatOptions(
            server_fallback_model_id=settings.model_default_id,
            history_limit=settings.chat_history_message_limit,
            lease_ttl_seconds=settings.chat_lease_ttl_seconds,
            timeout_seconds=settings.chat_generation_timeout_seconds,
            task_registry=task_registry,
        ),
    )
    return ok_response(ChatResponse.from_result(result))
