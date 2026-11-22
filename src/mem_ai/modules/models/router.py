"""HTTP routes for server-registered model discovery (`GET /v1/models`)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from mem_ai.api.dependencies import SessionFactoryDep, SettingsDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.models.schemas import ModelItemResponse, ModelListResponse
from mem_ai.modules.models.service import (
    list_available_models,
    model_routing_policy_from_settings,
)

router = APIRouter(prefix="/models", tags=["models"])


@router.get("", response_model=ApiResponse[ModelListResponse])
async def list_models(
    auth: AuthContextDep,
    settings: SettingsDep,
    session_factory: SessionFactoryDep,
    supports_streaming: Annotated[bool | None, Query(alias="supportsStreaming")] = None,
) -> ApiResponse[ModelListResponse]:
    """List enabled server-registered models available to the authenticated user."""
    policy = model_routing_policy_from_settings(settings)
    records = await list_available_models(
        session_factory,
        user_permissions=auth.permissions,
        policy=policy,
        supports_streaming=supports_streaming,
    )
    payload = ModelListResponse(items=[ModelItemResponse.from_record(item) for item in records])
    return ok_response(payload)
