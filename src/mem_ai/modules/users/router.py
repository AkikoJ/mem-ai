"""HTTP routes for the current authenticated user."""

from __future__ import annotations

from fastapi import APIRouter

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.users import service as users_service
from mem_ai.modules.users.schemas import UserMeResponse

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=ApiResponse[UserMeResponse])
async def get_current_user(
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[UserMeResponse]:
    """Return the authenticated local user resolved solely from trusted credentials."""
    user = await users_service.get_active_user(session_factory, user_id=auth.user_id)
    payload = UserMeResponse(
        user_id=user.id,
        actor_id=auth.actor_id,
        auth_method=auth.auth_method,
        permissions=sorted(auth.permissions),
        status=user.status,
        created_at=user.created_at,
    )
    return ok_response(payload)
