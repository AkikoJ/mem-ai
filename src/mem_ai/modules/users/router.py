"""HTTP routes for the current authenticated user."""

from __future__ import annotations

from fastapi import APIRouter, Response

from mem_ai.api.dependencies import (
    DeletionAuditArchiverDep,
    OptionalMemoryProviderDep,
    SessionFactoryDep,
    SettingsDep,
)
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.operations.schemas import DeletionReceiptResponse
from mem_ai.modules.users import service as users_service
from mem_ai.modules.users.cleanup import delete_user_for_owner
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
        email=user.email,
        auth_method=auth.auth_method,
        status=user.status,
        created_at=user.created_at,
    )
    return ok_response(payload)


@router.delete("/me", status_code=202, response_model=ApiResponse[DeletionReceiptResponse])
async def delete_current_user(
    response: Response,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
    memory_provider: OptionalMemoryProviderDep,
    audit_archiver: DeletionAuditArchiverDep,
) -> ApiResponse[DeletionReceiptResponse]:
    """Accept deletion once; the receipt authorizes only the returned operation URL."""
    receipt = await delete_user_for_owner(
        session_factory,
        owner_id=auth.user_id,
        settings=settings,
        memory_provider=memory_provider,
        audit_archiver=audit_archiver,
    )
    response.headers["Location"] = receipt.status_url
    response.headers["Cache-Control"] = "no-store"
    return ok_response(receipt)
