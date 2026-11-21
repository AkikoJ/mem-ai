"""HTTP routes for querying owned asynchronous operation status."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth.dependencies import AuthContextDep
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.schemas import OperationResponse

router = APIRouter(prefix="/operations", tags=["operations"])

OperationIdPath = Annotated[UUID, Path(alias="operationId")]


@router.get(
    "/{operationId}",
    response_model=ApiResponse[OperationResponse],
)
async def get_operation(
    operation_id: OperationIdPath,
    auth: AuthContextDep,
    session_factory: SessionFactoryDep,
) -> ApiResponse[OperationResponse]:
    """Return status and progress for an asynchronous operation owned by the current user."""
    record = await operations_service.get_operation_for_owner(
        session_factory,
        owner_id=auth.user_id,
        operation_id=operation_id,
    )
    return ok_response(OperationResponse.from_record(record))
