"""HTTP routes for querying owned asynchronous operation status."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path

from mem_ai.api.dependencies import SessionFactoryDep, SettingsDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth.dependencies import (
    DeletionReceiptAccess,
    OperationAuthContextDep,
)
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
    access: OperationAuthContextDep,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ApiResponse[OperationResponse]:
    """Read an owned operation or the single deletion operation authorized by its receipt."""
    if isinstance(access, DeletionReceiptAccess):
        record = await operations_service.get_operation_for_deletion_receipt(
            session_factory,
            operation_id=operation_id,
            receipt=access.receipt,
            settings=settings,
        )
    else:
        record = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=access.auth.user_id,
            operation_id=operation_id,
        )
    return ok_response(OperationResponse.from_record(record))
