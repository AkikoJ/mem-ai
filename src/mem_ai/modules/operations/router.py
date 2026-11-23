"""HTTP routes for querying owned asynchronous operation status."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path, Request

from mem_ai.api.dependencies import SessionFactoryDep, SettingsDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.modules.auth.dependencies import CredentialVerifierDep, get_auth_context
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
    request: Request,
    verifier: CredentialVerifierDep,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ApiResponse[OperationResponse]:
    """Read an owned operation or the single deletion operation authorized by its receipt."""
    authorization_headers = request.headers.getlist("authorization")
    receipt_scheme = any(
        header.split(maxsplit=1)[0].lower() == "deletionreceipt"
        for header in authorization_headers
        if header.strip()
    )
    if receipt_scheme:
        parts = authorization_headers[0].split()
        if len(authorization_headers) != 1 or len(parts) != 2:
            raise UnauthorizedError(
                code="invalid_deletion_receipt", message="invalid deletion receipt"
            )
        record = await operations_service.get_operation_for_deletion_receipt(
            session_factory,
            operation_id=operation_id,
            receipt=parts[1],
            settings=settings,
        )
    else:
        auth = await get_auth_context(request, verifier, session_factory)
        record = await operations_service.get_operation_for_owner(
            session_factory,
            owner_id=auth.user_id,
            operation_id=operation_id,
        )
    return ok_response(OperationResponse.from_record(record))
