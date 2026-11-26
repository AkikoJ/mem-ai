"""FastAPI auth dependencies verifying built-in Bearer JWT credentials against active users."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Annotated, cast

from fastapi import Depends, Request

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.core.logging import bind_log_context
from mem_ai.modules.auth import service as auth_service
from mem_ai.modules.auth.context import AuthenticationContext


def _has_deletion_receipt_scheme(authorization_headers: list[str]) -> bool:
    return any(
        header.split(maxsplit=1)[0].lower() == "deletionreceipt"
        for header in authorization_headers
        if header.strip()
    )


def parse_deletion_receipt_authorization(request: Request) -> str | None:
    """Extract a single `Authorization: DeletionReceipt <token>` credential, if present."""
    authorization_headers = request.headers.getlist("authorization")
    if not _has_deletion_receipt_scheme(authorization_headers):
        return None
    parts = authorization_headers[0].split()
    if len(authorization_headers) != 1 or len(parts) != 2:
        raise UnauthorizedError(
            code="invalid_deletion_receipt",
            message="invalid deletion receipt",
        )
    return parts[1]


@dataclass(frozen=True, slots=True)
class DeletionReceiptAccess:
    """Operation-scoped deletion receipt credential."""

    receipt: str


@dataclass(frozen=True, slots=True)
class UserOperationAccess:
    """Authenticated active-user access to owned operations."""

    auth: AuthenticationContext


OperationAuthContext = DeletionReceiptAccess | UserOperationAccess


async def get_auth_context(
    request: Request,
    session_factory: SessionFactoryDep,
) -> AuthenticationContext:
    """Authenticate a built-in Bearer Access Token and resolve an active local user context."""
    authorization_headers = request.headers.getlist("authorization")
    if _has_deletion_receipt_scheme(authorization_headers):
        raise UnauthorizedError(
            code="invalid_auth_scheme",
            message="deletion receipts can only query their deletion operation",
        )

    bearer_token = auth_service.extract_bearer_token(authorization_headers)
    settings = cast(Settings, request.app.state.settings)
    context = await auth_service.authenticate_builtin_access_token(
        session_factory,
        bearer_token=bearer_token,
        jwt_secret=settings.auth_jwt_secret.get_secret_value().strip(),
        leeway_seconds=settings.auth_jwt_leeway_seconds,
    )
    bind_log_context(user_id=str(context.user_id))
    return context


AuthContextDep = Annotated[AuthenticationContext, Depends(get_auth_context)]


async def _resolve_auth_context(
    request: Request,
    session_factory: SessionFactoryDep,
) -> AuthenticationContext:
    """Invoke `get_auth_context` while respecting any FastAPI dependency override."""
    override = request.app.dependency_overrides.get(get_auth_context)
    if override is not None:
        available = {
            "request": request,
            "session_factory": session_factory,
        }
        params = inspect.signature(override).parameters
        kwargs = {name: available[name] for name in params if name in available}
        resolved = override(**kwargs) if len(kwargs) == len(params) else override()
        auth = await resolved if inspect.isawaitable(resolved) else resolved
        return cast(AuthenticationContext, auth)
    return await get_auth_context(request, session_factory)


async def get_optional_auth_context(
    request: Request,
    session_factory: SessionFactoryDep,
) -> AuthenticationContext | None:
    """Resolve auth when an Authorization header is present; otherwise return None."""
    if not request.headers.getlist("authorization"):
        return None
    return await _resolve_auth_context(request, session_factory)


OptionalAuthContextDep = Annotated[
    AuthenticationContext | None,
    Depends(get_optional_auth_context),
]


async def get_operation_auth_context(
    request: Request,
    session_factory: SessionFactoryDep,
) -> OperationAuthContext:
    """Resolve either an operation-scoped deletion receipt or standard active-user auth."""
    receipt = parse_deletion_receipt_authorization(request)
    if receipt is not None:
        return DeletionReceiptAccess(receipt=receipt)
    auth = await _resolve_auth_context(request, session_factory)
    return UserOperationAccess(auth=auth)


OperationAuthContextDep = Annotated[
    OperationAuthContext,
    Depends(get_operation_auth_context),
]
