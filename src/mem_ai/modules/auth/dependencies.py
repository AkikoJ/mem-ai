"""FastAPI auth dependencies resolving layer-1 credentials into layer-2 local users."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Annotated, cast

from fastapi import Depends, Request

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.core.logging import bind_log_context
from mem_ai.modules.auth.adapters import AuthRequest, CredentialVerifier
from mem_ai.modules.auth.context import AuthenticationContext
from mem_ai.modules.users import service as users_service


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


def get_credential_verifier(request: Request) -> CredentialVerifier:
    """Return the configured CredentialVerifier (overridable via `app.dependency_overrides`)."""
    return cast(CredentialVerifier, request.app.state.credential_verifier)


CredentialVerifierDep = Annotated[CredentialVerifier, Depends(get_credential_verifier)]


async def get_auth_context(
    request: Request,
    verifier: CredentialVerifierDep,
    session_factory: SessionFactoryDep,
) -> AuthenticationContext:
    """Authenticate incoming credentials and resolve an active local user context."""
    if _has_deletion_receipt_scheme(request.headers.getlist("authorization")):
        raise UnauthorizedError(
            code="invalid_auth_scheme",
            message="deletion receipts can only query their deletion operation",
        )
    client_host = request.client.host if request.client is not None else None
    raw_headers = tuple(
        (key.decode("latin-1"), value.decode("latin-1")) for key, value in request.headers.raw
    )
    auth_request = AuthRequest(headers=raw_headers, client_host=client_host)

    # Layer 1: verify credential protocol and obtain a trusted external principal.
    principal = await verifier.verify(auth_request)

    # Layer 2: resolve stable local user mapping and enforce active user status.
    user = await users_service.resolve_active_user(
        session_factory,
        issuer=principal.issuer,
        subject=principal.subject,
    )

    context = AuthenticationContext(
        actor_id=principal.actor_id,
        user_id=user.id,
        auth_method=principal.auth_method,
        permissions=principal.permissions,
    )
    bind_log_context(user_id=str(context.user_id))
    return context


AuthContextDep = Annotated[AuthenticationContext, Depends(get_auth_context)]


async def get_operation_auth_context(
    request: Request,
    verifier: CredentialVerifierDep,
    session_factory: SessionFactoryDep,
) -> OperationAuthContext:
    """Resolve either an operation-scoped deletion receipt or standard active-user auth."""
    receipt = parse_deletion_receipt_authorization(request)
    if receipt is not None:
        return DeletionReceiptAccess(receipt=receipt)
    override = request.app.dependency_overrides.get(get_auth_context)
    if override is not None:
        available = {
            "request": request,
            "verifier": verifier,
            "session_factory": session_factory,
        }
        params = inspect.signature(override).parameters
        kwargs = {name: available[name] for name in params if name in available}
        resolved = override(**kwargs) if len(kwargs) == len(params) else override()
        auth = await resolved if inspect.isawaitable(resolved) else resolved
        return UserOperationAccess(auth=cast(AuthenticationContext, auth))
    auth = await get_auth_context(request, verifier, session_factory)
    return UserOperationAccess(auth=auth)


OperationAuthContextDep = Annotated[
    OperationAuthContext,
    Depends(get_operation_auth_context),
]
