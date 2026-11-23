"""FastAPI auth dependencies resolving layer-1 credentials into layer-2 local users."""

from __future__ import annotations

from typing import Annotated, cast

from fastapi import Depends, Request

from mem_ai.api.dependencies import SessionFactoryDep
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.core.logging import bind_log_context
from mem_ai.modules.auth.adapters import AuthRequest, CredentialVerifier
from mem_ai.modules.auth.context import AuthenticationContext
from mem_ai.modules.users import service as users_service


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
    if any(
        header.split(maxsplit=1)[0].lower() == "deletionreceipt"
        for header in request.headers.getlist("authorization")
        if header.strip()
    ):
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
