"""Trusted principal and resolved authentication context value objects."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class VerifiedPrincipal:
    """Trusted external subject produced by layer-1 credential verification."""

    issuer: str
    subject: str
    actor_id: str
    auth_method: str
    permissions: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class AuthenticationContext:
    """Resolved request identity mapping an authenticated actor to a stable local user."""

    actor_id: str
    user_id: UUID
    auth_method: str
    permissions: frozenset[str] = frozenset()
    email: str | None = None
    session_id: UUID | None = None

    def has_permission(self, permission: str) -> bool:
        """Return True when this context grants the requested permission."""
        return permission in self.permissions
