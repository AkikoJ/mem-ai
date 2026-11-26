"""Resolved built-in authentication context value object."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class AuthenticationContext:
    """Resolved request identity bound to a verified active local user."""

    user_id: UUID
    email: str
    auth_method: str = "builtin_jwt"
    session_id: UUID | None = None
