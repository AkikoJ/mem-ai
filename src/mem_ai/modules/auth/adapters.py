"""Layer-1 credential verification protocol and replaceable auth adapters."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from mem_ai.core.config import GatewayHeaders, ProxyNetwork, Settings, is_ip_in_networks
from mem_ai.core.exceptions import UnauthorizedError
from mem_ai.modules.auth.context import VerifiedPrincipal

_PERMISSION_SPLIT_PATTERN = re.compile(r"[\s,]+")
_MAX_IDENTITY_FIELD_LENGTH = 255


@dataclass(frozen=True, slots=True)
class AuthRequest:
    """Framework-agnostic HTTP request metadata inspected by credential verifiers."""

    headers: Mapping[str, str] | Sequence[tuple[str, str]]
    client_host: str | None = None

    def get_header_values(self, name: str) -> tuple[str, ...]:
        """Return all values for `name` using case-insensitive lookup."""
        target = name.lower()
        pairs = self.headers.items() if isinstance(self.headers, Mapping) else self.headers
        return tuple(value for key, value in pairs if key.lower() == target)

    def get_single_header(self, name: str) -> str | None:
        """Return a single header value or raise 401 when duplicate headers are present."""
        values = self.get_header_values(name)
        if not values:
            return None
        if len(values) > 1:
            raise UnauthorizedError(
                code="invalid_credentials",
                message="duplicate authentication header",
            )
        return values[0]


@runtime_checkable
class CredentialVerifier(Protocol):
    """Protocol implemented by development, test, and deployment credential adapters."""

    async def verify(self, request: AuthRequest) -> VerifiedPrincipal:
        """Validate incoming credentials and return a trusted external principal."""
        ...


def _extract_bearer_token(request: AuthRequest) -> str:
    raw = request.get_single_header("Authorization")
    if raw is None or not raw.strip():
        raise UnauthorizedError(
            code="missing_credentials",
            message="missing authorization credentials",
        )
    parts = raw.strip().split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise UnauthorizedError(
            code="invalid_credentials",
            message="invalid bearer authorization header",
        )
    return parts[1].strip()


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    try:
        return base64.urlsafe_b64decode(f"{segment}{padding}".encode("ascii"))
    except Exception as exc:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="malformed credential encoding",
        ) from exc


def _parse_permissions(raw: Any) -> frozenset[str]:
    if raw is None:
        return frozenset()
    if isinstance(raw, str):
        return frozenset(p for p in _PERMISSION_SPLIT_PATTERN.split(raw.strip()) if p)
    if isinstance(raw, (list, tuple, set, frozenset)):
        items: list[str] = []
        for entry in raw:
            if isinstance(entry, str) and entry.strip():
                items.append(entry.strip())
        return frozenset(items)
    return frozenset()


def _validate_identity_length(*, issuer: str, subject: str) -> None:
    if len(issuer) > _MAX_IDENTITY_FIELD_LENGTH or len(subject) > _MAX_IDENTITY_FIELD_LENGTH:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="credential identity claim exceeds maximum length",
        )


class DevelopmentAuthAdapter:
    """Fixed-principal development adapter; strictly forbidden in production."""

    def __init__(
        self,
        *,
        issuer: str,
        subject: str,
        actor_id: str | None = None,
        permissions: Iterable[str] = (),
    ) -> None:
        resolved_actor_id = (
            actor_id.strip() if actor_id and actor_id.strip() else f"{issuer}:{subject}"
        )
        self._principal = VerifiedPrincipal(
            issuer=issuer,
            subject=subject,
            actor_id=resolved_actor_id,
            auth_method="development",
            permissions=frozenset(p.strip() for p in permissions if p.strip()),
        )

    async def verify(self, request: AuthRequest) -> VerifiedPrincipal:
        # Deliberately ignore incoming headers so untrusted headers cannot override dev identity.
        _ = request
        return self._principal


class StaticTokenAuthAdapter:
    """In-memory Bearer token adapter for deterministic multi-subject tests."""

    def __init__(self, tokens: Mapping[str, VerifiedPrincipal]) -> None:
        self._tokens = dict(tokens)

    async def verify(self, request: AuthRequest) -> VerifiedPrincipal:
        token = _extract_bearer_token(request)
        principal = self._tokens.get(token)
        if principal is None:
            raise UnauthorizedError(
                code="invalid_credentials",
                message="invalid bearer token",
            )
        return principal


class JwtHs256AuthAdapter:
    """RFC 7519 HS256 Bearer JWT verifier validating signature, time, issuer, and audience."""

    def __init__(
        self,
        *,
        secret: str,
        trusted_issuers: Iterable[str],
        trusted_audience: str,
        leeway_seconds: int = 30,
    ) -> None:
        self._secret_bytes = secret.encode("utf-8")
        self._trusted_issuers = frozenset(iss.strip() for iss in trusted_issuers if iss.strip())
        self._trusted_audience = trusted_audience.strip()
        self._leeway_seconds = leeway_seconds

    async def verify(self, request: AuthRequest) -> VerifiedPrincipal:
        token = _extract_bearer_token(request)
        segments = token.split(".")
        if len(segments) != 3 or any(not seg for seg in segments):
            raise UnauthorizedError(
                code="invalid_credentials",
                message="malformed JWT token",
            )

        header_seg, payload_seg, sig_seg = segments
        try:
            header = json.loads(_b64url_decode(header_seg).decode("utf-8"))
            payload = json.loads(_b64url_decode(payload_seg).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnauthorizedError(
                code="invalid_credentials",
                message="malformed JWT JSON payload",
            ) from exc

        if not isinstance(header, dict) or not isinstance(payload, dict):
            raise UnauthorizedError(
                code="invalid_credentials",
                message="invalid JWT structure",
            )

        if header.get("alg") != "HS256":
            raise UnauthorizedError(
                code="invalid_credentials",
                message="unsupported JWT signing algorithm",
            )

        signing_input = f"{header_seg}.{payload_seg}".encode("ascii")
        expected_sig = hmac.new(self._secret_bytes, signing_input, hashlib.sha256).digest()
        provided_sig = _b64url_decode(sig_seg)
        if not hmac.compare_digest(expected_sig, provided_sig):
            raise UnauthorizedError(
                code="invalid_credentials",
                message="invalid JWT signature",
            )

        now = time.time()
        exp = payload.get("exp")
        if (
            not isinstance(exp, (int, float))
            or isinstance(exp, bool)
            or not math.isfinite(float(exp))
        ):
            raise UnauthorizedError(
                code="invalid_credentials",
                message="JWT missing finite numeric exp claim",
            )
        if now - self._leeway_seconds >= float(exp):
            raise UnauthorizedError(
                code="expired_credentials",
                message="credentials have expired",
            )

        nbf = payload.get("nbf")
        if nbf is not None:
            if (
                not isinstance(nbf, (int, float))
                or isinstance(nbf, bool)
                or not math.isfinite(float(nbf))
            ):
                raise UnauthorizedError(
                    code="invalid_credentials",
                    message="JWT nbf claim must be finite numeric",
                )
            if now + self._leeway_seconds < float(nbf):
                raise UnauthorizedError(
                    code="invalid_credentials",
                    message="credentials are not yet valid",
                )

        issuer = payload.get("iss")
        if not isinstance(issuer, str) or not issuer.strip():
            raise UnauthorizedError(
                code="invalid_issuer",
                message="JWT missing issuer claim",
            )
        issuer = issuer.strip()
        if issuer not in self._trusted_issuers:
            raise UnauthorizedError(
                code="invalid_issuer",
                message="untrusted credential issuer",
            )

        aud = payload.get("aud")
        aud_matches = (isinstance(aud, str) and aud == self._trusted_audience) or (
            isinstance(aud, list)
            and any(isinstance(item, str) and item == self._trusted_audience for item in aud)
        )
        if not aud_matches:
            raise UnauthorizedError(
                code="invalid_audience",
                message="credential audience mismatch",
            )

        subject = payload.get("sub")
        if not isinstance(subject, str) or not subject.strip():
            raise UnauthorizedError(
                code="invalid_credentials",
                message="JWT missing subject claim",
            )
        subject = subject.strip()
        _validate_identity_length(issuer=issuer, subject=subject)

        raw_actor = payload.get("actor_id")
        actor_id = (
            raw_actor.strip()
            if isinstance(raw_actor, str) and raw_actor.strip()
            else f"{issuer}:{subject}"
        )
        raw_permissions = (
            payload["permissions"]
            if "permissions" in payload and payload["permissions"] is not None
            else payload.get("scope")
        )
        permissions = _parse_permissions(raw_permissions)

        return VerifiedPrincipal(
            issuer=issuer,
            subject=subject,
            actor_id=actor_id,
            auth_method="jwt_hs256",
            permissions=permissions,
        )


class TrustedGatewayAuthAdapter:
    """Explicit trusted-gateway identity adapter enforcing direct peer proxy and shared secret."""

    def __init__(
        self,
        *,
        trusted_proxies: tuple[ProxyNetwork, ...],
        trusted_issuers: Iterable[str],
        shared_secret: str,
        headers: GatewayHeaders | None = None,
    ) -> None:
        self._trusted_proxies = trusted_proxies
        self._trusted_issuers = frozenset(iss.strip() for iss in trusted_issuers if iss.strip())
        self._shared_secret = shared_secret
        self._headers = headers if headers is not None else GatewayHeaders()

    async def verify(self, request: AuthRequest) -> VerifiedPrincipal:
        if not is_ip_in_networks(request.client_host, self._trusted_proxies):
            raise UnauthorizedError(
                code="untrusted_gateway_source",
                message="request did not originate from a trusted gateway proxy",
            )

        provided_secret = request.get_single_header(self._headers.secret)
        if not provided_secret or not hmac.compare_digest(
            provided_secret.strip(), self._shared_secret
        ):
            raise UnauthorizedError(
                code="untrusted_gateway_source",
                message="missing or invalid gateway authentication proof",
            )

        issuer = (request.get_single_header(self._headers.issuer) or "").strip()
        subject = (request.get_single_header(self._headers.subject) or "").strip()
        if not issuer or not subject:
            raise UnauthorizedError(
                code="missing_credentials",
                message="missing gateway identity headers",
            )

        if issuer not in self._trusted_issuers:
            raise UnauthorizedError(
                code="invalid_issuer",
                message="untrusted gateway identity issuer",
            )
        _validate_identity_length(issuer=issuer, subject=subject)

        raw_actor = (request.get_single_header(self._headers.actor) or "").strip()
        actor_id = raw_actor or f"{issuer}:{subject}"
        permissions = _parse_permissions(request.get_single_header(self._headers.permissions))

        return VerifiedPrincipal(
            issuer=issuer,
            subject=subject,
            actor_id=actor_id,
            auth_method="trusted_gateway",
            permissions=permissions,
        )


def build_credential_verifier(settings: Settings) -> CredentialVerifier:
    """Assemble the configured credential verifier adapter from validated settings."""
    if settings.auth_mode == "development":
        if settings.app_env == "production":
            raise ValueError("development authentication adapter is forbidden in production")
        return DevelopmentAuthAdapter(
            issuer=settings.auth_dev_issuer,
            subject=settings.auth_dev_subject,
            actor_id=settings.auth_dev_actor_id,
            permissions=settings.auth_dev_permissions,
        )

    if settings.auth_mode == "jwt_hs256":
        if settings.auth_jwt_hs256_secret is None or settings.auth_trusted_audience is None:
            raise ValueError("jwt_hs256 settings are incomplete")
        return JwtHs256AuthAdapter(
            secret=settings.auth_jwt_hs256_secret.get_secret_value().strip(),
            trusted_issuers=settings.auth_trusted_issuers,
            trusted_audience=settings.auth_trusted_audience,
            leeway_seconds=settings.auth_jwt_leeway_seconds,
        )

    if settings.auth_mode == "trusted_gateway":
        if settings.auth_gateway_shared_secret is None:
            raise ValueError("trusted_gateway shared secret is missing")
        return TrustedGatewayAuthAdapter(
            trusted_proxies=settings.trusted_proxy_networks(),
            trusted_issuers=settings.auth_trusted_issuers,
            shared_secret=settings.auth_gateway_shared_secret.get_secret_value().strip(),
            headers=settings.gateway_headers(),
        )

    raise ValueError(f"unsupported auth_mode: {settings.auth_mode!r}")
