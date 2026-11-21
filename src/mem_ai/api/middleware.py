"""HTTP middleware for request ID correlation, structured access logs, and CORS."""

from __future__ import annotations

import logging
import re
import time
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mem_ai.core.config import Settings, is_ip_in_networks
from mem_ai.core.logging import bind_log_context, reset_log_context

logger = logging.getLogger("mem_ai.access")

_REQUEST_ID_SAFE_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_ALWAYS_UNTRUSTED_IDENTITY_HEADERS = frozenset(
    {
        b"x-user-id",
        b"x-actor-id",
        b"x-forwarded-user",
        b"x-remote-user",
    }
)


class IdentityHeaderSanitizerMiddleware:
    """Strip forged identity headers when requests do not originate from a trusted gateway."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self._trusted_gateway_enabled = settings.auth_mode == "trusted_gateway"
        self._trusted_proxies = settings.trusted_proxy_networks()
        self._gateway_headers = frozenset(
            name.lower().encode("latin-1") for name in settings.gateway_headers().all_names()
        )
        self._non_gateway_untrusted = _ALWAYS_UNTRUSTED_IDENTITY_HEADERS - self._gateway_headers
        self._all_spoofable = _ALWAYS_UNTRUSTED_IDENTITY_HEADERS | self._gateway_headers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            client = scope.get("client")
            client_host = client[0] if client and isinstance(client[0], str) else None
            if self._trusted_gateway_enabled and is_ip_in_networks(
                client_host, self._trusted_proxies
            ):
                strip_headers = self._non_gateway_untrusted
            else:
                strip_headers = self._all_spoofable

            raw_headers = scope.get("headers")
            if raw_headers:
                scope["headers"] = [
                    (name, value)
                    for name, value in raw_headers
                    if name.lower() not in strip_headers
                ]

        await self.app(scope, receive, send)


def _extract_or_generate_request_id(scope: Scope, header_name: str) -> str:
    target = header_name.lower().encode("latin-1")
    for raw_name, raw_value in scope.get("headers", ()):
        if raw_name.lower() == target:
            candidate = str(raw_value.decode("latin-1", errors="ignore")).strip()
            if _REQUEST_ID_SAFE_PATTERN.fullmatch(candidate):
                return candidate
            break
    return f"req_{uuid4().hex}"


class RequestContextMiddleware:
    """ASGI middleware that binds request_id, sets X-Request-Id, and logs HTTP access."""

    def __init__(self, app: ASGIApp, *, request_id_header: str = "X-Request-Id") -> None:
        self.app = app
        self.request_id_header = request_id_header
        self._header_bytes = request_id_header.lower().encode("latin-1")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _extract_or_generate_request_id(scope, self.request_id_header)
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        tokens = bind_log_context(request_id=request_id, user_id=None)

        start = time.perf_counter()
        status_code = 500

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                headers = list(message.get("headers", ()))
                headers = [
                    (name, value) for name, value in headers if name.lower() != self._header_bytes
                ]
                headers.append((self._header_bytes, request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000.0, 2)
            logger.info(
                "http request completed",
                extra={
                    "extra_fields": {
                        "method": scope.get("method", ""),
                        "path": scope.get("path", ""),
                        "status_code": status_code,
                        "duration_ms": duration_ms,
                    }
                },
            )
            reset_log_context(tokens)


def register_middleware(app: FastAPI, settings: Settings) -> None:
    """Attach identity header sanitization, request correlation middleware, and CORS policy."""
    if settings.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.add_middleware(IdentityHeaderSanitizerMiddleware, settings=settings)
    app.add_middleware(
        RequestContextMiddleware,
        request_id_header=settings.trusted_request_id_header,
    )
