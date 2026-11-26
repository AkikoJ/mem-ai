"""HTTP middleware for request IDs, Host/CORS/proxy boundaries, budgets, logs, and metrics."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mem_ai.core.config import ProxyNetwork, Settings, is_ip_in_networks
from mem_ai.core.exception_handlers import build_error_envelope
from mem_ai.core.exceptions import PayloadTooLargeError
from mem_ai.core.limits import (
    ConcurrencyLimiter,
    NoopRateLimiter,
    RateLimiter,
    RateLimitRequest,
    compute_retry_after_seconds,
)
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.core.telemetry import (
    NoopTracer,
    TelemetryRegistry,
    Tracer,
    bind_telemetry,
    reset_telemetry,
)

logger = logging.getLogger("mem_ai.access")

_REQUEST_ID_SAFE_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_ALWAYS_UNTRUSTED_IDENTITY_HEADERS = frozenset(
    {
        b"x-user-id",
        b"x-actor-id",
        b"x-forwarded-user",
        b"x-remote-user",
        b"x-gateway-auth",
        b"x-auth-issuer",
        b"x-auth-subject",
        b"x-auth-actor-id",
        b"x-auth-permissions",
    }
)
_UNTRUSTED_PROXY_FORWARDING_HEADERS = frozenset(
    {
        b"x-forwarded-for",
        b"x-forwarded-proto",
        b"x-forwarded-host",
        b"x-real-ip",
        b"forwarded",
    }
)
_OPERATIONAL_PATHS = frozenset({"/health/live", "/health/ready", "/metrics"})
_UUID_SEGMENT_PATTERN = re.compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?=/|$)"
)
_NOOP_RATE_LIMITER = NoopRateLimiter()


def _normalize_rate_limit_path(path: str) -> str:
    """Replace UUID path segments with `{id}` so callers cannot bypass path-scoped budgets."""
    return _UUID_SEGMENT_PATTERN.sub("/{id}", path)


def _client_host_from_scope(scope: Scope) -> str | None:
    client = scope.get("client")
    if client and isinstance(client[0], str):
        return client[0]
    return None


def _request_id_from_scope(scope: Scope) -> str:
    state = scope.get("state")
    if isinstance(state, dict):
        rid = state.get("request_id")
        if isinstance(rid, str) and rid:
            return rid
    return "req_unknown"


def _resolve_canonical_route(scope: Scope) -> str:
    fastapi_meta = scope.get("fastapi")
    if isinstance(fastapi_meta, dict):
        ctx = fastapi_meta.get("effective_route_context")
        ctx_path = getattr(ctx, "path", None)
        if isinstance(ctx_path, str) and ctx_path:
            return ctx_path
    route = scope.get("route")
    route_path = getattr(route, "path", None)
    if isinstance(route_path, str) and route_path:
        return route_path
    raw_path = str(scope.get("path", ""))
    if raw_path in _OPERATIONAL_PATHS:
        return raw_path
    return "unmatched"


async def _send_json_error(
    send: Send,
    *,
    status_code: int,
    code: str,
    message: str,
    request_id: str,
    details: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> None:
    payload = build_error_envelope(
        code=code,
        message=message,
        request_id=request_id,
        details=details,
    )
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    if extra_headers:
        for name, value in extra_headers.items():
            headers.append((name.lower().encode("latin-1"), value.encode("latin-1")))
    await send({"type": "http.response.start", "status": status_code, "headers": headers})
    await send({"type": "http.response.body", "body": body})


def _normalize_host_header(raw_host: str) -> str | None:
    candidate = raw_host.strip().lower()
    if not candidate or any(ch.isspace() for ch in candidate) or "/" in candidate:
        return None
    if candidate.startswith("["):
        end_bracket = candidate.find("]")
        if end_bracket <= 1:
            return None
        return candidate[1:end_bracket]
    if ":" in candidate:
        host_part, port_part = candidate.rsplit(":", 1)
        if not port_part.isdigit():
            return None
        return host_part
    return candidate


def _is_host_allowed(host: str | None, allowed_patterns: tuple[str, ...]) -> bool:
    if not host:
        return False
    for pattern in allowed_patterns:
        if pattern == "*" or host == pattern:
            return True
        if pattern.startswith("*.") and len(pattern) > 2:
            suffix = pattern[1:]  # e.g. ".example.com"
            if host.endswith(suffix) and len(host) > len(suffix):
                return True
    return False


class AllowedHostMiddleware:
    """Enforce configured Host allowlist and reject unknown hosts with unified JSON 400."""

    def __init__(self, app: ASGIApp, *, allowed_hosts: list[str]) -> None:
        self.app = app
        self._allowed_patterns = tuple(
            host.strip().lower() for host in allowed_hosts if host.strip()
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and self._allowed_patterns:
            host_values = [
                value.decode("latin-1", errors="ignore")
                for name, value in scope.get("headers", ())
                if name.lower() == b"host"
            ]
            # Reject missing or duplicate Host headers as well as disallowed hostnames.
            normalized = _normalize_host_header(host_values[0]) if len(host_values) == 1 else None
            if not _is_host_allowed(normalized, self._allowed_patterns):
                await _send_json_error(
                    send,
                    status_code=400,
                    code="invalid_host",
                    message="invalid host header",
                    request_id=_request_id_from_scope(scope),
                )
                return

        await self.app(scope, receive, send)


class IdentityHeaderSanitizerMiddleware:
    """Strip forged identity headers always and proxy forwarding headers from untrusted peers."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self._trusted_proxies = settings.trusted_proxy_networks()
        self._all_spoofable = (
            _ALWAYS_UNTRUSTED_IDENTITY_HEADERS | _UNTRUSTED_PROXY_FORWARDING_HEADERS
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            client_host = _client_host_from_scope(scope)
            is_trusted_peer = is_ip_in_networks(client_host, self._trusted_proxies)
            strip_headers = (
                _ALWAYS_UNTRUSTED_IDENTITY_HEADERS if is_trusted_peer else self._all_spoofable
            )

            raw_headers = scope.get("headers")
            if raw_headers:
                scope["headers"] = [
                    (name, value)
                    for name, value in raw_headers
                    if name.lower() not in strip_headers
                ]

        await self.app(scope, receive, send)


class ResourceBudgetMiddleware:
    """Enforce replaceable rate limiter, process request concurrency, and request body byte cap."""

    def __init__(self, app: ASGIApp, *, settings: Settings) -> None:
        self.app = app
        self._max_body_bytes = settings.max_request_body_bytes
        self._default_retry_after = settings.rate_limit_retry_after_seconds

    async def _send_payload_too_large(
        self,
        send: Send,
        *,
        request_id: str,
        telemetry: TelemetryRegistry | None,
        record_metric: bool,
    ) -> None:
        if record_metric and telemetry is not None:
            telemetry.record_rate_limit_rejection(
                scope="request_body",
                reason="payload_too_large",
            )
        await _send_json_error(
            send,
            status_code=413,
            code="payload_too_large",
            message="request body exceeds maximum allowed size",
            request_id=request_id,
            details={"max_request_body_bytes": self._max_body_bytes},
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path", ""))
        method = str(scope.get("method", "GET")).upper()
        app_ref = scope.get("app")
        app_state = getattr(app_ref, "state", None)
        telemetry: TelemetryRegistry | None = getattr(app_state, "telemetry", None)
        request_id = _request_id_from_scope(scope)

        # 1. Request body byte budget (fast Content-Length check + streaming chunk guard).
        content_length: int | None = None
        for name, value in scope.get("headers", ()):
            if name.lower() == b"content-length":
                try:
                    content_length = int(value.decode("ascii").strip())
                except ValueError:
                    content_length = None
                break
        if content_length is not None and content_length > self._max_body_bytes:
            await self._send_payload_too_large(
                send, request_id=request_id, telemetry=telemetry, record_metric=True
            )
            return

        # 2. Replaceable rate limiter and process HTTP concurrency budget (non-operational routes).
        permit = None
        if path not in _OPERATIONAL_PATHS:
            rate_limiter: RateLimiter = getattr(app_state, "rate_limiter", _NOOP_RATE_LIMITER)
            client_ip = _client_host_from_scope(scope)
            normalized_path = _normalize_rate_limit_path(path)
            decision = await rate_limiter.check(
                RateLimitRequest(
                    method=method,
                    path=normalized_path,
                    client_ip=client_ip,
                    key=f"{client_ip or 'unknown'}:{method}:{normalized_path}",
                )
            )
            if not decision.allowed:
                retry_after = compute_retry_after_seconds(
                    decision.retry_after_seconds, default=self._default_retry_after
                )
                if telemetry is not None:
                    telemetry.record_rate_limit_rejection(
                        scope=decision.scope,
                        reason="rate_limit_exceeded",
                    )
                details: dict[str, Any] = {
                    "limit_scope": decision.scope,
                    "retry_after_seconds": retry_after,
                }
                if decision.limit is not None:
                    details["limit"] = decision.limit
                await _send_json_error(
                    send,
                    status_code=429,
                    code="rate_limit_exceeded",
                    message="rate limit exceeded",
                    request_id=request_id,
                    details=details,
                    extra_headers={"Retry-After": str(retry_after)},
                )
                return

            concurrency_limiter: ConcurrencyLimiter | None = getattr(
                app_state, "request_concurrency_limiter", None
            )
            if concurrency_limiter is not None:
                permit = concurrency_limiter.try_acquire()
                if permit is None:
                    retry_after = compute_retry_after_seconds(
                        None, default=self._default_retry_after
                    )
                    if telemetry is not None:
                        telemetry.record_rate_limit_rejection(
                            scope="request_concurrency",
                            reason="request_concurrency_limit_exceeded",
                        )
                    await _send_json_error(
                        send,
                        status_code=429,
                        code="request_concurrency_limit_exceeded",
                        message="server request concurrency budget exceeded",
                        request_id=request_id,
                        details={
                            "limit_scope": "http_requests",
                            "max_in_flight": concurrency_limiter.max_in_flight,
                            "retry_after_seconds": retry_after,
                        },
                        extra_headers={"Retry-After": str(retry_after)},
                    )
                    return

        received_bytes = 0
        max_body_bytes = self._max_body_bytes
        body_exceeded = False
        response_started = False

        async def guarded_receive() -> Message:
            nonlocal received_bytes, body_exceeded
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                received_bytes += len(chunk)
                if received_bytes > max_body_bytes:
                    body_exceeded = True
                    if telemetry is not None:
                        telemetry.record_rate_limit_rejection(
                            scope="request_body",
                            reason="payload_too_large",
                        )
                    raise PayloadTooLargeError(max_bytes=max_body_bytes)
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal response_started
            if body_exceeded and not response_started:
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            try:
                await self.app(scope, guarded_receive, guarded_send)
            except PayloadTooLargeError:
                body_exceeded = True
            if body_exceeded and not response_started:
                await self._send_payload_too_large(
                    send, request_id=request_id, telemetry=telemetry, record_metric=False
                )
        finally:
            if permit is not None:
                permit.release()


def _extract_or_generate_request_id(
    scope: Scope,
    *,
    header_name: str,
    trust_from_proxies_only: bool,
    trusted_proxies: tuple[ProxyNetwork, ...],
) -> str:
    client_host = _client_host_from_scope(scope)
    if trust_from_proxies_only and not is_ip_in_networks(client_host, trusted_proxies):
        return f"req_{uuid4().hex}"

    target = header_name.lower().encode("latin-1")
    for raw_name, raw_value in scope.get("headers", ()):
        if raw_name.lower() == target:
            candidate = str(raw_value.decode("latin-1", errors="ignore")).strip()
            if _REQUEST_ID_SAFE_PATTERN.fullmatch(candidate):
                return candidate
            break
    return f"req_{uuid4().hex}"


class RequestContextMiddleware:
    """ASGI middleware binding request_id, telemetry, tracing, X-Request-Id, and access logs."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        request_id_header: str = "X-Request-Id",
        trust_from_proxies_only: bool = False,
        trusted_proxies: tuple[ProxyNetwork, ...] = (),
    ) -> None:
        self.app = app
        self.request_id_header = request_id_header
        self._header_bytes = request_id_header.lower().encode("latin-1")
        self._trust_from_proxies_only = trust_from_proxies_only
        self._trusted_proxies = trusted_proxies

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _extract_or_generate_request_id(
            scope,
            header_name=self.request_id_header,
            trust_from_proxies_only=self._trust_from_proxies_only,
            trusted_proxies=self._trusted_proxies,
        )
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        log_tokens = bind_log_context(
            request_id=request_id,
            user_id=None,
            conversation_id=None,
            generation_id=None,
            provider=None,
            job_id=None,
        )

        app_ref = scope.get("app")
        app_state = getattr(app_ref, "state", None)
        telemetry: TelemetryRegistry | None = getattr(app_state, "telemetry", None)
        tracer: Tracer = getattr(app_state, "tracer", NoopTracer())
        tel_tokens = bind_telemetry(telemetry=telemetry, tracer=tracer)

        method = str(scope.get("method", "GET")).upper()
        start = time.perf_counter()
        status_code = 500
        if telemetry is not None:
            telemetry.inc_gauge(
                "mem_ai_http_requests_in_flight",
                help_text="Current number of in-flight HTTP requests in this API process.",
            )

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
            with tracer.start_span(
                "http.request",
                attributes={"http.method": method, "request_id": request_id},
            ) as span:
                try:
                    await self.app(scope, receive, send_with_request_id)
                finally:
                    route = _resolve_canonical_route(scope)
                    span.set_attribute("http.route", route)
                    span.set_attribute("http.status_code", status_code)
                    if status_code >= 500:
                        span.set_status("error", f"http_{status_code}")
                    else:
                        span.set_status("ok")
        finally:
            duration_seconds = max(0.0, time.perf_counter() - start)
            duration_ms = round(duration_seconds * 1000.0, 2)
            route = _resolve_canonical_route(scope)
            if telemetry is not None:
                telemetry.dec_gauge("mem_ai_http_requests_in_flight")
                telemetry.record_http_request(
                    method=method,
                    route=route,
                    status_code=status_code,
                    duration_seconds=duration_seconds,
                )
            logger.info(
                "http request completed",
                extra={
                    "extra_fields": {
                        "method": method,
                        "path": scope.get("path", ""),
                        "route": route,
                        "status_code": status_code,
                        "duration_ms": duration_ms,
                    }
                },
            )
            reset_telemetry(tel_tokens)
            reset_log_context(log_tokens)


def register_middleware(app: FastAPI, settings: Settings) -> None:
    """Attach request context, Host allowlist, CORS, header sanitization, and resource budgets."""
    # Starlette executes middleware in LIFO order (last added is outermost).
    app.add_middleware(ResourceBudgetMiddleware, settings=settings)
    app.add_middleware(IdentityHeaderSanitizerMiddleware, settings=settings)
    if settings.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    if settings.allowed_hosts:
        app.add_middleware(AllowedHostMiddleware, allowed_hosts=settings.allowed_hosts)
    app.add_middleware(
        RequestContextMiddleware,
        request_id_header=settings.trusted_request_id_header,
        trust_from_proxies_only=settings.effective_trust_request_id_from_proxies_only(),
        trusted_proxies=settings.trusted_proxy_networks(),
    )
