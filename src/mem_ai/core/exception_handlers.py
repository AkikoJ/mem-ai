"""FastAPI exception handlers returning the unified nested JSON error envelope."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from mem_ai.core.exceptions import AppError
from mem_ai.core.logging import get_request_id

logger = logging.getLogger("mem_ai.errors")

_HTTP_STATUS_CODES: dict[int, str] = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    422: "validation_error",
    429: "rate_limit_exceeded",
    500: "internal_server_error",
    502: "upstream_error",
    503: "service_unavailable",
    504: "gateway_timeout",
}


def _resolve_request_id(request: Request) -> str:
    state_id = getattr(request.state, "request_id", None)
    if isinstance(state_id, str) and state_id:
        return state_id
    ctx_id = get_request_id()
    return ctx_id if ctx_id else "req_unknown"


def build_error_envelope(
    *,
    code: str,
    message: str,
    request_id: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct the canonical nested error response payload."""
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
            "details": details if details is not None else {},
        }
    }


def register_exception_handlers(app: FastAPI) -> None:
    """Register canonical error handlers on the FastAPI application."""

    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        request_id = _resolve_request_id(request)
        return JSONResponse(
            status_code=exc.status_code,
            content=build_error_envelope(
                code=exc.code,
                message=exc.message,
                request_id=request_id,
                details=exc.details,
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        request_id = _resolve_request_id(request)
        sanitized_errors = [
            {
                "loc": [str(part) for part in err.get("loc", ())],
                "msg": str(err.get("msg", "invalid value")),
                "type": str(err.get("type", "value_error")),
            }
            for err in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=build_error_envelope(
                code="validation_error",
                message="request validation failed",
                request_id=request_id,
                details={"errors": sanitized_errors},
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        request_id = _resolve_request_id(request)
        code = _HTTP_STATUS_CODES.get(exc.status_code, "http_error")
        message = exc.detail if isinstance(exc.detail, str) else code.replace("_", " ")
        return JSONResponse(
            status_code=exc.status_code,
            content=build_error_envelope(
                code=code,
                message=message,
                request_id=request_id,
                details={},
            ),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        request_id = _resolve_request_id(request)
        settings = getattr(request.app.state, "settings", None)
        header_name = getattr(settings, "trusted_request_id_header", "X-Request-Id")
        logger.error(
            "unhandled exception during request",
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={
                "extra_fields": {
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "error_code": "internal_server_error",
                }
            },
        )
        return JSONResponse(
            status_code=500,
            content=build_error_envelope(
                code="internal_server_error",
                message="internal server error",
                request_id=request_id,
                details={},
            ),
            headers={header_name: request_id},
        )
