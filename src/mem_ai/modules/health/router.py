"""Health check (`/health/live`, `/health/ready`) and controlled `/metrics` endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

from mem_ai.api.dependencies import (
    AppReadyDep,
    OptionalSessionFactoryDep,
    SessionFactoryDep,
    SettingsDep,
    TelemetryDep,
)
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.health.service import (
    ReadinessStatus,
    build_metrics_response,
    check_readiness,
)

router = APIRouter(prefix="/health", tags=["health"])
metrics_router = APIRouter(tags=["observability"])


@router.get("/live", response_model=ApiResponse[None])
async def health_live() -> ApiResponse[None]:
    """Report process liveness without hitting external dependencies."""
    return ok_response(None)


@router.get("/ready", response_model=ApiResponse[ReadinessStatus])
async def health_ready(
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
    app_ready: AppReadyDep,
) -> ApiResponse[ReadinessStatus]:
    """Report readiness after startup completion and bounded DB/migration verification."""
    return ok_response(await check_readiness(session_factory, settings, app_ready=app_ready))


@metrics_router.get("/metrics", include_in_schema=False)
async def get_metrics(
    request: Request,
    settings: SettingsDep,
    telemetry: TelemetryDep,
    session_factory: OptionalSessionFactoryDep,
) -> PlainTextResponse:
    """Expose low-cardinality Prometheus metrics guarded by network allowlist and optional token."""
    return await build_metrics_response(
        request,
        session_factory=session_factory,
        settings=settings,
        telemetry=telemetry,
    )
