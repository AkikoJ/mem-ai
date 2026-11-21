"""Health check HTTP endpoints (`/health/live` and `/health/ready`)."""

from __future__ import annotations

from fastapi import APIRouter

from mem_ai.api.dependencies import SessionFactoryDep, SettingsDep
from mem_ai.modules.health.service import LivenessStatus, ReadinessStatus, check_readiness

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", response_model=LivenessStatus)
async def health_live() -> LivenessStatus:
    """Report process liveness without hitting external dependencies."""
    return LivenessStatus()


@router.get("/ready", response_model=ReadinessStatus)
async def health_ready(
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ReadinessStatus:
    """Report readiness after bounded PostgreSQL and migration revision verification."""
    return await check_readiness(session_factory, settings)
