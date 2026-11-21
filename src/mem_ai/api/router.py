"""Top-level API router aggregating health and versioned `/v1` business routes."""

from __future__ import annotations

from fastapi import APIRouter

from mem_ai.modules.health.router import router as health_router


def build_api_router() -> APIRouter:
    """Assemble root and `/v1` routers."""
    root_router = APIRouter()
    root_router.include_router(health_router)

    v1_router = APIRouter(prefix="/v1")
    root_router.include_router(v1_router)
    return root_router
