"""Top-level API router aggregating health and versioned `/v1` business routes."""

from __future__ import annotations

from fastapi import APIRouter

from mem_ai.modules.agents.router import router as agents_router
from mem_ai.modules.conversations.router import router as conversations_router
from mem_ai.modules.health.router import router as health_router
from mem_ai.modules.memories.router import router as memories_router
from mem_ai.modules.models.router import router as models_router
from mem_ai.modules.operations.router import router as operations_router
from mem_ai.modules.users.router import router as users_router


def build_api_router() -> APIRouter:
    """Assemble root and `/v1` routers."""
    root_router = APIRouter()
    root_router.include_router(health_router)

    v1_router = APIRouter(prefix="/v1")
    v1_router.include_router(users_router)
    v1_router.include_router(agents_router)
    v1_router.include_router(models_router)
    v1_router.include_router(conversations_router)
    v1_router.include_router(memories_router)
    v1_router.include_router(operations_router)
    root_router.include_router(v1_router)
    return root_router
