"""FastAPI application factory, lifespan resource management, and provider assembly."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.api.middleware import register_middleware
from mem_ai.api.router import build_api_router
from mem_ai.core.config import Settings
from mem_ai.core.exception_handlers import register_exception_handlers
from mem_ai.core.logging import configure_logging
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.integrations.http import create_http_client


def create_app(
    settings: Settings | None = None,
    *,
    providers: ProviderRegistry | None = None,
) -> FastAPI:
    """Construct and configure the Mem-AI FastAPI application."""
    resolved_settings = settings if settings is not None else Settings()
    configure_logging(resolved_settings.app_log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db_engine = create_async_engine_from_settings(resolved_settings)
        session_factory = create_session_factory(db_engine)
        http_client = create_http_client(resolved_settings)

        app.state.db_engine = db_engine
        app.state.session_factory = session_factory
        app.state.http_client = http_client

        try:
            yield
        finally:
            await http_client.aclose()
            await db_engine.dispose()

    app = FastAPI(
        title=resolved_settings.app_name,
        version=resolved_settings.app_version,
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.providers = providers if providers is not None else ProviderRegistry()

    register_middleware(app, resolved_settings)
    register_exception_handlers(app)
    app.include_router(build_api_router())
    return app
