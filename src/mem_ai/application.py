"""FastAPI application factory, lifespan resource management, and provider assembly."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import fastapi_offline
from fastapi import FastAPI
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.api.middleware import register_middleware
from mem_ai.api.router import build_api_router
from mem_ai.core.config import Settings
from mem_ai.core.exception_handlers import register_exception_handlers
from mem_ai.core.limits import ConcurrencyLimiter, NoopRateLimiter, RateLimiter
from mem_ai.core.logging import configure_logging
from mem_ai.core.tasks import drain_tasks_bounded
from mem_ai.core.telemetry import (
    NoopTracer,
    TelemetryRegistry,
    Tracer,
    bind_telemetry,
    reset_telemetry,
)
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.integrations.http import create_http_client
from mem_ai.modules.auth.adapters import (
    CredentialVerifier,
    DevelopmentAuthAdapter,
    StaticTokenAuthAdapter,
    build_credential_verifier,
)
from mem_ai.modules.conversations.execution import SyncGenerationTaskRegistry
from mem_ai.modules.conversations.queries import (
    DatabaseConversationAccessChecker,
    DatabaseRecalledMemoryReferenceMarker,
)
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations.audit import build_deletion_audit_archiver
from mem_ai.workers.assembly import build_worker_registry_from_settings, default_periodic_jobs
from mem_ai.workers.runner import WorkerRunner, worker_config_from_settings

_OFFLINE_DOCS_STATIC_DIR = Path(fastapi_offline.__file__).resolve().parent / "static"
_OFFLINE_DOCS_STATIC_PREFIX = "/static/docs"
logger = logging.getLogger("mem_ai.application")


def _register_offline_docs(app: FastAPI) -> None:
    """Serve Swagger UI and ReDoc using bundled local static assets (no external CDN)."""
    openapi_url = app.openapi_url
    if openapi_url is None:
        return

    app.mount(
        _OFFLINE_DOCS_STATIC_PREFIX,
        StaticFiles(directory=_OFFLINE_DOCS_STATIC_DIR),
        name="docs-static",
    )

    @app.get("/docs", include_in_schema=False)
    async def swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{app.title} - Swagger UI",
            swagger_js_url=f"{_OFFLINE_DOCS_STATIC_PREFIX}/swagger-ui-bundle.js",
            swagger_css_url=f"{_OFFLINE_DOCS_STATIC_PREFIX}/swagger-ui.css",
            swagger_favicon_url=f"{_OFFLINE_DOCS_STATIC_PREFIX}/favicon.png",
        )

    @app.get("/redoc", include_in_schema=False)
    async def redoc_html() -> HTMLResponse:
        return get_redoc_html(
            openapi_url=openapi_url,
            title=f"{app.title} - ReDoc",
            redoc_js_url=f"{_OFFLINE_DOCS_STATIC_PREFIX}/redoc.standalone.js",
            redoc_favicon_url=f"{_OFFLINE_DOCS_STATIC_PREFIX}/favicon.png",
            with_google_fonts=False,
        )


def create_app(
    settings: Settings | None = None,
    *,
    providers: ProviderRegistry | None = None,
    credential_verifier: CredentialVerifier | None = None,
    rate_limiter: RateLimiter | None = None,
    telemetry: TelemetryRegistry | None = None,
    tracer: Tracer | None = None,
) -> FastAPI:
    """Construct and configure the Mem-AI FastAPI application."""
    resolved_settings = settings if settings is not None else Settings()
    resolved_verifier = (
        credential_verifier
        if credential_verifier is not None
        else build_credential_verifier(resolved_settings)
    )
    if resolved_settings.app_env == "production" and isinstance(
        resolved_verifier, (DevelopmentAuthAdapter, StaticTokenAuthAdapter)
    ):
        raise ValueError("development or test authentication adapter is forbidden in production")

    resolved_telemetry = telemetry if telemetry is not None else TelemetryRegistry()
    resolved_tracer: Tracer = tracer if tracer is not None else NoopTracer()
    resolved_rate_limiter: RateLimiter = (
        rate_limiter if rate_limiter is not None else NoopRateLimiter()
    )

    configure_logging(resolved_settings.app_log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        tel_tokens = bind_telemetry(telemetry=resolved_telemetry, tracer=resolved_tracer)
        app.state.ready = False
        db_engine = create_async_engine_from_settings(resolved_settings)
        session_factory = create_session_factory(db_engine)
        http_client = create_http_client(resolved_settings)
        sync_task_registry = SyncGenerationTaskRegistry()

        app.state.db_engine = db_engine
        app.state.session_factory = session_factory
        app.state.http_client = http_client
        app.state.sync_generation_tasks = sync_task_registry
        app.state.conversation_access_checker = DatabaseConversationAccessChecker(
            session_factory=session_factory
        )
        app.state.recalled_memory_marker = DatabaseRecalledMemoryReferenceMarker()
        app.state.deletion_audit_archiver = build_deletion_audit_archiver(
            resolved_settings.deletion_audit_archive_path
        )

        model_seeds = [
            ModelConfigSeed.model_validate(entry)
            for entry in resolved_settings.model_registry_entries
        ]
        if resolved_settings.model_sync_on_startup and model_seeds:
            await models_service.sync_model_configs(session_factory, seeds=model_seeds)

        base_providers = providers if providers is not None else ProviderRegistry()
        resolved_model_provider = base_providers.model_provider
        if (
            resolved_model_provider is None
            and resolved_settings.model_provider_mode == "openai_compatible"
            and resolved_settings.openai_compat_api_key is not None
        ):
            upstream_model_map = {
                seed.model_id: seed.resolved_upstream_model() for seed in model_seeds
            }
            resolved_model_provider = OpenAICompatibleModelProvider(
                http_client=http_client,
                base_url=resolved_settings.openai_compat_base_url,
                api_key=resolved_settings.openai_compat_api_key,
                timeout_seconds=resolved_settings.openai_compat_request_timeout_seconds,
                upstream_model_map=upstream_model_map,
            )

        resolved_memory_provider = base_providers.memory_provider
        if (
            resolved_memory_provider is None
            and resolved_settings.memory_provider_mode == "memos"
            and resolved_settings.memos_api_key is not None
        ):
            resolved_memory_provider = MemOSMemoryProvider(
                http_client=http_client,
                session_factory=session_factory,
                api_key=resolved_settings.memos_api_key,
                base_url=resolved_settings.memos_base_url,
                timeout_seconds=resolved_settings.memos_request_timeout_seconds,
                list_page_size=resolved_settings.memos_list_page_size,
                list_max_items=resolved_settings.memos_list_max_items,
                snapshot_ttl_seconds=resolved_settings.memory_list_snapshot_ttl_seconds,
            )

        app.state.providers = ProviderRegistry(
            model_provider=resolved_model_provider,
            memory_provider=resolved_memory_provider,
        )

        worker_runner = WorkerRunner(
            session_factory=session_factory,
            registry=build_worker_registry_from_settings(
                session_factory,
                resolved_settings,
                memory_provider=resolved_memory_provider,
                audit_archiver=app.state.deletion_audit_archiver,
            ),
            config=worker_config_from_settings(resolved_settings),
            periodic_jobs=default_periodic_jobs(resolved_settings),
            telemetry=resolved_telemetry,
            tracer=resolved_tracer,
        )
        worker_task: asyncio.Task[None] | None = None
        if resolved_settings.worker_loop_enabled:
            worker_task = asyncio.create_task(
                worker_runner.run_forever(),
                name="mem-ai-worker-runner",
            )
        app.state.worker_runner = worker_runner
        app.state.worker_task = worker_task
        app.state.ready = True

        try:
            yield
        finally:
            app.state.ready = False
            worker_runner.request_shutdown()

            async def _shutdown_worker() -> None:
                worker_error: BaseException | None = None
                try:
                    if worker_task is not None:
                        if not worker_task.done():
                            # `run_forever` drains within `worker_shutdown_grace_seconds`;
                            # keep a slight outer slack so the runner owns its own drain window.
                            outer_timeout = resolved_settings.worker_shutdown_grace_seconds + 0.5
                            remaining = await drain_tasks_bounded(
                                (worker_task,),
                                timeout_seconds=outer_timeout,
                            )
                            if remaining:
                                logger.warning(
                                    "Embedded worker task remained active after shutdown grace",
                                    extra={
                                        "extra_fields": {
                                            "worker_id": worker_runner.config.worker_id
                                        }
                                    },
                                )
                        if worker_task.done() and not worker_task.cancelled():
                            worker_error = worker_task.exception()
                finally:
                    await worker_runner.drain()

                if worker_error is not None:
                    raise worker_error

            shutdown_results = await asyncio.gather(
                sync_task_registry.drain(
                    timeout_seconds=min(resolved_settings.chat_generation_timeout_seconds, 10.0)
                ),
                _shutdown_worker(),
                return_exceptions=True,
            )
            try:
                for result in shutdown_results:
                    if isinstance(result, BaseException):
                        raise result
            finally:
                await http_client.aclose()
                await db_engine.dispose()
                reset_telemetry(tel_tokens)

    docs_enabled = resolved_settings.app_env != "production"
    app = FastAPI(
        title=resolved_settings.app_name,
        version=resolved_settings.app_version,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )
    app.state.settings = resolved_settings
    app.state.providers = providers if providers is not None else ProviderRegistry()
    app.state.credential_verifier = resolved_verifier
    app.state.telemetry = resolved_telemetry
    app.state.tracer = resolved_tracer
    app.state.rate_limiter = resolved_rate_limiter
    app.state.request_concurrency_limiter = ConcurrencyLimiter(
        max_in_flight=resolved_settings.max_concurrent_requests,
        name="http_requests",
    )
    app.state.generation_concurrency_limiter = ConcurrencyLimiter(
        max_in_flight=resolved_settings.max_concurrent_generations,
        name="generations",
    )
    app.state.ready = False

    register_middleware(app, resolved_settings)
    register_exception_handlers(app)
    app.include_router(build_api_router())
    if docs_enabled:
        _register_offline_docs(app)
    return app
