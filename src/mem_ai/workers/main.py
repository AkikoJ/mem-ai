"""Standalone Worker process entry point, resource lifecycle, and handler assembly."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Callable, Generator, Sequence
from datetime import datetime

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.api.middleware import RequestContextMiddleware
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.config import Settings
from mem_ai.core.exception_handlers import register_exception_handlers
from mem_ai.core.logging import configure_logging
from mem_ai.core.telemetry import TelemetryRegistry, Tracer
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.integrations.http import create_http_client
from mem_ai.modules.conversations import generation as conversations_generation
from mem_ai.modules.conversations.cleanup import (
    JOB_TYPE_CONVERSATION_DELETE,
    ConversationCleanupJobHandler,
)
from mem_ai.modules.conversations.queries import (
    DatabaseRecalledMemoryReferenceMarker,
    DatabaseTurnContentLoader,
)
from mem_ai.modules.conversations.schemas import DEFAULT_IDEMPOTENCY_RETENTION_SECONDS
from mem_ai.modules.health.service import (
    ReadinessStatus,
    build_metrics_response,
    check_readiness,
)
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.memories.jobs import (
    JOB_TYPE_MEMORY_CLEAR,
    JOB_TYPE_MEMORY_DELETE,
    JOB_TYPE_MEMORY_EXPLICIT_CREATE,
    JOB_TYPE_MEMORY_TURN_WRITE,
    ExplicitMemoryReconcileJobHandler,
    MemoryClearJobHandler,
    MemoryDeleteJobHandler,
    TurnMemoryWriteJobHandler,
)
from mem_ai.modules.operations.audit import (
    DeletionAuditArchiver,
    build_deletion_audit_archiver,
)
from mem_ai.modules.retention.schemas import RetentionPolicy, retention_policy_from_settings
from mem_ai.modules.retention.service import (
    JOB_TYPE_RETENTION_CLEANUP,
    RetentionCleanupJobHandler,
)
from mem_ai.modules.users.cleanup import JOB_TYPE_USER_DELETE, UserCleanupJobHandler
from mem_ai.workers.runner import (
    JobHandlerRegistry,
    PeriodicJobSpec,
    WorkerConfig,
    WorkerHealthSnapshot,
    WorkerRunner,
    worker_config_from_settings,
)

logger = logging.getLogger("mem_ai.workers.main")

JOB_TYPE_GENERATION_LEASE_RECOVERY = "generation_lease_recovery"


class GenerationLeaseRecoveryJobHandler:
    """Fallback Worker handler converging expired `running` generations via shared entry."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: int = 50,
        idempotency_retention_seconds: float = DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._idempotency_retention_seconds = idempotency_retention_seconds

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Converge expired generation leases using `recover_expired_generations_phase`."""
        _ = ctx
        recovered = await conversations_generation.recover_expired_generations_phase(
            self._session_factory,
            batch_size=self._batch_size,
            idempotency_retention_seconds=self._idempotency_retention_seconds,
        )
        return JobHandlerOutcome(
            outcome="succeeded",
            result_summary={"recovered_count": len(recovered)},
        )


def build_worker_registry(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    memory_provider: MemoryProvider | None = None,
    idempotency_retention_seconds: float = DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
    memory_inflight_settle_timeout_seconds: float = 60.0,
    memory_clear_compensation_delay_seconds: float = 30.0,
    audit_archiver: DeletionAuditArchiver | None = None,
    clock: Callable[[], datetime] | None = None,
    conversation_cleanup_batch_size: int = 50,
    retention_policy: RetentionPolicy | None = None,
) -> JobHandlerRegistry:
    """Assemble typed Worker job handlers with unidirectional module dependencies."""
    registry = JobHandlerRegistry()
    registry.register(
        JOB_TYPE_USER_DELETE,
        UserCleanupJobHandler(session_factory=session_factory, audit_archiver=audit_archiver),
    )
    registry.register(
        JOB_TYPE_CONVERSATION_DELETE,
        ConversationCleanupJobHandler(
            session_factory=session_factory,
            memory_provider=memory_provider,
            batch_size=conversation_cleanup_batch_size,
            audit_archiver=audit_archiver,
        ),
    )
    registry.register(
        JOB_TYPE_GENERATION_LEASE_RECOVERY,
        GenerationLeaseRecoveryJobHandler(
            session_factory=session_factory,
            idempotency_retention_seconds=idempotency_retention_seconds,
        ),
    )
    registry.register(
        JOB_TYPE_RETENTION_CLEANUP,
        RetentionCleanupJobHandler(
            session_factory=session_factory,
            policy=retention_policy,
            clock=clock,
            prunable_periodic_job_types=(
                JOB_TYPE_GENERATION_LEASE_RECOVERY,
                JOB_TYPE_RETENTION_CLEANUP,
            ),
        ),
    )

    if memory_provider is not None:
        turn_loader = DatabaseTurnContentLoader(session_factory=session_factory)
        recalled_marker = DatabaseRecalledMemoryReferenceMarker()
        registry.register(
            JOB_TYPE_MEMORY_TURN_WRITE,
            TurnMemoryWriteJobHandler(
                session_factory=session_factory,
                memory_provider=memory_provider,
                turn_loader=turn_loader,
            ),
        )
        registry.register(
            JOB_TYPE_MEMORY_EXPLICIT_CREATE,
            ExplicitMemoryReconcileJobHandler(
                session_factory=session_factory,
                memory_provider=memory_provider,
                turn_loader=turn_loader,
            ),
        )
        registry.register(
            JOB_TYPE_MEMORY_DELETE,
            MemoryDeleteJobHandler(
                session_factory=session_factory,
                memory_provider=memory_provider,
                audit_archiver=audit_archiver,
            ),
        )
        registry.register(
            JOB_TYPE_MEMORY_CLEAR,
            MemoryClearJobHandler(
                session_factory=session_factory,
                memory_provider=memory_provider,
                inflight_settle_timeout_seconds=memory_inflight_settle_timeout_seconds,
                compensation_delay_seconds=memory_clear_compensation_delay_seconds,
                recalled_memory_marker=recalled_marker,
                audit_archiver=audit_archiver,
                clock=clock,
            ),
        )

    return registry


def default_periodic_jobs(settings: Settings) -> tuple[PeriodicJobSpec, ...]:
    """Return window-deduplicated periodic jobs managed by the standalone Worker."""
    recovery_interval = max(10.0, float(settings.chat_lease_ttl_seconds) / 2.0)
    return (
        PeriodicJobSpec(
            job_type=JOB_TYPE_GENERATION_LEASE_RECOVERY,
            interval_seconds=recovery_interval,
            max_attempts=3,
        ),
        PeriodicJobSpec(
            job_type=JOB_TYPE_RETENTION_CLEANUP,
            interval_seconds=float(settings.retention_cleanup_interval_seconds),
            max_attempts=3,
        ),
    )


class _WorkerProbeServer(uvicorn.Server):
    """Uvicorn server whose shutdown is coordinated by `run_worker` rather than OS signals."""

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None]:
        yield


def build_worker_probe_app(
    *,
    runner: WorkerRunner,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> FastAPI:
    """Build standalone operational probe & controlled `/metrics` ASGI app for the Worker."""
    probe_app = FastAPI(
        title=f"{settings.app_name}-worker",
        version=settings.app_version,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    probe_app.state.settings = settings
    probe_app.state.telemetry = runner.telemetry
    probe_app.add_middleware(
        RequestContextMiddleware,
        request_id_header=settings.trusted_request_id_header,
        trust_from_proxies_only=settings.effective_trust_request_id_from_proxies_only(),
        trusted_proxies=settings.trusted_proxy_networks(),
    )
    register_exception_handlers(probe_app)

    async def worker_live() -> ApiResponse[WorkerHealthSnapshot]:
        return ok_response(runner.health_snapshot())

    async def worker_ready() -> ApiResponse[ReadinessStatus]:
        snap = runner.health_snapshot()
        return ok_response(
            await check_readiness(session_factory, settings, app_ready=not snap.shutting_down)
        )

    async def worker_metrics(request: Request) -> PlainTextResponse:
        return await build_metrics_response(
            request,
            session_factory=session_factory,
            settings=settings,
            telemetry=runner.telemetry,
        )

    probe_app.add_api_route(
        "/health/live",
        worker_live,
        methods=["GET"],
        response_model=ApiResponse[WorkerHealthSnapshot],
    )
    probe_app.add_api_route(
        "/health/ready",
        worker_ready,
        methods=["GET"],
        response_model=ApiResponse[ReadinessStatus],
    )
    probe_app.add_api_route("/metrics", worker_metrics, methods=["GET"])
    return probe_app


async def run_worker(
    settings: Settings | None = None,
    *,
    memory_provider: MemoryProvider | None = None,
    stop_event: asyncio.Event | None = None,
    config_override: WorkerConfig | None = None,
    periodic_jobs: Sequence[PeriodicJobSpec] | None = None,
    telemetry: TelemetryRegistry | None = None,
    tracer: Tracer | None = None,
) -> None:
    """Initialize standalone Worker resources, run until shutdown signal, and clean up."""
    resolved_settings = settings if settings is not None else Settings()
    configure_logging(resolved_settings.app_log_level)

    db_engine = create_async_engine_from_settings(resolved_settings)
    session_factory = create_session_factory(db_engine)
    http_client = create_http_client(resolved_settings)

    resolved_memory_provider = memory_provider
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

    audit_archiver = build_deletion_audit_archiver(resolved_settings.deletion_audit_archive_path)
    registry = build_worker_registry(
        session_factory,
        memory_provider=resolved_memory_provider,
        idempotency_retention_seconds=float(resolved_settings.idempotency_retention_hours * 3600),
        memory_inflight_settle_timeout_seconds=resolved_settings.memory_inflight_settle_timeout,
        memory_clear_compensation_delay_seconds=(
            resolved_settings.memory_clear_compensation_delay_seconds
        ),
        audit_archiver=audit_archiver,
        retention_policy=retention_policy_from_settings(resolved_settings),
    )
    worker_config = (
        config_override
        if config_override is not None
        else worker_config_from_settings(resolved_settings)
    )
    resolved_periodic = (
        tuple(periodic_jobs)
        if periodic_jobs is not None
        else default_periodic_jobs(resolved_settings)
    )
    runner = WorkerRunner(
        session_factory=session_factory,
        registry=registry,
        config=worker_config,
        periodic_jobs=resolved_periodic,
        telemetry=telemetry,
        tracer=tracer,
    )

    probe_server: uvicorn.Server | None = None
    probe_task: asyncio.Task[None] | None = None
    if resolved_settings.worker_metrics_port is not None:
        probe_app = build_worker_probe_app(
            runner=runner,
            session_factory=session_factory,
            settings=resolved_settings,
        )
        uv_config = uvicorn.Config(
            probe_app,
            host=resolved_settings.worker_metrics_host,
            port=resolved_settings.worker_metrics_port,
            log_config=None,
            access_log=False,
        )
        probe_server = _WorkerProbeServer(uv_config)
        probe_task = asyncio.create_task(probe_server.serve())

    effective_stop = stop_event if stop_event is not None else asyncio.Event()
    installed_signals: list[signal.Signals] = []
    if stop_event is None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, effective_stop.set)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError):
                pass

    try:
        await runner.run_forever(stop_event=effective_stop)
    finally:
        if probe_server is not None:
            probe_server.should_exit = True
        if probe_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(probe_task, timeout=5.0)
        if installed_signals:
            loop = asyncio.get_running_loop()
            for sig in installed_signals:
                loop.remove_signal_handler(sig)
        await http_client.aclose()
        await db_engine.dispose()


def main() -> None:
    """CLI entry point for `python -m mem_ai.workers.main`."""
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
