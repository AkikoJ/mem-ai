"""Standalone Worker process entry point, resource lifecycle, and handler assembly."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Sequence
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.config import Settings
from mem_ai.core.logging import configure_logging
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


async def run_worker(
    settings: Settings | None = None,
    *,
    memory_provider: MemoryProvider | None = None,
    stop_event: asyncio.Event | None = None,
    config_override: WorkerConfig | None = None,
    periodic_jobs: Sequence[PeriodicJobSpec] | None = None,
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
    )

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
