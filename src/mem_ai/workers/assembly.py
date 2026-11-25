"""Application job-handler assembly and window-deduplicated periodic jobs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.core.config import Settings
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
from mem_ai.modules.operations.audit import DeletionAuditArchiver
from mem_ai.modules.retention.schemas import RetentionPolicy, retention_policy_from_settings
from mem_ai.modules.retention.service import (
    JOB_TYPE_RETENTION_CLEANUP,
    RetentionCleanupJobHandler,
)
from mem_ai.modules.users.cleanup import JOB_TYPE_USER_DELETE, UserCleanupJobHandler
from mem_ai.workers.runner import JobHandlerRegistry, PeriodicJobSpec

JOB_TYPE_GENERATION_LEASE_RECOVERY = "generation_lease_recovery"


class GenerationLeaseRecoveryJobHandler:
    """Fallback handler converging expired `running` generations via shared entry."""

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
        """Converge expired generation leases through the generation phase entry."""
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
    """Assemble typed handlers while keeping each resource rule in its owner module."""
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


def build_worker_registry_from_settings(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    memory_provider: MemoryProvider | None = None,
    audit_archiver: DeletionAuditArchiver | None = None,
) -> JobHandlerRegistry:
    """Build the application's handler registry from shared runtime settings."""
    return build_worker_registry(
        session_factory,
        memory_provider=memory_provider,
        idempotency_retention_seconds=float(settings.idempotency_retention_hours * 3600),
        memory_inflight_settle_timeout_seconds=settings.memory_inflight_settle_timeout,
        memory_clear_compensation_delay_seconds=(settings.memory_clear_compensation_delay_seconds),
        audit_archiver=audit_archiver,
        retention_policy=retention_policy_from_settings(settings),
    )


def default_periodic_jobs(settings: Settings) -> tuple[PeriodicJobSpec, ...]:
    """Return periodic generation recovery and retention windows for the runner."""
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
