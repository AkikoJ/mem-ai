"""Bounded, cursor-resumable data retention sweeps and periodic Worker job handler."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ConflictError
from mem_ai.db.base import get_database_now, normalize_utc
from mem_ai.modules.auth import service as auth_service
from mem_ai.modules.conversations import idempotency as conversations_idempotency
from mem_ai.modules.jobs import cleanup as jobs_cleanup
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome
from mem_ai.modules.memories import barrier as memories_barrier
from mem_ai.modules.memories import snapshots as memories_snapshots
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.retention.schemas import (
    RETENTION_STAGES,
    STAGE_COUNT_KEYS,
    RetentionBatchResult,
    RetentionCursor,
    RetentionJobMetadata,
    RetentionPolicy,
    RetentionStage,
    RetentionSweepSummary,
)

logger = logging.getLogger("mem_ai.retention")

JOB_TYPE_RETENTION_CLEANUP = "retention_cleanup"
_DEFAULT_PRUNABLE_PERIODIC_JOB_TYPES: tuple[str, ...] = (JOB_TYPE_RETENTION_CLEANUP,)
_CONTINUATION_RETRY_DELAY_SECONDS = 1.0


class _RetentionBatchExecutionError(Exception):
    """Carries the last durable checkpoint state when a mid-sweep batch fails."""

    def __init__(
        self,
        *,
        stage: RetentionStage,
        state: RetentionJobMetadata,
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.stage = stage
        self.state = state
        self.cause = cause


def _to_cursor(raw: tuple[datetime, UUID] | None) -> RetentionCursor | None:
    if raw is None:
        return None
    return RetentionCursor(timestamp=raw[0], id=raw[1])


async def run_retention_batch_in_session(
    session: AsyncSession,
    *,
    stage: RetentionStage,
    policy: RetentionPolicy,
    current_time: datetime,
    cursor: RetentionCursor | None = None,
    exclude_job_id: UUID | None = None,
    prunable_periodic_job_types: Sequence[str] = _DEFAULT_PRUNABLE_PERIODIC_JOB_TYPES,
) -> RetentionBatchResult:
    """Execute one bounded retention batch for `stage` via its owning module's public entry."""
    utc_now = normalize_utc(current_time)
    raw_cursor = cursor.as_tuple() if cursor is not None else None

    if stage == "idempotency_records":
        idem_batch = await conversations_idempotency.purge_expired_idempotency_batch_in_session(
            session,
            current_time=utc_now,
            batch_size=policy.batch_size,
            cursor=raw_cursor,
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=idem_batch.deleted_count,
            protected_skipped=0,
            scanned_count=idem_batch.scanned_count,
            next_cursor=_to_cursor(idem_batch.next_cursor),
            has_more=idem_batch.has_more,
        )

    if stage == "job_attempts":

        async def _protect_job_attempts(
            active_session: AsyncSession,
            job_ids: Sequence[UUID],
            operation_ids: Sequence[UUID],
        ) -> tuple[frozenset[UUID], frozenset[UUID]]:
            return await operations_service.find_non_terminal_operation_links_in_session(
                active_session,
                job_ids=job_ids,
                operation_ids=operation_ids,
            )

        attempt_batch = await jobs_cleanup.purge_expired_job_attempts_batch_in_session(
            session,
            current_time=utc_now,
            batch_size=policy.batch_size,
            cursor=raw_cursor,
            protected_job_filter=_protect_job_attempts,
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=attempt_batch.deleted_count,
            protected_skipped=attempt_batch.protected_skipped,
            scanned_count=attempt_batch.scanned_count,
            next_cursor=_to_cursor(attempt_batch.next_cursor),
            has_more=attempt_batch.has_more,
        )

    if stage == "memory_list_snapshots":
        snap_batch = await memories_snapshots.purge_expired_list_snapshots_batch_in_session(
            session,
            current_time=utc_now,
            batch_size=policy.batch_size,
            cursor=raw_cursor,
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=snap_batch.deleted_count,
            protected_skipped=0,
            scanned_count=snap_batch.scanned_count,
            next_cursor=_to_cursor(snap_batch.next_cursor),
            has_more=snap_batch.has_more,
        )

    if stage == "user_delete_operations":
        receipt_batch = (
            await operations_service.purge_expired_user_delete_operations_batch_in_session(
                session,
                current_time=utc_now,
                batch_size=policy.batch_size,
                cursor=raw_cursor,
            )
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=receipt_batch.deleted_count,
            protected_skipped=receipt_batch.protected_skipped,
            scanned_count=receipt_batch.scanned_count,
            next_cursor=_to_cursor(receipt_batch.next_cursor),
            has_more=receipt_batch.has_more,
        )

    if stage == "operation_summaries":

        async def _protect_operations(
            active_session: AsyncSession,
            operation_ids: Sequence[UUID],
        ) -> frozenset[UUID]:
            return await memories_barrier.find_protected_operation_ids_in_session(
                active_session,
                operation_ids=operation_ids,
            )

        async def _protect_jobs(
            active_session: AsyncSession,
            job_ids: Sequence[UUID],
        ) -> frozenset[UUID]:
            return await jobs_cleanup.find_active_or_unsettled_job_ids_in_session(
                active_session,
                job_ids=job_ids,
                current_time=utc_now,
            )

        summary_batch = await operations_service.purge_expired_operation_summaries_batch_in_session(
            session,
            current_time=utc_now,
            retention_days=policy.operation_summary_retention_days,
            batch_size=policy.batch_size,
            cursor=raw_cursor,
            protected_operation_filter=_protect_operations,
            protected_job_filter=_protect_jobs,
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=summary_batch.deleted_count,
            protected_skipped=summary_batch.protected_skipped,
            scanned_count=summary_batch.scanned_count,
            next_cursor=_to_cursor(summary_batch.next_cursor),
            has_more=summary_batch.has_more,
        )

    if stage == "auth_sessions":
        auth_batch = await auth_service.purge_expired_auth_sessions_batch_in_session(
            session,
            current_time=utc_now,
            retention_days=policy.auth_sessions_retention_days,
            batch_size=policy.batch_size,
            cursor=raw_cursor,
        )
        return RetentionBatchResult(
            stage=stage,
            deleted_count=auth_batch.deleted_count,
            protected_skipped=0,
            scanned_count=auth_batch.scanned_count,
            next_cursor=_to_cursor(auth_batch.next_cursor),
            has_more=auth_batch.has_more,
        )

    periodic_batch = await jobs_cleanup.purge_expired_periodic_jobs_batch_in_session(
        session,
        current_time=utc_now,
        retention_days=policy.job_attempt_retention_days,
        allowed_job_types=prunable_periodic_job_types,
        exclude_job_id=exclude_job_id,
        batch_size=policy.batch_size,
        cursor=raw_cursor,
    )
    return RetentionBatchResult(
        stage=stage,
        deleted_count=periodic_batch.deleted_count,
        protected_skipped=0,
        scanned_count=periodic_batch.scanned_count,
        next_cursor=_to_cursor(periodic_batch.next_cursor),
        has_more=periodic_batch.has_more,
    )


def _advance_state(
    state: RetentionJobMetadata,
    batch: RetentionBatchResult,
) -> RetentionJobMetadata:
    totals = dict(state.totals)
    count_key = STAGE_COUNT_KEYS[batch.stage]
    totals[count_key] = totals.get(count_key, 0) + batch.deleted_count
    totals["protected_skipped"] = totals.get("protected_skipped", 0) + batch.protected_skipped

    if batch.has_more and batch.next_cursor is not None:
        next_stage_index = state.stage_index
        next_cursor: RetentionCursor | None = batch.next_cursor
    else:
        next_stage_index = min(len(RETENTION_STAGES), state.stage_index + 1)
        next_cursor = None

    return RetentionJobMetadata(
        stage_index=next_stage_index,
        cursor=next_cursor,
        batches_completed=state.batches_completed + 1,
        totals=totals,
    )


def _build_summary(
    state: RetentionJobMetadata,
    *,
    duration_ms: float,
    failure_summary: str | None = None,
) -> RetentionSweepSummary:
    return RetentionSweepSummary(
        idempotency_records_deleted=state.totals["idempotency_records_deleted"],
        job_attempts_deleted=state.totals["job_attempts_deleted"],
        memory_list_snapshots_deleted=state.totals["memory_list_snapshots_deleted"],
        user_delete_operations_deleted=state.totals["user_delete_operations_deleted"],
        operation_summaries_deleted=state.totals["operation_summaries_deleted"],
        periodic_jobs_deleted=state.totals["periodic_jobs_deleted"],
        auth_sessions_deleted=state.totals["auth_sessions_deleted"],
        protected_skipped=state.totals["protected_skipped"],
        batches_executed=state.batches_completed,
        duration_ms=duration_ms,
        completed=state.stage_index >= len(RETENTION_STAGES) and failure_summary is None,
        failure_summary=failure_summary,
    )


async def _execute_bounded_sweep(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    policy: RetentionPolicy,
    clock: Callable[[], datetime] | None,
    initial_state: RetentionJobMetadata,
    job_id: UUID | None,
    lease_token: str | None,
    prunable_periodic_job_types: Sequence[str],
    wrap_errors: bool,
) -> tuple[RetentionSweepSummary, RetentionJobMetadata]:
    """Run up to `policy.max_batches_per_run` short-transaction batches, checkpointing each."""
    state = initial_state
    started = time.perf_counter()
    batches_this_run = 0

    while (
        state.stage_index < len(RETENTION_STAGES) and batches_this_run < policy.max_batches_per_run
    ):
        stage = RETENTION_STAGES[state.stage_index]
        try:
            async with session_factory.begin() as session:
                current_time = (
                    normalize_utc(clock()) if clock is not None else await get_database_now(session)
                )
                batch = await run_retention_batch_in_session(
                    session,
                    stage=stage,
                    policy=policy,
                    current_time=current_time,
                    cursor=state.cursor,
                    exclude_job_id=job_id,
                    prunable_periodic_job_types=prunable_periodic_job_types,
                )
                next_state = _advance_state(state, batch)
                if job_id is not None and lease_token is not None:
                    await jobs_cleanup.checkpoint_job_metadata_in_session(
                        session,
                        job_id=job_id,
                        lease_token=lease_token,
                        metadata_patch=next_state.to_metadata_patch(),
                        now=current_time,
                    )
        except Exception as exc:
            if not wrap_errors or (isinstance(exc, ConflictError) and exc.code == "job_lease_lost"):
                raise
            raise _RetentionBatchExecutionError(stage=stage, state=state, cause=exc) from exc

        state = next_state
        batches_this_run += 1

    duration_ms = (time.perf_counter() - started) * 1000.0
    return _build_summary(state, duration_ms=duration_ms), state


async def run_retention_sweep(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    policy: RetentionPolicy | None = None,
    clock: Callable[[], datetime] | None = None,
    initial_state: RetentionJobMetadata | None = None,
    job_id: UUID | None = None,
    lease_token: str | None = None,
    prunable_periodic_job_types: Sequence[str] = _DEFAULT_PRUNABLE_PERIODIC_JOB_TYPES,
) -> tuple[RetentionSweepSummary, RetentionJobMetadata]:
    """Run bounded short-transaction retention batches, checkpointing cursors after each batch."""
    return await _execute_bounded_sweep(
        session_factory,
        policy=policy if policy is not None else RetentionPolicy(),
        clock=clock,
        initial_state=initial_state if initial_state is not None else RetentionJobMetadata(),
        job_id=job_id,
        lease_token=lease_token,
        prunable_periodic_job_types=prunable_periodic_job_types,
        wrap_errors=False,
    )


class RetentionCleanupJobHandler:
    """Periodic Worker handler executing bounded, resumable, idempotent expiry cleanup."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        policy: RetentionPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
        prunable_periodic_job_types: Sequence[str] = _DEFAULT_PRUNABLE_PERIODIC_JOB_TYPES,
    ) -> None:
        self._session_factory = session_factory
        self._policy = policy if policy is not None else RetentionPolicy()
        self._clock = clock
        self._prunable_periodic_job_types = tuple(prunable_periodic_job_types)

    @property
    def policy(self) -> RetentionPolicy:
        """Return immutable retention policy used by this handler."""
        return self._policy

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Resume from durable cursor metadata, run bounded batches, and log sanitized metrics."""
        try:
            initial_state = RetentionJobMetadata.model_validate(ctx.job.metadata_json)
        except ValidationError:
            return JobHandlerOutcome(
                outcome="failed",
                error_code="invalid_retention_metadata",
                error_summary="retention cleanup job metadata failed validation",
                result_summary={"failure_summary": "invalid_retention_metadata"},
            )

        started = time.perf_counter()
        try:
            summary, final_state = await _execute_bounded_sweep(
                self._session_factory,
                policy=self._policy,
                clock=self._clock,
                initial_state=initial_state,
                job_id=ctx.job.id,
                lease_token=ctx.lease_token,
                prunable_periodic_job_types=self._prunable_periodic_job_types,
                wrap_errors=True,
            )
        except _RetentionBatchExecutionError as err:
            duration_ms = (time.perf_counter() - started) * 1000.0
            if isinstance(err.cause, ConflictError):
                error_code = err.cause.code
                failure_summary = f"stage={err.stage} code={err.cause.code}"
            else:
                error_code = "retention_batch_failed"
                failure_summary = f"stage={err.stage} error={type(err.cause).__name__}"
            summary = _build_summary(
                err.state,
                duration_ms=duration_ms,
                failure_summary=failure_summary,
            )
            logger.warning(
                "Retention cleanup batch failed; checkpoint preserved for retry",
                extra={"extra_fields": summary.to_result_summary()},
            )
            return JobHandlerOutcome(
                outcome="retryable",
                error_code=error_code,
                error_summary=failure_summary,
                metadata_patch=err.state.to_metadata_patch(),
                result_summary=summary.to_result_summary(),
            )

        if not summary.completed:
            logger.info(
                "Retention cleanup batch budget reached; continuing from cursor checkpoint",
                extra={"extra_fields": summary.to_result_summary()},
            )
            return JobHandlerOutcome(
                outcome="reconciling",
                retry_after_seconds=_CONTINUATION_RETRY_DELAY_SECONDS,
                metadata_patch=final_state.to_metadata_patch(),
                result_summary=summary.to_result_summary(),
            )

        logger.info(
            "Completed bounded retention cleanup sweep",
            extra={"extra_fields": summary.to_result_summary()},
        )
        return JobHandlerOutcome(
            outcome="succeeded",
            metadata_patch=final_state.to_metadata_patch(),
            result_summary=summary.to_result_summary(),
        )
