"""Shared Worker runner: handler registry, SKIP LOCKED polling, leases, and shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import AppError, ConflictError
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.core.tasks import drain_tasks_bounded
from mem_ai.core.telemetry import (
    NoopTracer,
    Span,
    TelemetryRegistry,
    Tracer,
    bind_telemetry,
    reset_telemetry,
)
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    ClaimedJob,
    JobExecutionContext,
    JobHandlerOutcome,
)
from mem_ai.workers.lease import JobLeaseHeartbeat
from mem_ai.workers.schemas import (
    JobHandlerRegistry,
    PeriodicJobSpec,
    WorkerConfig,
    WorkerHealthSnapshot,
    default_worker_id,
    worker_config_from_settings,
)

logger = logging.getLogger("mem_ai.workers.runner")

__all__ = [
    "JobHandlerRegistry",
    "PeriodicJobSpec",
    "WorkerConfig",
    "WorkerHealthSnapshot",
    "WorkerRunner",
    "default_worker_id",
    "worker_config_from_settings",
]


class WorkerRunner:
    """Shared PostgreSQL job runner supporting concurrency, backoff, recovery, and graceful exit."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        registry: JobHandlerRegistry,
        config: WorkerConfig | None = None,
        periodic_jobs: Sequence[PeriodicJobSpec] = (),
        random_fn: Callable[[], float] | None = None,
        telemetry: TelemetryRegistry | None = None,
        tracer: Tracer | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._config = config if config is not None else WorkerConfig()
        self._periodic_jobs = tuple(periodic_jobs)
        self._random_fn = random_fn
        self._telemetry = telemetry if telemetry is not None else TelemetryRegistry()
        self._tracer: Tracer = tracer if tracer is not None else NoopTracer()
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._run_once_tasks: set[asyncio.Task[Any]] = set()
        self._concurrency_semaphore = asyncio.Semaphore(self._config.concurrency)
        self._reserved_slots = 0
        self._poll_loop_task: asyncio.Task[None] | None = None
        self._drain_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

        self._started_at: datetime | None = None
        self._last_poll_at: datetime | None = None
        self._last_claim_at: datetime | None = None
        self._last_success_at: datetime | None = None
        self._last_heartbeat_log_monotonic: float | None = None
        self._last_queue_observation_monotonic: float | None = None
        self._job_backlog_count = 0
        self._pending_reconciliation_count = 0
        self._processed_count = 0
        self._succeeded_count = 0
        self._retry_wait_count = 0
        self._reconciling_count = 0
        self._failed_count = 0
        self._cancelled_count = 0
        self._lease_lost_count = 0

    @property
    def config(self) -> WorkerConfig:
        """Return immutable worker runtime configuration."""
        return self._config

    @property
    def telemetry(self) -> TelemetryRegistry:
        """Return the Worker's low-cardinality `TelemetryRegistry`."""
        return self._telemetry

    def _emit_heartbeat(
        self,
        *,
        now: datetime | None = None,
        force_log: bool = False,
    ) -> None:
        """Update Worker heartbeat gauges, optional heartbeat file, and periodic JSON log."""
        current_time = now if now is not None else datetime.now(UTC)
        self._telemetry.record_worker_heartbeat(
            active_tasks=len(self._active_tasks),
            last_heartbeat_ts=current_time.timestamp(),
            last_claim_ts=self._last_claim_at.timestamp() if self._last_claim_at else None,
            last_success_ts=self._last_success_at.timestamp() if self._last_success_at else None,
        )
        if self._config.heartbeat_file:
            self._write_heartbeat_file(self._config.heartbeat_file)

        monotonic_now = time.perf_counter()
        if (
            force_log
            or self._last_heartbeat_log_monotonic is None
            or (monotonic_now - self._last_heartbeat_log_monotonic)
            >= self._config.heartbeat_interval_seconds
        ):
            self._last_heartbeat_log_monotonic = monotonic_now
            snap = self.health_snapshot()
            logger.info(
                "Worker heartbeat",
                extra={
                    "extra_fields": {
                        "worker_id": snap.worker_id,
                        "active_tasks": snap.active_tasks,
                        "processed_count": snap.processed_count,
                        "succeeded_count": snap.succeeded_count,
                        "reconciling_count": snap.reconciling_count,
                        "job_backlog_count": snap.job_backlog_count,
                        "pending_reconciliation_count": snap.pending_reconciliation_count,
                        "failed_count": snap.failed_count,
                        "shutting_down": snap.shutting_down,
                        "last_poll_at": snap.last_poll_at.isoformat()
                        if snap.last_poll_at
                        else None,
                        "last_claim_at": snap.last_claim_at.isoformat()
                        if snap.last_claim_at
                        else None,
                        "last_success_at": snap.last_success_at.isoformat()
                        if snap.last_success_at
                        else None,
                    }
                },
            )

    def _write_heartbeat_file(self, raw_path: str) -> None:
        try:
            target = Path(raw_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            payload = self.health_snapshot().model_dump(mode="json")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp_path, target)
        except OSError:
            logger.warning(
                "Failed to write Worker heartbeat file",
                extra={"extra_fields": {"worker_id": self._config.worker_id}},
                exc_info=True,
            )

    def request_shutdown(self) -> None:
        """Signal the runner to stop claiming new work and drain in-flight tasks."""
        self._stop_event.set()
        poll_task = self._poll_loop_task
        if poll_task is not None and not poll_task.done():
            poll_task.cancel()

    def health_snapshot(self) -> WorkerHealthSnapshot:
        """Return an immutable snapshot of Worker heartbeat and execution counters."""
        return WorkerHealthSnapshot(
            worker_id=self._config.worker_id,
            started_at=self._started_at,
            last_poll_at=self._last_poll_at,
            last_claim_at=self._last_claim_at,
            last_success_at=self._last_success_at,
            active_tasks=len(self._active_tasks),
            processed_count=self._processed_count,
            succeeded_count=self._succeeded_count,
            retry_wait_count=self._retry_wait_count,
            reconciling_count=self._reconciling_count,
            job_backlog_count=self._job_backlog_count,
            pending_reconciliation_count=self._pending_reconciliation_count,
            failed_count=self._failed_count,
            cancelled_count=self._cancelled_count,
            lease_lost_count=self._lease_lost_count,
            shutting_down=self._stop_event.is_set(),
        )

    async def _refresh_job_observability(self, *, force: bool = False) -> None:
        """Refresh bounded queue counts used in Worker heartbeat logs."""
        monotonic_now = time.perf_counter()
        if (
            not force
            and self._last_queue_observation_monotonic is not None
            and monotonic_now - self._last_queue_observation_monotonic
            < self._config.heartbeat_interval_seconds
        ):
            return

        self._last_queue_observation_monotonic = monotonic_now
        try:
            snapshot = await jobs_service.get_job_observability(self._session_factory)
        except Exception:
            logger.warning(
                "Failed to collect durable job counts for Worker heartbeat",
                extra={"extra_fields": {"worker_id": self._config.worker_id}},
                exc_info=True,
            )
            return

        self._job_backlog_count = snapshot.pending_count + snapshot.retry_wait_count
        self._pending_reconciliation_count = snapshot.reconciling_count

    async def _refresh_job_observability_bounded(
        self,
        *,
        force: bool = False,
        timeout_seconds: float | None = None,
    ) -> None:
        """Refresh queue counts within a tight timeout so shutdown cannot block on DB."""
        cap = (
            timeout_seconds
            if timeout_seconds is not None
            else min(0.5, max(0.02, self._config.shutdown_grace_seconds * 0.2))
        )
        if cap <= 0.0:
            return
        try:
            async with asyncio.timeout(cap):
                await self._refresh_job_observability(force=force)
        except TimeoutError:
            logger.warning(
                "Timed out collecting durable job counts for Worker heartbeat",
                extra={"extra_fields": {"worker_id": self._config.worker_id}},
            )

    def _available_claim_slots(self) -> int:
        return max(0, self._config.concurrency - len(self._active_tasks) - self._reserved_slots)

    async def _run_job_with_concurrency_guard(
        self,
        claimed: ClaimedJob,
        *,
        now: datetime | None = None,
    ) -> None:
        async with self._concurrency_semaphore:
            await self._execute_claimed_job(claimed, now=now)

    def _track_active_task(self, task: asyncio.Task[None]) -> None:
        self._active_tasks.add(task)

        def _observe_completion(done_task: asyncio.Task[None]) -> None:
            self._active_tasks.discard(done_task)
            if done_task.cancelled():
                return
            error = done_task.exception()
            if error is not None:
                logger.error(
                    "Worker task exited unexpectedly",
                    extra={"extra_fields": {"worker_id": self._config.worker_id}},
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(_observe_completion)

    async def schedule_periodic_jobs(self, *, now: datetime | None = None) -> int:
        """Idempotently enqueue window-deduplicated periodic jobs across multi-Worker replicas."""
        if not self._periodic_jobs:
            return 0

        created_count = 0
        async with self._session_factory.begin() as session:
            current_time = now if now is not None else datetime.now(UTC)
            for spec in self._periodic_jobs:
                if spec.interval_seconds <= 0:
                    continue
                bucket = int(current_time.timestamp() // spec.interval_seconds)
                dedupe_key = f"periodic:{spec.job_type}:{bucket}"
                _, created = await jobs_service.enqueue_job_in_session(
                    session,
                    job_type=spec.job_type,
                    business_id=f"window:{bucket}",
                    user_id=None,
                    dedupe_key=dedupe_key,
                    metadata_json={
                        "window_bucket": bucket,
                        **spec.metadata_json,
                    },
                    max_attempts=spec.max_attempts,
                    now=current_time,
                )
                if created:
                    created_count += 1
        return created_count

    async def _claim_and_spawn_tasks(
        self,
        *,
        registered_types: tuple[str, ...],
        requested: int,
        task_name: str,
        now: datetime | None = None,
        swallow_claim_errors: bool = False,
    ) -> list[asyncio.Task[None]]:
        if requested <= 0 or not registered_types:
            return []
        available_slots = self._available_claim_slots()
        if available_slots <= 0:
            return []

        limit = min(requested, available_slots)
        self._reserved_slots += limit
        tasks: list[asyncio.Task[None]] = []
        try:
            try:
                claimed_jobs = await jobs_service.claim_due_jobs(
                    self._session_factory,
                    worker_id=self._config.worker_id,
                    job_types=registered_types,
                    limit=limit,
                    lease_ttl_seconds=self._config.lease_ttl_seconds,
                    attempt_retention_days=self._config.attempt_retention_days,
                    now=now,
                )
            except Exception:
                if not swallow_claim_errors:
                    raise
                logger.exception(
                    "Error claiming due jobs from PostgreSQL",
                    extra={"extra_fields": {"worker_id": self._config.worker_id}},
                )
                return []

            if claimed_jobs:
                self._last_claim_at = now if now is not None else datetime.now(UTC)
                for item in claimed_jobs:
                    task = asyncio.create_task(
                        self._run_job_with_concurrency_guard(item, now=now),
                        name=task_name,
                    )
                    self._track_active_task(task)
                    tasks.append(task)
        finally:
            self._reserved_slots -= limit
        return tasks

    async def run_once(
        self,
        *,
        max_jobs: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Run one bounded claim-and-execute cycle; ideal for deterministic tests and CLI probes."""
        if self._stop_event.is_set():
            return 0

        cycle_task = asyncio.current_task()
        if cycle_task is not None:
            self._run_once_tasks.add(cycle_task)
        try:
            registered_types = self._registry.job_types()
            if not registered_types:
                return 0

            tel_tokens = bind_telemetry(telemetry=self._telemetry, tracer=self._tracer)
            try:
                await self.schedule_periodic_jobs(now=now)
                self._last_poll_at = now if now is not None else datetime.now(UTC)
                if self._stop_event.is_set():
                    self._emit_heartbeat(now=now)
                    return 0

                requested = max_jobs if max_jobs is not None else self._config.claim_batch_size
                tasks = await self._claim_and_spawn_tasks(
                    registered_types=registered_types,
                    requested=requested,
                    task_name="mem-ai-worker-run-once-job",
                    now=now,
                )
                if not tasks:
                    await self._refresh_job_observability(force=True)
                    self._emit_heartbeat(now=now)
                    return 0

                self._emit_heartbeat(now=now)
                await asyncio.gather(*tasks)
                await self._refresh_job_observability(force=True)
                self._emit_heartbeat(now=now, force_log=True)
                return len(tasks)
            finally:
                reset_telemetry(tel_tokens)
        finally:
            if cycle_task is not None:
                self._run_once_tasks.discard(cycle_task)

    async def _poll_loop(
        self,
        effective_stop: asyncio.Event,
        registered_types: tuple[str, ...],
    ) -> None:
        while not effective_stop.is_set() and not self._stop_event.is_set():
            try:
                await self.schedule_periodic_jobs()
            except Exception:
                logger.exception(
                    "Failed to schedule periodic jobs; retrying next poll",
                    extra={"extra_fields": {"worker_id": self._config.worker_id}},
                )

            await self._refresh_job_observability()
            self._last_poll_at = datetime.now(UTC)
            self._emit_heartbeat()
            claimed_tasks: list[asyncio.Task[None]] = []
            if not effective_stop.is_set() and not self._stop_event.is_set():
                claimed_tasks = await self._claim_and_spawn_tasks(
                    registered_types=registered_types,
                    requested=self._config.claim_batch_size,
                    task_name="mem-ai-worker-job",
                    swallow_claim_errors=True,
                )
                if claimed_tasks:
                    self._emit_heartbeat()

            if not claimed_tasks or self._available_claim_slots() <= 0:
                await self._wait_for_poll_or_stop(effective_stop)

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        """Continuously claim and execute jobs until shutdown, then drain within grace period."""
        effective_stop = stop_event if stop_event is not None else self._stop_event
        self._started_at = datetime.now(UTC)
        registered_types = self._registry.job_types()
        tel_tokens = bind_telemetry(telemetry=self._telemetry, tracer=self._tracer)
        logger.info(
            "Worker runner started",
            extra={
                "extra_fields": {
                    "worker_id": self._config.worker_id,
                    "concurrency": self._config.concurrency,
                    "job_types": list(registered_types),
                }
            },
        )
        if not effective_stop.is_set() and not self._stop_event.is_set():
            await self._refresh_job_observability(force=True)
        self._emit_heartbeat(force_log=True)

        try:
            if not effective_stop.is_set() and not self._stop_event.is_set():
                poll_task = asyncio.create_task(
                    self._poll_loop(effective_stop, registered_types),
                    name="mem-ai-worker-poll-loop",
                )
                self._poll_loop_task = poll_task
                try:
                    await poll_task
                except asyncio.CancelledError:
                    if not (effective_stop.is_set() or self._stop_event.is_set()):
                        raise
                finally:
                    if self._poll_loop_task is poll_task:
                        self._poll_loop_task = None
                    if not poll_task.done():
                        poll_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await poll_task
        finally:
            self._stop_event.set()
            shutdown_deadline = time.monotonic() + self._config.shutdown_grace_seconds
            await self._refresh_job_observability_bounded(force=True)
            self._emit_heartbeat(force_log=True)
            await self.drain(timeout_seconds=max(0.0, shutdown_deadline - time.monotonic()))
            await self._refresh_job_observability_bounded(force=True)
            self._emit_heartbeat(force_log=True)
            logger.info(
                "Worker runner stopped",
                extra={
                    "extra_fields": {
                        "worker_id": self._config.worker_id,
                        "processed_count": self._processed_count,
                    }
                },
            )
            reset_telemetry(tel_tokens)

    async def _wait_for_poll_or_stop(self, stop_event: asyncio.Event) -> None:
        waiters: list[asyncio.Task[Any]] = [
            asyncio.create_task(stop_event.wait()),
            asyncio.create_task(self._stop_event.wait()),
        ]
        try:
            await asyncio.wait(
                waiters,
                timeout=self._config.poll_interval_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def drain(self, *, timeout_seconds: float | None = None) -> None:
        """Drain in-flight work and bound cancellation cleanup for lease recovery."""
        current_task = asyncio.current_task()
        effective_timeout = (
            self._config.shutdown_grace_seconds if timeout_seconds is None else timeout_seconds
        )
        async with self._drain_lock:
            pending = set(self._active_tasks)
            pending.update(task for task in self._run_once_tasks if task is not current_task)
            cancellation_pending = await drain_tasks_bounded(
                pending,
                timeout_seconds=effective_timeout,
            )
            if cancellation_pending:
                logger.warning(
                    "Worker tasks remained active after cancellation grace",
                    extra={
                        "extra_fields": {
                            "worker_id": self._config.worker_id,
                            "active_task_count": len(cancellation_pending),
                        }
                    },
                )

    def _record_lease_lost(
        self,
        *,
        job_type: str,
        started_monotonic: float,
        span: Span,
    ) -> None:
        self._lease_lost_count += 1
        elapsed = max(0.0, time.perf_counter() - started_monotonic)
        self._telemetry.record_worker_job(
            job_type=job_type,
            status="lease_lost",
            duration_seconds=elapsed,
        )
        span.set_status("error", "job_lease_lost")

    async def _execute_claimed_job(
        self,
        claimed: ClaimedJob,
        *,
        now: datetime | None = None,
    ) -> None:
        started_monotonic = time.perf_counter()
        job = claimed.job
        handler = self._registry.get(job.job_type)
        raw_conv = job.metadata_json.get("conversation_id")
        raw_gen = job.metadata_json.get("generation_id")
        raw_provider = job.metadata_json.get("provider_id")
        log_tokens = bind_log_context(
            job_id=str(job.id),
            user_id=str(job.user_id) if job.user_id is not None else None,
            conversation_id=str(raw_conv) if isinstance(raw_conv, str) and raw_conv else None,
            generation_id=str(raw_gen) if isinstance(raw_gen, str) and raw_gen else None,
            provider=str(raw_provider) if isinstance(raw_provider, str) and raw_provider else None,
        )
        handler_task: asyncio.Task[JobHandlerOutcome] | None = None

        def _abort_on_lease_lost(_error: AppError) -> None:
            if handler_task is not None and not handler_task.done():
                handler_task.cancel()

        try:
            with self._tracer.start_span(
                "worker.job",
                attributes={
                    "job_type": job.job_type,
                    "job_id": str(job.id),
                    "attempt_number": claimed.attempt_number,
                },
            ) as span:
                if handler is None:
                    outcome = JobHandlerOutcome(
                        outcome="failed",
                        error_code="job_handler_not_registered",
                        error_summary=f"no handler registered for job_type={job.job_type}",
                        result_summary={"phase": "failed", "reason": "unregistered_job_type"},
                    )
                else:
                    ctx = JobExecutionContext(
                        job=job,
                        attempt_id=claimed.attempt_id,
                        attempt_number=claimed.attempt_number,
                        worker_id=claimed.worker_id,
                        lease_token=claimed.lease_token,
                    )
                    async with JobLeaseHeartbeat(
                        session_factory=self._session_factory,
                        claimed=claimed,
                        lease_ttl_seconds=self._config.lease_ttl_seconds,
                        renew_interval_seconds=self._config.lease_renew_interval_seconds,
                        on_lease_lost=_abort_on_lease_lost,
                    ) as heartbeat:
                        handler_task = asyncio.create_task(handler.handle(ctx))
                        try:
                            outcome = await handler_task
                        except asyncio.CancelledError:
                            if heartbeat.loss_error is not None:
                                self._record_lease_lost(
                                    job_type=job.job_type,
                                    started_monotonic=started_monotonic,
                                    span=span,
                                )
                                return
                            # Graceful shutdown cancellation: keep job `running` for lease reclaim.
                            span.set_status("error", "shutdown_cancelled")
                            raise
                        except ConflictError as exc:
                            if exc.code == "job_lease_lost":
                                self._record_lease_lost(
                                    job_type=job.job_type,
                                    started_monotonic=started_monotonic,
                                    span=span,
                                )
                                return
                            outcome = JobHandlerOutcome(
                                outcome="retryable",
                                error_code=exc.code,
                                error_summary="job handler encountered conflict",
                                result_summary={"phase": "conflict", "error_code": exc.code},
                            )
                        except Exception as exc:
                            span.record_exception(exc)
                            logger.exception(
                                "Unhandled exception inside job handler",
                                extra={
                                    "extra_fields": {
                                        "job_type": job.job_type,
                                        "attempt_number": claimed.attempt_number,
                                    }
                                },
                            )
                            outcome = JobHandlerOutcome(
                                outcome="retryable",
                                error_code="job_handler_error",
                                error_summary="unexpected job handler error",
                                result_summary={"phase": "handler_exception"},
                            )
                        finally:
                            await heartbeat.stop()
                            if handler_task is not None and not handler_task.done():
                                handler_task.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await handler_task

                        if heartbeat.loss_error is not None:
                            self._record_lease_lost(
                                job_type=job.job_type,
                                started_monotonic=started_monotonic,
                                span=span,
                            )
                            return

                updated = await jobs_service.record_job_outcome(
                    self._session_factory,
                    job_id=job.id,
                    attempt_id=claimed.attempt_id,
                    lease_token=claimed.lease_token,
                    outcome=outcome,
                    backoff_policy=self._config.backoff_policy,
                    random_fn=self._random_fn,
                    attempt_retention_days=self._config.attempt_retention_days,
                    now=now,
                )
                duration_seconds = max(0.0, time.perf_counter() - started_monotonic)
                self._processed_count += 1
                if updated is None:
                    self._record_lease_lost(
                        job_type=job.job_type,
                        started_monotonic=started_monotonic,
                        span=span,
                    )
                    logger.warning(
                        "Discarded stale job execution outcome after lease loss",
                        extra={
                            "extra_fields": {
                                "worker_id": self._config.worker_id,
                                "job_type": job.job_type,
                                "attempt_number": claimed.attempt_number,
                            }
                        },
                    )
                    return

                if updated.status == "succeeded":
                    self._succeeded_count += 1
                    self._last_success_at = now if now is not None else datetime.now(UTC)
                    span.set_status("ok")
                elif updated.status == "retry_wait":
                    self._retry_wait_count += 1
                    span.set_status("error", "retry_wait")
                elif updated.status == "reconciling":
                    self._reconciling_count += 1
                    span.set_status("ok", "reconciling")
                elif updated.status == "failed":
                    self._failed_count += 1
                    span.set_status("error", updated.last_error_code or "failed")
                elif updated.status == "cancelled":
                    self._cancelled_count += 1
                    span.set_status("ok", "cancelled")

                self._telemetry.record_worker_job(
                    job_type=updated.job_type,
                    status=updated.status,
                    duration_seconds=duration_seconds,
                )
                self._emit_heartbeat(now=now)
                duration_ms = round(duration_seconds * 1000.0, 2)
                logger.info(
                    "Processed durable job attempt",
                    extra={
                        "extra_fields": {
                            "worker_id": self._config.worker_id,
                            "job_type": updated.job_type,
                            "status": updated.status,
                            "attempt_count": updated.attempt_count,
                            "max_attempts": updated.max_attempts,
                            "external_ref": updated.external_ref,
                            "error_code": updated.last_error_code,
                            "next_run_at": updated.next_run_at.isoformat(),
                            "duration_ms": duration_ms,
                        }
                    },
                )
        finally:
            reset_log_context(log_tokens)
