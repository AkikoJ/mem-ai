"""Shared Worker runner: handler registry, SKIP LOCKED polling, leases, and shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import AppError, ConflictError
from mem_ai.core.logging import bind_log_context, reset_log_context
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import (
    ClaimedJob,
    JobBackoffPolicy,
    JobExecutionContext,
    JobHandler,
    JobHandlerOutcome,
)

logger = logging.getLogger("mem_ai.workers.runner")


def default_worker_id() -> str:
    """Generate a unique identifier for a Worker runner process."""
    host = socket.gethostname() or "localhost"
    return f"worker-{host}-{os.getpid()}-{uuid4().hex[:8]}"


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Runtime concurrency, polling, lease, backoff, and shutdown settings for `WorkerRunner`."""

    worker_id: str = field(default_factory=default_worker_id)
    concurrency: int = 4
    poll_interval_seconds: float = 1.0
    claim_batch_size: int = 10
    lease_ttl_seconds: float = 60.0
    lease_renew_interval_seconds: float | None = 20.0
    backoff_policy: JobBackoffPolicy = field(default_factory=JobBackoffPolicy)
    attempt_retention_days: int = 30
    shutdown_grace_seconds: float = 15.0


def worker_config_from_settings(
    settings: Settings,
    *,
    worker_id: str | None = None,
) -> WorkerConfig:
    """Build `WorkerConfig` from validated application `Settings`."""
    return WorkerConfig(
        worker_id=worker_id or default_worker_id(),
        concurrency=settings.worker_concurrency,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        claim_batch_size=settings.worker_claim_batch_size,
        lease_ttl_seconds=float(settings.worker_lease_ttl_seconds),
        lease_renew_interval_seconds=settings.worker_lease_renew_interval_seconds,
        backoff_policy=JobBackoffPolicy(
            base_delay_seconds=settings.worker_backoff_base_seconds,
            max_delay_seconds=settings.worker_backoff_max_seconds,
            jitter_ratio=settings.worker_backoff_jitter_ratio,
            reconcile_delay_seconds=settings.worker_reconcile_delay_seconds,
        ),
        attempt_retention_days=settings.job_attempt_retention_days,
        shutdown_grace_seconds=settings.worker_shutdown_grace_seconds,
    )


@dataclass(frozen=True, slots=True)
class PeriodicJobSpec:
    """Periodic window job specification deduplicated via PostgreSQL `dedupe_key`."""

    job_type: str
    interval_seconds: float
    max_attempts: int = 3
    metadata_json: dict[str, Any] = field(default_factory=dict)


class WorkerHealthSnapshot(BaseModel):
    """Observable runtime heartbeat and counters exported by a `WorkerRunner`."""

    model_config = ConfigDict(frozen=True)

    worker_id: str
    started_at: datetime | None = None
    last_poll_at: datetime | None = None
    last_claim_at: datetime | None = None
    last_success_at: datetime | None = None
    active_tasks: int = 0
    processed_count: int = 0
    succeeded_count: int = 0
    retry_wait_count: int = 0
    reconciling_count: int = 0
    failed_count: int = 0
    cancelled_count: int = 0
    lease_lost_count: int = 0
    shutting_down: bool = False


class JobHandlerRegistry:
    """Type-keyed registry mapping `jobs.job_type` to concrete `JobHandler` instances."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        """Register a handler for `job_type`, rejecting blanks and duplicates."""
        normalized = job_type.strip()
        if not normalized:
            raise ValueError("job_type must not be empty")
        if normalized in self._handlers:
            raise ValueError(f"job handler already registered for job_type={normalized!r}")
        self._handlers[normalized] = handler

    def get(self, job_type: str) -> JobHandler | None:
        """Look up a registered handler by `job_type`."""
        return self._handlers.get(job_type)

    def job_types(self) -> tuple[str, ...]:
        """Return all registered `job_type` keys in deterministic order."""
        return tuple(sorted(self._handlers.keys()))


class JobLeaseHeartbeat:
    """Periodic lease renewal context manager protecting long-running job handlers."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        claimed: ClaimedJob,
        lease_ttl_seconds: float,
        renew_interval_seconds: float | None,
        on_lease_lost: Callable[[AppError], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._claimed = claimed
        self._lease_ttl_seconds = lease_ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._on_lease_lost = on_lease_lost
        self._task: asyncio.Task[None] | None = None
        self.loss_error: AppError | None = None

    async def __aenter__(self) -> JobLeaseHeartbeat:
        if self._renew_interval_seconds is not None and self._renew_interval_seconds > 0:
            self._task = asyncio.create_task(self._run_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)
        await self.stop()

    async def stop(self) -> None:
        """Stop background lease renewal before conditional outcome commit."""
        task = self._task
        self._task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run_loop(self) -> None:
        assert self._renew_interval_seconds is not None
        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                await jobs_service.renew_job_lease(
                    self._session_factory,
                    job_id=self._claimed.job.id,
                    lease_token=self._claimed.lease_token,
                    lease_ttl_seconds=self._lease_ttl_seconds,
                )
            except ConflictError as exc:
                self.loss_error = exc
                if self._on_lease_lost is not None:
                    self._on_lease_lost(exc)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Transient error renewing job lease; retrying before expiry",
                    extra={
                        "extra_fields": {
                            "job_id": str(self._claimed.job.id),
                            "job_type": self._claimed.job.job_type,
                        }
                    },
                    exc_info=True,
                )


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
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._config = config if config is not None else WorkerConfig()
        self._periodic_jobs = tuple(periodic_jobs)
        self._random_fn = random_fn
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()

        self._started_at: datetime | None = None
        self._last_poll_at: datetime | None = None
        self._last_claim_at: datetime | None = None
        self._last_success_at: datetime | None = None
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

    def request_shutdown(self) -> None:
        """Signal the runner to stop claiming new work and drain in-flight tasks."""
        self._stop_event.set()

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
            failed_count=self._failed_count,
            cancelled_count=self._cancelled_count,
            lease_lost_count=self._lease_lost_count,
            shutting_down=self._stop_event.is_set(),
        )

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

    async def run_once(
        self,
        *,
        max_jobs: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Run one bounded claim-and-execute cycle; ideal for deterministic tests and CLI probes."""
        registered_types = self._registry.job_types()
        if not registered_types:
            return 0

        await self.schedule_periodic_jobs(now=now)
        self._last_poll_at = now if now is not None else datetime.now(UTC)

        limit = min(
            max_jobs if max_jobs is not None else self._config.claim_batch_size,
            self._config.concurrency,
        )
        claimed_jobs = await jobs_service.claim_due_jobs(
            self._session_factory,
            worker_id=self._config.worker_id,
            job_types=registered_types,
            limit=max(1, limit),
            lease_ttl_seconds=self._config.lease_ttl_seconds,
            attempt_retention_days=self._config.attempt_retention_days,
            now=now,
        )
        if not claimed_jobs:
            return 0

        self._last_claim_at = now if now is not None else datetime.now(UTC)
        semaphore = asyncio.Semaphore(self._config.concurrency)

        async def _bounded_exec(item: ClaimedJob) -> None:
            async with semaphore:
                await self._execute_claimed_job(item, now=now)

        await asyncio.gather(*(_bounded_exec(item) for item in claimed_jobs))
        return len(claimed_jobs)

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        """Continuously claim and execute jobs until shutdown, then drain within grace period."""
        effective_stop = stop_event if stop_event is not None else self._stop_event
        self._started_at = datetime.now(UTC)
        registered_types = self._registry.job_types()
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

        try:
            while not effective_stop.is_set() and not self._stop_event.is_set():
                try:
                    await self.schedule_periodic_jobs()
                except Exception:
                    logger.exception(
                        "Failed to schedule periodic jobs; retrying next poll",
                        extra={"extra_fields": {"worker_id": self._config.worker_id}},
                    )

                self._last_poll_at = datetime.now(UTC)
                available_slots = self._config.concurrency - len(self._active_tasks)
                claimed_count = 0

                if available_slots > 0 and registered_types:
                    claim_limit = min(available_slots, self._config.claim_batch_size)
                    try:
                        claimed_jobs = await jobs_service.claim_due_jobs(
                            self._session_factory,
                            worker_id=self._config.worker_id,
                            job_types=registered_types,
                            limit=claim_limit,
                            lease_ttl_seconds=self._config.lease_ttl_seconds,
                            attempt_retention_days=self._config.attempt_retention_days,
                        )
                    except Exception:
                        logger.exception(
                            "Error claiming due jobs from PostgreSQL",
                            extra={"extra_fields": {"worker_id": self._config.worker_id}},
                        )
                        claimed_jobs = []

                    claimed_count = len(claimed_jobs)
                    if claimed_jobs:
                        self._last_claim_at = datetime.now(UTC)
                        for claimed in claimed_jobs:
                            task = asyncio.create_task(self._execute_claimed_job(claimed))
                            self._active_tasks.add(task)
                            task.add_done_callback(self._active_tasks.discard)

                if claimed_count == 0 or len(self._active_tasks) >= self._config.concurrency:
                    await self._wait_for_poll_or_stop(effective_stop)
        finally:
            self._stop_event.set()
            await self._drain_active_tasks()
            logger.info(
                "Worker runner stopped",
                extra={
                    "extra_fields": {
                        "worker_id": self._config.worker_id,
                        "processed_count": self._processed_count,
                    }
                },
            )

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

    async def _drain_active_tasks(self) -> None:
        """Give in-flight jobs bounded time to converge; cancel stragglers for lease recovery."""
        if not self._active_tasks:
            return
        pending = list(self._active_tasks)
        _, still_pending = await asyncio.wait(
            pending,
            timeout=self._config.shutdown_grace_seconds,
        )
        for task in still_pending:
            task.cancel()
        if still_pending:
            await asyncio.gather(*still_pending, return_exceptions=True)

    async def _execute_claimed_job(
        self,
        claimed: ClaimedJob,
        *,
        now: datetime | None = None,
    ) -> None:
        started_monotonic = time.perf_counter()
        job = claimed.job
        handler = self._registry.get(job.job_type)
        log_tokens = bind_log_context(
            job_id=str(job.id),
            user_id=str(job.user_id) if job.user_id is not None else None,
        )
        handler_task: asyncio.Task[JobHandlerOutcome] | None = None

        def _abort_on_lease_lost(_error: AppError) -> None:
            if handler_task is not None and not handler_task.done():
                handler_task.cancel()

        try:
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
                            self._lease_lost_count += 1
                            return
                        # Graceful shutdown cancellation: keep job `running` for lease reclaim.
                        raise
                    except ConflictError as exc:
                        if exc.code == "job_lease_lost":
                            self._lease_lost_count += 1
                            return
                        outcome = JobHandlerOutcome(
                            outcome="retryable",
                            error_code=exc.code,
                            error_summary="job handler encountered conflict",
                            result_summary={"phase": "conflict", "error_code": exc.code},
                        )
                    except Exception:
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
                        if handler_task is not None and not handler_task.done():
                            handler_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await handler_task

                    if heartbeat.loss_error is not None:
                        self._lease_lost_count += 1
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
            self._processed_count += 1
            if updated is None:
                self._lease_lost_count += 1
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
            elif updated.status == "retry_wait":
                self._retry_wait_count += 1
            elif updated.status == "reconciling":
                self._reconciling_count += 1
            elif updated.status == "failed":
                self._failed_count += 1
            elif updated.status == "cancelled":
                self._cancelled_count += 1

            duration_ms = round((time.perf_counter() - started_monotonic) * 1000.0, 2)
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
