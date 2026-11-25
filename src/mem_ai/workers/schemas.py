"""Configuration, health snapshot schemas, and handler registry for `WorkerRunner`."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from mem_ai.core.config import Settings
from mem_ai.modules.jobs.schemas import JobBackoffPolicy, JobHandler


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
    heartbeat_interval_seconds: float = 30.0
    heartbeat_file: str | None = None


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
        heartbeat_interval_seconds=settings.worker_heartbeat_interval_seconds,
        heartbeat_file=settings.worker_heartbeat_file,
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
    job_backlog_count: int = 0
    pending_reconciliation_count: int = 0
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
