"""Immutable application DTOs, handler protocols, and backoff policies for `jobs`."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

JobStatus = Literal[
    "pending",
    "running",
    "retry_wait",
    "reconciling",
    "succeeded",
    "failed",
    "cancelled",
]
JobAttemptStatus = Literal[
    "running",
    "succeeded",
    "retry_wait",
    "reconciling",
    "failed",
    "cancelled",
    "lease_lost",
]
JobOutcomeKind = Literal[
    "succeeded",
    "retryable",
    "reconciling",
    "failed",
    "cancelled",
]


class JobRecord(BaseModel):
    """Immutable snapshot of a persisted durable job."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    job_type: str
    business_id: str | None
    user_id: UUID | None
    dedupe_key: str | None
    status: JobStatus
    attempt_count: int
    max_attempts: int
    lease_token: str | None
    lease_expires_at: datetime | None
    next_run_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    external_ref: str | None
    last_error_code: str | None
    last_error_summary: str | None
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    def has_active_lease(self, now: datetime) -> bool:
        """Return True when this job currently holds an unexpired running lease."""
        return (
            self.status == "running"
            and self.lease_expires_at is not None
            and self.lease_expires_at > now
        )


class JobAttemptRecord(BaseModel):
    """Immutable snapshot of a sanitized job execution attempt summary."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    job_id: UUID
    attempt_number: int
    worker_id: str | None
    lease_token: str | None
    status: JobAttemptStatus
    started_at: datetime
    finished_at: datetime | None
    external_ref: str | None
    error_code: str | None
    result_summary: dict[str, Any]
    expires_at: datetime
    created_at: datetime


class ClaimedJob(BaseModel):
    """Claimed job paired with its active attempt ID and lease ownership token."""

    model_config = ConfigDict(frozen=True)

    job: JobRecord
    attempt_id: UUID
    attempt_number: int
    lease_token: str
    worker_id: str


class MemoryJobViewRecord(BaseModel):
    """Domain projection read from the read-only `memory_jobs` PostgreSQL view."""

    model_config = ConfigDict(frozen=True)

    id: UUID
    job_type: str
    business_id: str | None
    user_id: UUID | None
    dedupe_key: str | None
    status: JobStatus
    attempt_count: int
    max_attempts: int
    lease_token: str | None
    lease_expires_at: datetime | None
    next_run_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    external_ref: str | None
    last_error_code: str | None
    last_error_summary: str | None
    write_id: str | None
    provider_id: str | None
    scope: str | None
    conversation_id: str | None
    generation_id: str | None
    memory_epoch: int | None
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class JobBackoffPolicy(BaseModel):
    """Exponential backoff, jitter, and reconciliation polling configuration."""

    model_config = ConfigDict(frozen=True)

    base_delay_seconds: float = Field(default=2.0, gt=0.0)
    max_delay_seconds: float = Field(default=300.0, gt=0.0)
    jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)
    reconcile_delay_seconds: float = Field(default=10.0, gt=0.0)


class JobExecutionContext(BaseModel):
    """Immutable context supplied to a typed job handler during Worker execution."""

    model_config = ConfigDict(frozen=True)

    job: JobRecord
    attempt_id: UUID
    attempt_number: int
    worker_id: str
    lease_token: str


JobBeforeCommitHook = Callable[[AsyncSession, JobRecord], Awaitable[None]]
JobAfterCommitHook = Callable[[JobRecord], Awaitable[None]]


class JobHandlerOutcome(BaseModel):
    """Structured project result returned by any typed `JobHandler`."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    outcome: JobOutcomeKind
    external_ref: str | None = None
    error_code: str | None = None
    error_summary: str | None = None
    result_summary: dict[str, Any] = Field(default_factory=dict)
    metadata_patch: dict[str, Any] = Field(default_factory=dict)
    retry_after_seconds: float | None = Field(default=None, ge=0.0)
    before_commit: JobBeforeCommitHook | None = Field(default=None, exclude=True)
    after_commit: JobAfterCommitHook | None = Field(default=None, exclude=True)


class ResolvedJobTransition(BaseModel):
    """Pure state transition derived from a `JobHandlerOutcome` and retry budget."""

    model_config = ConfigDict(frozen=True)

    job_status: JobStatus
    attempt_status: JobAttemptStatus
    finished_at: datetime | None
    next_run_at: datetime | None


class JobObservabilitySnapshot(BaseModel):
    """Aggregated backlog and status metrics for durable job monitoring."""

    model_config = ConfigDict(frozen=True)

    status_counts: dict[str, int]
    pending_count: int
    running_count: int
    retry_wait_count: int
    reconciling_count: int
    failed_count: int
    oldest_due_at: datetime | None
    last_success_at: datetime | None = None


@runtime_checkable
class JobHandler(Protocol):
    """Typed job handler protocol executed by the shared Worker runner."""

    async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
        """Execute one leased attempt of a job outside any database transaction."""
        ...
