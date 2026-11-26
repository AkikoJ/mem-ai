"""Typed data retention policies, resumable scan cursors, and sanitized sweep summaries."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mem_ai.core.config import Settings
from mem_ai.db.base import normalize_utc

RetentionStage = Literal[
    "idempotency_records",
    "job_attempts",
    "memory_list_snapshots",
    "user_delete_operations",
    "operation_summaries",
    "periodic_jobs",
    "auth_sessions",
]

RETENTION_STAGES: tuple[RetentionStage, ...] = (
    "idempotency_records",
    "job_attempts",
    "memory_list_snapshots",
    "user_delete_operations",
    "operation_summaries",
    "periodic_jobs",
    "auth_sessions",
)

STAGE_COUNT_KEYS: dict[RetentionStage, str] = {
    "idempotency_records": "idempotency_records_deleted",
    "job_attempts": "job_attempts_deleted",
    "memory_list_snapshots": "memory_list_snapshots_deleted",
    "user_delete_operations": "user_delete_operations_deleted",
    "operation_summaries": "operation_summaries_deleted",
    "periodic_jobs": "periodic_jobs_deleted",
    "auth_sessions": "auth_sessions_deleted",
}

_TOTAL_COUNT_KEYS: tuple[str, ...] = (
    "idempotency_records_deleted",
    "job_attempts_deleted",
    "memory_list_snapshots_deleted",
    "user_delete_operations_deleted",
    "operation_summaries_deleted",
    "periodic_jobs_deleted",
    "auth_sessions_deleted",
    "protected_skipped",
)


def empty_retention_totals() -> dict[str, int]:
    """Return a zeroed copy of all sanitized retention counter keys."""
    return {key: 0 for key in _TOTAL_COUNT_KEYS}


class RetentionPolicy(BaseModel):
    """Server-side data retention periods and bounded sweep budgets."""

    model_config = ConfigDict(frozen=True)

    conversation_retention_policy: Literal["indefinite"] = "indefinite"
    generation_retention_policy: Literal["indefinite"] = "indefinite"
    recalled_memory_refs_follow_conversation: Literal[True] = True
    job_attempt_retention_days: int = Field(default=30, ge=1, le=365)
    idempotency_retention_hours: int = Field(default=24, ge=1, le=720)
    memory_list_snapshot_ttl_seconds: int = Field(default=600, ge=10, le=86400)
    deletion_receipt_ttl_seconds: int = Field(default=2592000, ge=60, le=31536000)
    operation_summary_retention_days: int = Field(default=30, ge=1, le=365)
    auth_sessions_retention_days: int = Field(default=90, ge=1, le=365)
    cleanup_interval_seconds: int = Field(default=900, ge=10, le=86400)
    batch_size: int = Field(default=100, ge=1, le=500)
    max_batches_per_run: int = Field(default=50, ge=1, le=1000)


def retention_policy_from_settings(settings: Settings) -> RetentionPolicy:
    """Derive immutable `RetentionPolicy` from validated application `Settings`."""
    return RetentionPolicy(
        conversation_retention_policy=settings.conversation_retention_policy,
        generation_retention_policy=settings.conversation_retention_policy,
        recalled_memory_refs_follow_conversation=True,
        job_attempt_retention_days=settings.job_attempt_retention_days,
        idempotency_retention_hours=settings.idempotency_retention_hours,
        memory_list_snapshot_ttl_seconds=settings.memory_list_snapshot_ttl_seconds,
        deletion_receipt_ttl_seconds=settings.deletion_receipt_ttl_seconds,
        operation_summary_retention_days=settings.operation_summary_retention_days,
        auth_sessions_retention_days=settings.auth_sessions_retention_days,
        cleanup_interval_seconds=settings.retention_cleanup_interval_seconds,
        batch_size=settings.retention_batch_size,
        max_batches_per_run=settings.retention_max_batches_per_run,
    )


class RetentionCursor(BaseModel):
    """Stable `(timestamp, id)` keyset cursor persisted between bounded cleanup batches."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    id: UUID

    @field_validator("timestamp")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        return normalize_utc(value)

    def as_tuple(self) -> tuple[datetime, UUID]:
        """Return `(timestamp, id)` tuple for resource-owner batch entry points."""
        return (self.timestamp, self.id)


class RetentionJobMetadata(BaseModel):
    """Durable checkpoint persisted in `jobs.metadata_json` for crash-resumable sweeps."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    stage_index: int = Field(default=0, ge=0, le=len(RETENTION_STAGES))
    cursor: RetentionCursor | None = None
    batches_completed: int = Field(default=0, ge=0)
    totals: dict[str, int] = Field(default_factory=empty_retention_totals)

    @field_validator("totals")
    @classmethod
    def _normalize_totals(cls, value: dict[str, int]) -> dict[str, int]:
        normalized = empty_retention_totals()
        for key in _TOTAL_COUNT_KEYS:
            raw = value.get(key, 0)
            normalized[key] = max(0, int(raw))
        return normalized

    def to_metadata_patch(self) -> dict[str, Any]:
        """Serialize into sanitized JSON-compatible job metadata fields."""
        return {
            "stage_index": self.stage_index,
            "cursor": (
                {"timestamp": self.cursor.timestamp.isoformat(), "id": str(self.cursor.id)}
                if self.cursor is not None
                else None
            ),
            "batches_completed": self.batches_completed,
            "totals": dict(self.totals),
        }


class RetentionBatchResult(BaseModel):
    """Outcome of one short-transaction retention batch for a single stage."""

    model_config = ConfigDict(frozen=True)

    stage: RetentionStage
    deleted_count: int = 0
    protected_skipped: int = 0
    scanned_count: int = 0
    next_cursor: RetentionCursor | None = None
    has_more: bool = False


class RetentionSweepSummary(BaseModel):
    """Sanitized counts, timing, and failure summary emitted to logs and job attempts."""

    model_config = ConfigDict(frozen=True)

    idempotency_records_deleted: int = 0
    job_attempts_deleted: int = 0
    memory_list_snapshots_deleted: int = 0
    user_delete_operations_deleted: int = 0
    operation_summaries_deleted: int = 0
    periodic_jobs_deleted: int = 0
    auth_sessions_deleted: int = 0
    protected_skipped: int = 0
    batches_executed: int = 0
    duration_ms: float = 0.0
    completed: bool = True
    failure_summary: str | None = None

    @property
    def total_deleted(self) -> int:
        """Return total rows removed across all retention targets."""
        return (
            self.idempotency_records_deleted
            + self.job_attempts_deleted
            + self.memory_list_snapshots_deleted
            + self.user_delete_operations_deleted
            + self.operation_summaries_deleted
            + self.periodic_jobs_deleted
            + self.auth_sessions_deleted
        )

    def to_result_summary(self) -> dict[str, Any]:
        """Return sanitized JSON dict containing only counts, duration, status, and safe summary."""
        payload: dict[str, Any] = {
            "idempotency_records_deleted": self.idempotency_records_deleted,
            "job_attempts_deleted": self.job_attempts_deleted,
            "memory_list_snapshots_deleted": self.memory_list_snapshots_deleted,
            "user_delete_operations_deleted": self.user_delete_operations_deleted,
            "operation_summaries_deleted": self.operation_summaries_deleted,
            "periodic_jobs_deleted": self.periodic_jobs_deleted,
            "auth_sessions_deleted": self.auth_sessions_deleted,
            "total_deleted": self.total_deleted,
            "protected_skipped": self.protected_skipped,
            "batches_executed": self.batches_executed,
            "duration_ms": round(self.duration_ms, 2),
            "completed": self.completed,
        }
        if self.failure_summary is not None:
            payload["failure_summary"] = self.failure_summary
        return payload
