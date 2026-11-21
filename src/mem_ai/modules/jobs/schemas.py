"""Immutable application DTOs for the unified `jobs` module."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

JobStatus = Literal[
    "pending",
    "running",
    "retry_wait",
    "reconciling",
    "succeeded",
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
