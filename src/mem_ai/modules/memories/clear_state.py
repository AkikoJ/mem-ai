"""Durable memory-clear phases, execution metadata, and atomic recovery transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from mem_ai.modules.jobs.schemas import JobBeforeCommitHook, JobRecord
from mem_ai.modules.memories.barrier import (
    transition_scope_barrier_in_session,
    transition_tombstone_in_session,
)
from mem_ai.modules.operations import service as operations_service
from mem_ai.modules.operations.schemas import OperationStatus

MemoryClearPhase = Literal[
    "await_inflight",
    "execute_final_clear",
    "clear_dispatched",
    "verify_clear",
    "compensation_clear",
    "completed",
    "failed",
]


@dataclass(frozen=True, slots=True)
class ClearExecutionPlan:
    """Bundled state carried across final clear and delayed compensation clear steps."""

    is_compensation: bool = False
    settle_timeout_exceeded: bool = False
    unsettled_audit_job_ids: tuple[str, ...] = ()


class MemoryClearJobMetadata(BaseModel):
    """Validated metadata persisted for a `memory_clear` durable job."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    operation_id: UUID
    user_cleanup_operation_id: UUID | None = None
    tombstone_id: UUID | None = None
    provider_id: str
    previous_epoch: int = 0
    new_epoch: int = 1
    barrier_started_at: datetime
    clear_phase: MemoryClearPhase = "await_inflight"
    settle_timeout_exceeded: bool = False
    compensation_scheduled: bool = False
    verifying_compensation: bool = False
    unsettled_audit_job_ids: list[str] = Field(default_factory=list)
    inflight_scan_after_id: UUID | None = None


def make_hold_barrier_hook(
    *,
    user_id: UUID,
    meta: MemoryClearJobMetadata,
    op_status: OperationStatus,
    phase: str,
    current_time: datetime,
    barrier_state: Literal["clearing", "reconciling"] = "reconciling",
    error_code: str | None = None,
    error_message: str | None = None,
    tombstone_external_ref: str | None = None,
    extra_progress: dict[str, Any] | None = None,
    fail_op_when_job_exhausted: bool = False,
    exhausted_error_message: str | None = None,
) -> JobBeforeCommitHook:
    async def _hook(session: AsyncSession, updated_job: JobRecord) -> None:
        is_exhausted = fail_op_when_job_exhausted and updated_job.status == "failed"
        effective_status: OperationStatus = "failed" if is_exhausted else op_status
        effective_phase = "failed" if is_exhausted else phase
        effective_message = (
            exhausted_error_message or error_message if is_exhausted else error_message
        )
        retry_required = (
            meta.user_cleanup_operation_id is not None and updated_job.status == "failed"
        )
        if retry_required:
            # The job stops at its retry boundary. Keep the deletion-owned
            # operation recoverable through the controlled jobs requeue entry.
            effective_status = "reconciling"
            effective_phase = "awaiting_retry"
        progress: dict[str, Any] = {
            "phase": effective_phase,
            "provider_id": meta.provider_id,
            "previous_epoch": meta.previous_epoch,
            "new_epoch": meta.new_epoch,
            "barrier_state": barrier_state,
            "retry_required": retry_required,
        }
        if extra_progress:
            progress.update(extra_progress)
        if barrier_state == "reconciling":
            await transition_scope_barrier_in_session(
                session,
                user_id=user_id,
                provider_id=meta.provider_id,
                new_barrier_state="reconciling",
                expected_barrier_operation_id=meta.operation_id,
                now=current_time,
            )
        if tombstone_external_ref is not None and meta.tombstone_id is not None:
            await transition_tombstone_in_session(
                session,
                tombstone_id=meta.tombstone_id,
                status="reconciling",
                external_ref=tombstone_external_ref,
                now=current_time,
            )
        await operations_service.transition_operation_in_session(
            session,
            operation_id=meta.operation_id,
            status=effective_status,
            progress=progress,
            error_code=error_code,
            error_message=effective_message,
            now=current_time,
        )

    return _hook
