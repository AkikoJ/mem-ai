"""Unit tests for §11.3.1 deletion state machines, audit archive seam, and clear helpers."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryWriteInput,
    MemoryWriteResult,
    provider_supports_write_status_verification,
)
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.modules.jobs.schemas import JobRecord, JobStatus
from mem_ai.modules.memories.barrier import (
    is_old_epoch_write_job,
    is_valid_barrier_transition,
    is_valid_tombstone_transition,
    is_write_job_already_audited_for_clear,
    is_write_job_settled_for_clear,
)
from mem_ai.modules.operations.audit import (
    DeletionAuditEvent,
    JsonlDeletionAuditArchiver,
    NoopDeletionAuditArchiver,
    build_deletion_audit_archiver,
    export_deletion_audit_events_safely,
)
from mem_ai.modules.operations.service import is_valid_operation_transition


def _make_job(
    *,
    job_type: str = "memory_turn_write",
    status: JobStatus = "pending",
    metadata_json: dict[str, Any] | None = None,
    last_error_code: str | None = None,
) -> JobRecord:
    now = datetime.now(UTC)
    return JobRecord(
        id=uuid4(),
        job_type=job_type,
        business_id="biz-1",
        user_id=uuid4(),
        dedupe_key=f"{job_type}:{uuid4().hex}",
        status=status,
        attempt_count=1,
        max_attempts=5,
        lease_token=None,
        lease_expires_at=None,
        next_run_at=now,
        started_at=now,
        finished_at=None,
        external_ref="ext-1",
        last_error_code=last_error_code,
        last_error_summary=None,
        metadata_json=metadata_json or {},
        created_at=now,
        updated_at=now,
    )


def test_section_11_3_1_explicit_state_machines() -> None:
    # 1. operations.status transitions
    assert is_valid_operation_transition("pending", "running") is True
    assert is_valid_operation_transition("pending", "failed") is True
    assert is_valid_operation_transition("pending", "succeeded") is False
    assert is_valid_operation_transition("pending", "reconciling") is False

    assert is_valid_operation_transition("running", "running") is True
    assert is_valid_operation_transition("running", "reconciling") is True
    assert is_valid_operation_transition("running", "succeeded") is True
    assert is_valid_operation_transition("running", "failed") is True
    assert is_valid_operation_transition("running", "pending") is False

    assert is_valid_operation_transition("reconciling", "reconciling") is True
    assert is_valid_operation_transition("reconciling", "succeeded") is True
    assert is_valid_operation_transition("reconciling", "failed") is True
    assert is_valid_operation_transition("reconciling", "running") is False
    assert is_valid_operation_transition("reconciling", "pending") is False

    for target in ("pending", "running", "reconciling", "succeeded", "failed"):
        assert is_valid_operation_transition("succeeded", target) is False  # type: ignore[arg-type]
        assert is_valid_operation_transition("failed", target) is False  # type: ignore[arg-type]

    # 2. memory_scopes.barrier_state transitions
    assert is_valid_barrier_transition("idle", "clearing") is True
    assert is_valid_barrier_transition("idle", "reconciling") is False
    assert is_valid_barrier_transition("idle", "idle") is False

    assert is_valid_barrier_transition("clearing", "clearing") is True
    assert is_valid_barrier_transition("clearing", "reconciling") is True
    assert is_valid_barrier_transition("clearing", "idle") is True

    assert is_valid_barrier_transition("reconciling", "reconciling") is True
    assert is_valid_barrier_transition("reconciling", "idle") is True
    assert is_valid_barrier_transition("reconciling", "clearing") is False

    # 3. memory_tombstones.status transitions
    assert is_valid_tombstone_transition("pending", "reconciling") is True
    assert is_valid_tombstone_transition("pending", "verified") is True
    assert is_valid_tombstone_transition("pending", "pending") is False

    assert is_valid_tombstone_transition("reconciling", "reconciling") is True
    assert is_valid_tombstone_transition("reconciling", "verified") is True
    assert is_valid_tombstone_transition("reconciling", "pending") is False

    for target in ("pending", "reconciling", "verified"):
        assert is_valid_tombstone_transition("verified", target) is False  # type: ignore[arg-type]


def test_old_epoch_and_settlement_predicates() -> None:
    turn_old = _make_job(
        job_type="memory_turn_write",
        status="pending",
        metadata_json={"provider_id": "memos", "memory_epoch": 1},
    )
    turn_new = _make_job(
        job_type="memory_turn_write",
        status="pending",
        metadata_json={"provider_id": "memos", "memory_epoch": 2},
    )
    other_provider = _make_job(
        job_type="memory_turn_write",
        status="pending",
        metadata_json={"provider_id": "other", "memory_epoch": 0},
    )
    assert is_old_epoch_write_job(turn_old, provider_id="memos", max_epoch=1) is True
    assert is_old_epoch_write_job(turn_new, provider_id="memos", max_epoch=1) is False
    assert is_old_epoch_write_job(other_provider, provider_id="memos", max_epoch=1) is False

    settled_turn = _make_job(
        job_type="memory_turn_write",
        status="reconciling",
        metadata_json={"dispatch_state": "confirmed_during_clear"},
    )
    unsettled_turn = _make_job(
        job_type="memory_turn_write",
        status="reconciling",
        metadata_json={"dispatch_state": "uncertain"},
    )
    settled_explicit = _make_job(
        job_type="memory_explicit_create",
        status="reconciling",
        last_error_code="memory_scope_clearing",
        metadata_json={"memory_id": "mem_1"},
    )
    audited = _make_job(
        job_type="memory_turn_write",
        status="reconciling",
        metadata_json={"superseded_by_clear_operation_id": str(uuid4())},
    )
    assert is_write_job_settled_for_clear(settled_turn) is True
    assert is_write_job_settled_for_clear(unsettled_turn) is False
    assert is_write_job_settled_for_clear(settled_explicit) is True
    assert is_write_job_already_audited_for_clear(audited) is True
    assert is_write_job_already_audited_for_clear(unsettled_turn) is False


def test_provider_write_status_verification_capability_helper() -> None:
    class _MinimalProvider:
        provider_id = "minimal"

        async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
            return []

        async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
            return MemoryWriteResult(status="succeeded", provider_id=self.provider_id)

        async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
            return MemoryWriteResult(status="succeeded", provider_id=self.provider_id)

        async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
            return MemoryListPage(items=[])

        async def delete_memory(self, *, user_id: Any, memory_id: str) -> MemoryDeleteResult:
            return MemoryDeleteResult(status="succeeded")

        async def clear_memories(self, *, user_id: Any) -> MemoryDeleteResult:
            return MemoryDeleteResult(status="succeeded")

        async def verify_operation(
            self, *, user_id: Any, external_ref: str
        ) -> MemoryOperationStatus:
            return MemoryOperationStatus(status="reconciling")

    class _NoVerifyProvider(_MinimalProvider):
        supports_write_status_verification = False

    assert provider_supports_write_status_verification(_MinimalProvider()) is True
    assert provider_supports_write_status_verification(_NoVerifyProvider()) is False


@pytest.mark.anyio
async def test_jsonl_deletion_audit_archiver_appends_sanitized_events_and_handles_errors(
    tmp_path: Path,
) -> None:
    assert isinstance(build_deletion_audit_archiver(None), NoopDeletionAuditArchiver)
    archive_file = tmp_path / "nested" / "deletion-audit.jsonl"
    archiver = build_deletion_audit_archiver(str(archive_file))
    assert isinstance(archiver, JsonlDeletionAuditArchiver)

    uid = uuid4()
    op_id = uuid4()
    ev1 = DeletionAuditEvent(
        event_type="memory_epoch_advanced",
        user_id=uid,
        operation_id=op_id,
        operation_type="memory_clear",
        provider_id="memos",
        target_type="memory_scope",
        target_id=f"{uid}:memos",
        memory_epoch=2,
        barrier_state="clearing",
        metadata={
            "previous_epoch": 1,
            "api_key": "sk-secret-must-redact",
            "content": "PRIVATE_USER_TEXT",
        },
    )
    ev2 = DeletionAuditEvent(
        event_type="memory_barrier_released",
        user_id=uid,
        operation_id=op_id,
        operation_type="memory_clear",
        provider_id="memos",
        target_type="memory_scope",
        target_id=f"{uid}:memos",
        memory_epoch=2,
        status="succeeded",
        barrier_state="idle",
    )
    await export_deletion_audit_events_safely(archiver, (ev1, ev2))

    raw = archive_file.read_text(encoding="utf-8")
    assert "sk-secret-must-redact" not in raw
    assert "PRIVATE_USER_TEXT" not in raw
    lines = [json.loads(line) for line in raw.splitlines() if line.strip()]
    assert len(lines) == 2
    assert lines[0]["event_type"] == "memory_epoch_advanced"
    assert lines[0]["memory_epoch"] == 2
    assert lines[0]["metadata"]["previous_epoch"] == 1
    assert lines[0]["metadata"]["api_key"] == "[REDACTED]"
    assert lines[0]["metadata"]["content"] == "[REDACTED]"
    assert lines[1]["event_type"] == "memory_barrier_released"
    assert lines[1]["barrier_state"] == "idle"

    # Archiver failure logs structured alert without crashing caller.
    class _BrokenArchiver:
        async def export_event(self, event: DeletionAuditEvent) -> None:
            _ = event
            raise OSError("disk full")

    log_buf = StringIO()
    handler = logging.StreamHandler(log_buf)
    handler.setFormatter(JsonLogFormatter())
    audit_logger = logging.getLogger("mem_ai.operations.audit")
    audit_logger.addHandler(handler)
    try:
        await export_deletion_audit_events_safely(_BrokenArchiver(), (ev1,))
    finally:
        audit_logger.removeHandler(handler)

    parsed_logs = [json.loads(line) for line in log_buf.getvalue().splitlines() if line.strip()]
    assert any(entry.get("alert") == "deletion_audit_export_failed" for entry in parsed_logs)
