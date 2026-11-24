"""Unit tests for data retention policy derivation, cursor state, and sanitized summaries."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from mem_ai.core.config import Settings
from mem_ai.core.logging import REDACTED_VALUE, sanitize_log_value
from mem_ai.modules.retention.schemas import (
    RETENTION_STAGES,
    RetentionCursor,
    RetentionJobMetadata,
    RetentionSweepSummary,
    retention_policy_from_settings,
)


def test_retention_policy_defaults_preserve_conversations_generations_and_recalled_refs() -> None:
    policy = retention_policy_from_settings(Settings(_env_file=None, app_env="test"))

    assert policy.conversation_retention_policy == "indefinite"
    assert policy.generation_retention_policy == "indefinite"
    assert policy.recalled_memory_refs_follow_conversation is True
    assert policy.job_attempt_retention_days == 30
    assert policy.idempotency_retention_hours == 24
    assert policy.memory_list_snapshot_ttl_seconds == 600
    assert policy.deletion_receipt_ttl_seconds == 2592000
    assert policy.operation_summary_retention_days == 30
    assert policy.cleanup_interval_seconds == 900
    assert policy.batch_size == 100
    assert policy.max_batches_per_run == 50


def test_retention_job_metadata_round_trips_cursor_and_sanitized_totals() -> None:
    ts = datetime(2026, 11, 24, 10, 0, 0, tzinfo=UTC)
    row_id = uuid4()
    meta = RetentionJobMetadata(
        stage_index=2,
        cursor=RetentionCursor(timestamp=ts, id=row_id),
        batches_completed=3,
        totals={
            "idempotency_records_deleted": 4,
            "job_attempts_deleted": 7,
            "memory_list_snapshots_deleted": 2,
            "user_delete_operations_deleted": 1,
            "operation_summaries_deleted": 3,
            "periodic_jobs_deleted": 5,
            "protected_skipped": 6,
        },
    )

    dumped = meta.to_metadata_patch()
    restored = RetentionJobMetadata.model_validate(dumped)
    assert restored == meta
    assert RETENTION_STAGES[restored.stage_index] == "memory_list_snapshots"


def test_retention_sweep_summary_log_fields_contain_no_redacted_or_sensitive_keys() -> None:
    summary = RetentionSweepSummary(
        idempotency_records_deleted=3,
        job_attempts_deleted=5,
        memory_list_snapshots_deleted=2,
        user_delete_operations_deleted=1,
        operation_summaries_deleted=4,
        periodic_jobs_deleted=2,
        protected_skipped=7,
        batches_executed=6,
        duration_ms=12.5,
        completed=True,
        failure_summary=None,
    )
    payload = summary.to_result_summary()
    sanitized = sanitize_log_value(None, payload)
    assert sanitized == payload
    assert REDACTED_VALUE not in str(sanitized)
    assert payload["total_deleted"] == 17
