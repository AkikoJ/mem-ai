"""Unit tests for durable job backoff, jitter, and outcome transition rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from mem_ai.modules.jobs.schemas import JobBackoffPolicy, JobHandlerOutcome
from mem_ai.modules.jobs.service import (
    compute_backoff_delay_seconds,
    resolve_job_transition,
)


def test_exponential_backoff_scales_and_caps_with_zero_jitter() -> None:
    policy = JobBackoffPolicy(
        base_delay_seconds=2.0,
        max_delay_seconds=10.0,
        jitter_ratio=0.0,
        reconcile_delay_seconds=7.0,
    )

    assert compute_backoff_delay_seconds(attempt_number=1, policy=policy) == 2.0
    assert compute_backoff_delay_seconds(attempt_number=2, policy=policy) == 4.0
    assert compute_backoff_delay_seconds(attempt_number=3, policy=policy) == 8.0
    assert compute_backoff_delay_seconds(attempt_number=4, policy=policy) == 10.0
    assert compute_backoff_delay_seconds(attempt_number=10, policy=policy) == 10.0


def test_backoff_applies_bounded_jitter_deterministically() -> None:
    policy = JobBackoffPolicy(
        base_delay_seconds=10.0,
        max_delay_seconds=100.0,
        jitter_ratio=0.2,
    )

    low = compute_backoff_delay_seconds(attempt_number=1, policy=policy, random_fn=lambda: 0.0)
    mid = compute_backoff_delay_seconds(attempt_number=1, policy=policy, random_fn=lambda: 0.5)
    high = compute_backoff_delay_seconds(attempt_number=1, policy=policy, random_fn=lambda: 1.0)

    assert low == pytest.approx(8.0)
    assert mid == pytest.approx(10.0)
    assert high == pytest.approx(12.0)


def test_resolve_job_transition_handles_retry_exhaustion_and_reconciling() -> None:
    now = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)
    policy = JobBackoffPolicy(
        base_delay_seconds=5.0,
        max_delay_seconds=60.0,
        jitter_ratio=0.0,
        reconcile_delay_seconds=12.0,
    )

    # Retryable while attempts remain -> retry_wait with backoff.
    retry_wait = resolve_job_transition(
        attempt_count=2,
        max_attempts=5,
        outcome=JobHandlerOutcome(
            outcome="retryable",
            error_code="memory_provider_unavailable",
            error_summary="provider temporarily unavailable",
        ),
        backoff_policy=policy,
        current_time=now,
    )
    assert retry_wait.job_status == "retry_wait"
    assert retry_wait.attempt_status == "retry_wait"
    assert retry_wait.finished_at is None
    assert retry_wait.next_run_at == now + timedelta(seconds=10.0)

    # Retryable when attempt_count reaches max_attempts -> terminal failed.
    exhausted = resolve_job_transition(
        attempt_count=5,
        max_attempts=5,
        outcome=JobHandlerOutcome(
            outcome="retryable",
            error_code="memory_provider_unavailable",
            error_summary="provider temporarily unavailable",
        ),
        backoff_policy=policy,
        current_time=now,
    )
    assert exhausted.job_status == "failed"
    assert exhausted.attempt_status == "failed"
    assert exhausted.finished_at == now
    assert exhausted.next_run_at is None

    # Reconciling never silently transitions to failed even at/beyond max_attempts.
    reconciling = resolve_job_transition(
        attempt_count=8,
        max_attempts=5,
        outcome=JobHandlerOutcome(
            outcome="reconciling",
            external_ref="ext_123",
            error_code="memory_provider_timeout",
            error_summary="response dropped; verification pending",
        ),
        backoff_policy=policy,
        current_time=now,
    )
    assert reconciling.job_status == "reconciling"
    assert reconciling.attempt_status == "reconciling"
    assert reconciling.finished_at is None
    assert reconciling.next_run_at == now + timedelta(seconds=12.0)

    # Succeeded & cancelled are terminal.
    succeeded = resolve_job_transition(
        attempt_count=1,
        max_attempts=5,
        outcome=JobHandlerOutcome(outcome="succeeded", external_ref="ext_123"),
        backoff_policy=policy,
        current_time=now,
    )
    assert succeeded.job_status == "succeeded"
    assert succeeded.finished_at == now
    assert succeeded.next_run_at is None

    cancelled = resolve_job_transition(
        attempt_count=1,
        max_attempts=5,
        outcome=JobHandlerOutcome(outcome="cancelled", error_code="memory_scope_superseded"),
        backoff_policy=policy,
        current_time=now,
    )
    assert cancelled.job_status == "cancelled"
    assert cancelled.finished_at == now
