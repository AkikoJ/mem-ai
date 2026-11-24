"""Unit tests for replaceable rate limiting, concurrency budgets, metrics, and tracing seam."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from mem_ai.core.exceptions import RateLimitError
from mem_ai.core.limits import (
    ConcurrencyLimiter,
    FixedWindowRateLimiter,
    NoopRateLimiter,
    RateLimitRequest,
)
from mem_ai.core.telemetry import (
    GaugeSample,
    InMemoryTracer,
    NoopTracer,
    OtelTracerAdapter,
    TelemetryRegistry,
    validate_metric_labels,
)


@pytest.mark.anyio
async def test_noop_and_fixed_window_rate_limiters_enforce_budget_and_retry_after() -> None:
    noop = NoopRateLimiter()
    req = RateLimitRequest(
        method="POST",
        path="/v1/chat",
        client_ip="127.0.0.1",
        key="127.0.0.1:/v1/chat",
    )
    decision = await noop.check(req)
    assert decision.allowed is True
    assert decision.scope == "ingress"

    now = [100.0]
    limiter = FixedWindowRateLimiter(
        max_requests=2,
        window_seconds=10.0,
        scope="app_test",
        clock=lambda: now[0],
    )
    first = await limiter.check(req)
    second = await limiter.check(req)
    third = await limiter.check(req)

    assert first.allowed is True
    assert first.remaining == 1
    assert second.allowed is True
    assert second.remaining == 0
    assert third.allowed is False
    assert third.remaining == 0
    assert third.scope == "app_test"
    assert third.retry_after_seconds == pytest.approx(10.0)

    # Advance past window -> budget resets cleanly.
    now[0] = 110.5
    fourth = await limiter.check(req)
    assert fourth.allowed is True
    assert fourth.remaining == 1


def test_concurrency_limiter_enforces_capacity_and_idempotent_release() -> None:
    limiter = ConcurrencyLimiter(max_in_flight=2, name="generations")
    assert limiter.active == 0

    permit1 = limiter.try_acquire()
    permit2 = limiter.try_acquire()
    assert permit1 is not None
    assert permit2 is not None
    assert limiter.active == 2
    assert limiter.try_acquire() is None

    with pytest.raises(RateLimitError) as exc_info:
        limiter.acquire_or_raise(
            code="generation_concurrency_limit_exceeded",
            message="too many active generations",
            retry_after_seconds=7,
        )
    assert exc_info.value.status_code == 429
    assert exc_info.value.code == "generation_concurrency_limit_exceeded"
    assert exc_info.value.headers.get("Retry-After") == "7"
    assert exc_info.value.details["retry_after_seconds"] == 7

    # Release is idempotent and restores capacity.
    permit1.release()
    permit1.release()
    assert limiter.active == 1

    with limiter.acquire_or_raise(retry_after_seconds=3):
        assert limiter.active == 2
    assert limiter.active == 1
    permit2.release()
    assert limiter.active == 0


def test_telemetry_registry_rejects_high_cardinality_business_id_labels() -> None:
    for forbidden in (
        "user_id",
        "conversation_id",
        "generation_id",
        "job_id",
        "operation_id",
        "request_id",
        "memory_id",
        "idempotency_key",
    ):
        with pytest.raises(ValueError, match="high-cardinality"):
            validate_metric_labels({forbidden: "some-id"})

    registry = TelemetryRegistry()
    with pytest.raises(ValueError, match="high-cardinality"):
        registry.inc_counter("mem_ai_test_total", labels={"user_id": "u-1"})


def test_telemetry_registry_records_and_renders_prometheus_exposition() -> None:
    registry = TelemetryRegistry()
    registry.record_http_request(
        method="POST",
        route="/v1/chat",
        status_code=200,
        duration_seconds=0.125,
    )
    registry.record_model_first_token(model_id="gpt-test", duration_seconds=0.04)
    registry.record_model_request(
        model_id="gpt-test",
        mode="stream",
        outcome="succeeded",
        duration_seconds=0.45,
    )
    registry.record_generation_outcome(mode="stream", outcome="succeeded")
    registry.record_generation_lease_event("worker_recovery")
    registry.record_memory_recall(
        provider="fake-memory",
        outcome="degraded",
        reason="memory_scope_clearing",
    )
    assert (
        registry.gauge_value("mem_ai_memory_provider_degraded", {"provider": "fake-memory"}) is None
    )

    registry.record_memory_recall(
        provider="fake-memory",
        outcome="degraded",
        reason="memory_provider_timeout",
    )
    assert (
        registry.gauge_value("mem_ai_memory_provider_degraded", {"provider": "fake-memory"}) == 1.0
    )
    registry.record_memory_barrier_duration(
        provider="fake-memory",
        outcome="succeeded",
        duration_seconds=1.5,
    )
    registry.record_worker_job(
        job_type="memory_turn_write",
        status="succeeded",
        duration_seconds=0.2,
    )
    registry.record_worker_heartbeat(
        active_tasks=1,
        last_heartbeat_ts=1700000010.0,
        last_claim_ts=1700000005.0,
        last_success_ts=1700000008.0,
    )

    text = registry.render_prometheus(
        extra_gauges=(
            GaugeSample(
                name="mem_ai_jobs_backlog",
                value=3.0,
                help_text="Number of pending or retry-waiting durable jobs.",
            ),
            GaugeSample(
                name="mem_ai_jobs_oldest_wait_seconds",
                value=42.5,
                help_text="Age in seconds of the oldest waiting or reconciling job.",
            ),
        )
    )

    assert 'mem_ai_http_requests_total{method="POST",route="/v1/chat",status_code="200"} 1' in text
    assert 'mem_ai_http_request_duration_seconds_count{method="POST",route="/v1/chat"} 1' in text
    assert 'mem_ai_model_first_token_duration_seconds_count{model_id="gpt-test"} 1' in text
    assert (
        'mem_ai_model_request_duration_seconds_count{mode="stream",model_id="gpt-test",'
        'outcome="succeeded"} 1'
    ) in text
    assert (
        'mem_ai_generations_total{error_code="none",mode="stream",outcome="succeeded"} 1'
    ) in text
    assert 'mem_ai_generation_lease_events_total{event="worker_recovery"} 1' in text
    assert (
        'mem_ai_memory_degradations_total{provider="fake-memory",'
        'reason="memory_provider_timeout"} 1'
    ) in text
    assert 'mem_ai_memory_provider_degraded{provider="fake-memory"} 1' in text
    assert (
        'mem_ai_memory_barrier_duration_seconds_count{outcome="succeeded",provider="fake-memory"} 1'
    ) in text
    assert (
        'mem_ai_worker_jobs_processed_total{job_type="memory_turn_write",status="succeeded"} 1'
    ) in text
    assert "mem_ai_jobs_backlog 3" in text
    assert "mem_ai_jobs_oldest_wait_seconds 42.5" in text
    assert "mem_ai_worker_last_heartbeat_timestamp_seconds 1700000010" in text


def test_tracing_seam_supports_noop_inmemory_and_otel_adapter_with_redaction() -> None:
    noop = NoopTracer()
    with noop.start_span("noop.op", attributes={"safe": "value"}) as span:
        span.set_attribute("k", "v")
        span.set_status("ok")

    mem_tracer = InMemoryTracer()
    with mem_tracer.start_span(
        "chat.generate",
        attributes={"model_id": "gpt-test", "authorization": "Bearer super-secret"},
    ) as span:
        span.set_attribute("content", "private user prompt")
        span.set_attribute("input_tokens", 12)
        span.record_exception(RuntimeError("boom"))
        span.set_status("error", "boom")

    assert len(mem_tracer.spans) == 1
    recorded = mem_tracer.spans[0]
    assert recorded.name == "chat.generate"
    assert recorded.attributes["model_id"] == "gpt-test"
    assert recorded.attributes["authorization"] == "[REDACTED]"
    assert recorded.attributes["content"] == "[REDACTED]"
    assert recorded.attributes["input_tokens"] == 12
    assert recorded.status == "error"
    assert recorded.exception_type == "RuntimeError"

    # OpenTelemetry duck-typed tracer adapter
    otel_events: list[tuple[str, dict[str, Any]]] = []

    class _FakeOtelSpan:
        def __init__(self, name: str) -> None:
            self.name = name
            self.attrs: dict[str, Any] = {}

        def set_attribute(self, key: str, value: Any) -> None:
            self.attrs[key] = value

        def record_exception(self, exc: BaseException) -> None:
            self.attrs["exc"] = type(exc).__name__

        def set_status(self, status: Any, description: str | None = None) -> None:
            self.attrs["status"] = (status, description)

    class _FakeOtelTracer:
        @contextmanager
        def start_as_current_span(self, name: str) -> Iterator[_FakeOtelSpan]:
            s = _FakeOtelSpan(name)
            try:
                yield s
            finally:
                otel_events.append((name, s.attrs))

    adapter = OtelTracerAdapter(_FakeOtelTracer())
    with adapter.start_span("otel.op", attributes={"api_key": "sk-secret", "route": "/v1/chat"}):
        pass
    assert otel_events == [("otel.op", {"api_key": "[REDACTED]", "route": "/v1/chat"})]
