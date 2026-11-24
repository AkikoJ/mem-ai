"""Low-cardinality metrics registry, Prometheus exposition, and OpenTelemetry tracing seam."""

from __future__ import annotations

import math
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from mem_ai.core.logging import sanitize_log_value

GenerationMode = Literal["sync", "stream"]
GenerationOutcome = Literal["succeeded", "failed", "cancelled", "timeout"]
ModelOutcome = Literal["succeeded", "error", "cancelled", "timeout"]
LeaseEvent = Literal["inline_takeover", "worker_recovery", "lease_lost", "renew_failed"]
RecallOutcome = Literal["ok", "degraded", "skipped"]
SpanStatus = Literal["unset", "ok", "error"]

# Business identifiers belong strictly in structured logs and traces, never in metric labels.
FORBIDDEN_METRIC_LABEL_KEYS: frozenset[str] = frozenset(
    {
        "actor_id",
        "agent_id",
        "client_ip",
        "conversation_id",
        "generation_id",
        "idempotency_key",
        "job_id",
        "memory_id",
        "message_id",
        "operation_id",
        "receipt",
        "request_id",
        "secret",
        "subject",
        "token",
        "user_id",
    }
)

DEFAULT_LATENCY_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
)

LabelTuple = tuple[tuple[str, str], ...]


def validate_metric_labels(labels: Mapping[str, str] | None) -> LabelTuple:
    """Validate that metric labels stay low-cardinality and contain no business IDs."""
    if not labels:
        return ()
    normalized: list[tuple[str, str]] = []
    for raw_key, raw_value in labels.items():
        key = str(raw_key).strip()
        if key.lower() in FORBIDDEN_METRIC_LABEL_KEYS:
            raise ValueError(
                f"Metric label {key!r} is forbidden as a high-cardinality business identifier"
            )
        normalized.append((key, str(raw_value)))
    normalized.sort(key=lambda item: item[0])
    return tuple(normalized)


def _escape_prom_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_prom_labels(labels: LabelTuple, extra: tuple[str, str] | None = None) -> str:
    items = list(labels)
    if extra is not None:
        items.append(extra)
    if not items:
        return ""
    encoded = ",".join(f'{k}="{_escape_prom_label(v)}"' for k, v in items)
    return "{" + encoded + "}"


def _format_prom_number(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


@dataclass(frozen=True, slots=True)
class GaugeSample:
    """Single point-in-time gauge sample injected during metrics exposition."""

    name: str
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)
    help_text: str = ""


@dataclass(frozen=True, slots=True)
class HistogramSnapshot:
    """Immutable view of a histogram series."""

    buckets: tuple[tuple[float, int], ...]
    count: int
    sum: float


@dataclass(slots=True)
class _HistogramState:
    bounds: tuple[float, ...]
    counts: list[int]
    total_count: int = 0
    total_sum: float = 0.0

    def observe(self, value: float) -> None:
        observed = max(0.0, float(value))
        self.total_count += 1
        self.total_sum += observed
        for idx, upper in enumerate(self.bounds):
            if observed <= upper:
                self.counts[idx] += 1

    def snapshot(self) -> HistogramSnapshot:
        return HistogramSnapshot(
            buckets=tuple(zip(self.bounds, self.counts, strict=True)),
            count=self.total_count,
            sum=self.total_sum,
        )


class TelemetryRegistry:
    """Thread-safe low-cardinality metrics registry with Prometheus text exposition."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._help: dict[str, str] = {}
        self._types: dict[str, str] = {}
        self._counters: dict[tuple[str, LabelTuple], float] = {}
        self._gauges: dict[tuple[str, LabelTuple], float] = {}
        self._histograms: dict[tuple[str, LabelTuple], _HistogramState] = {}

    def _register_meta_locked(self, name: str, metric_type: str, help_text: str | None) -> None:
        self._types.setdefault(name, metric_type)
        if help_text and name not in self._help:
            self._help[name] = help_text

    def inc_counter(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: Mapping[str, str] | None = None,
        help_text: str | None = None,
    ) -> None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            self._register_meta_locked(name, "counter", help_text)
            key = (name, label_tuple)
            self._counters[key] = self._counters.get(key, 0.0) + float(amount)

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
        help_text: str | None = None,
    ) -> None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            self._register_meta_locked(name, "gauge", help_text)
            self._gauges[(name, label_tuple)] = float(value)

    def inc_gauge(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: Mapping[str, str] | None = None,
        help_text: str | None = None,
    ) -> None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            self._register_meta_locked(name, "gauge", help_text)
            key = (name, label_tuple)
            self._gauges[key] = self._gauges.get(key, 0.0) + float(amount)

    def dec_gauge(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: Mapping[str, str] | None = None,
        help_text: str | None = None,
    ) -> None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            self._register_meta_locked(name, "gauge", help_text)
            key = (name, label_tuple)
            self._gauges[key] = max(0.0, self._gauges.get(key, 0.0) - float(amount))

    def observe_histogram(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
        buckets: tuple[float, ...] = DEFAULT_LATENCY_BUCKETS,
        help_text: str | None = None,
    ) -> None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            self._register_meta_locked(name, "histogram", help_text)
            key = (name, label_tuple)
            state = self._histograms.get(key)
            if state is None:
                state = _HistogramState(bounds=buckets, counts=[0] * len(buckets))
                self._histograms[key] = state
            state.observe(value)

    def counter_value(self, name: str, labels: Mapping[str, str] | None = None) -> float:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            return self._counters.get((name, label_tuple), 0.0)

    def gauge_value(self, name: str, labels: Mapping[str, str] | None = None) -> float | None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            return self._gauges.get((name, label_tuple))

    def histogram_snapshot(
        self,
        name: str,
        labels: Mapping[str, str] | None = None,
    ) -> HistogramSnapshot | None:
        label_tuple = validate_metric_labels(labels)
        with self._lock:
            state = self._histograms.get((name, label_tuple))
            return state.snapshot() if state is not None else None

    # ---- Standardized domain observation helpers (low-cardinality labels only) ----

    def record_http_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        self.inc_counter(
            "mem_ai_http_requests_total",
            labels={"method": method.upper(), "route": route, "status_code": str(status_code)},
            help_text="Total HTTP requests served by method, canonical route, and status code.",
        )
        self.observe_histogram(
            "mem_ai_http_request_duration_seconds",
            duration_seconds,
            labels={"method": method.upper(), "route": route},
            help_text="HTTP request latency in seconds by method and canonical route.",
        )

    def record_rate_limit_rejection(self, *, scope: str, reason: str) -> None:
        self.inc_counter(
            "mem_ai_rate_limit_rejections_total",
            labels={"scope": scope, "reason": reason},
            help_text="Total requests rejected by rate, concurrency, or payload budgets.",
        )

    def record_model_request(
        self,
        *,
        model_id: str,
        mode: GenerationMode,
        outcome: ModelOutcome,
        duration_seconds: float,
    ) -> None:
        labels = {"model_id": model_id, "mode": mode, "outcome": outcome}
        self.inc_counter(
            "mem_ai_model_requests_total",
            labels=labels,
            help_text="Total upstream model invocations by model, mode, and outcome.",
        )
        self.observe_histogram(
            "mem_ai_model_request_duration_seconds",
            duration_seconds,
            labels=labels,
            help_text="Total upstream model invocation duration in seconds.",
        )

    def record_model_first_token(self, *, model_id: str, duration_seconds: float) -> None:
        self.observe_histogram(
            "mem_ai_model_first_token_duration_seconds",
            duration_seconds,
            labels={"model_id": model_id},
            help_text="Streaming time-to-first-token latency in seconds.",
        )

    def record_generation_outcome(
        self,
        *,
        mode: GenerationMode,
        outcome: GenerationOutcome,
        error_code: str = "none",
    ) -> None:
        self.inc_counter(
            "mem_ai_generations_total",
            labels={"mode": mode, "outcome": outcome, "error_code": error_code or "none"},
            help_text="Total generations reaching terminal outcomes.",
        )

    def record_generation_lease_event(self, event: LeaseEvent) -> None:
        self.inc_counter(
            "mem_ai_generation_lease_events_total",
            labels={"event": event},
            help_text="Generation lease lifecycle events (takeover, recovery, loss, renew error).",
        )

    def record_memory_recall(
        self,
        *,
        provider: str,
        outcome: RecallOutcome,
        reason: str = "none",
    ) -> None:
        self.inc_counter(
            "mem_ai_memory_recall_total",
            labels={"provider": provider, "outcome": outcome, "reason": reason or "none"},
            help_text="Total scoped memory recall executions by provider and outcome.",
        )
        if outcome == "degraded":
            self.inc_counter(
                "mem_ai_memory_degradations_total",
                labels={"provider": provider, "reason": reason or "unknown"},
                help_text="Total memory recall degradations by provider and reason.",
            )
            # Intentional per-user clearing barriers must not trip the global provider outage gauge.
            if reason != "memory_scope_clearing":
                self.set_gauge(
                    "mem_ai_memory_provider_degraded",
                    1.0,
                    labels={"provider": provider},
                    help_text=(
                        "Indicates whether recent memory provider recall degraded (1) or ok (0)."
                    ),
                )
        elif outcome == "ok":
            self.set_gauge(
                "mem_ai_memory_provider_degraded",
                0.0,
                labels={"provider": provider},
                help_text="Indicates whether recent memory provider recall degraded (1) or ok (0).",
            )

    def record_memory_barrier_duration(
        self,
        *,
        provider: str,
        outcome: str,
        duration_seconds: float,
    ) -> None:
        self.observe_histogram(
            "mem_ai_memory_barrier_duration_seconds",
            duration_seconds,
            labels={"provider": provider, "outcome": outcome},
            help_text="Duration of memory clearing barriers from installation to convergence.",
        )

    def record_worker_job(
        self,
        *,
        job_type: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        labels = {"job_type": job_type, "status": status}
        self.inc_counter(
            "mem_ai_worker_jobs_processed_total",
            labels=labels,
            help_text="Total durable jobs processed by the Worker by type and resulting status.",
        )
        self.observe_histogram(
            "mem_ai_worker_job_duration_seconds",
            duration_seconds,
            labels=labels,
            help_text="Worker job attempt execution duration in seconds.",
        )

    def record_worker_heartbeat(
        self,
        *,
        active_tasks: int,
        last_heartbeat_ts: float,
        last_claim_ts: float | None,
        last_success_ts: float | None,
    ) -> None:
        self.set_gauge(
            "mem_ai_worker_active_tasks",
            float(active_tasks),
            help_text="Current number of active job tasks in this Worker process.",
        )
        self.set_gauge(
            "mem_ai_worker_last_heartbeat_timestamp_seconds",
            float(last_heartbeat_ts),
            help_text="Unix timestamp of the latest Worker loop heartbeat.",
        )
        self.set_gauge(
            "mem_ai_worker_last_claim_timestamp_seconds",
            float(last_claim_ts) if last_claim_ts is not None else 0.0,
            help_text="Unix timestamp of the latest successful Worker job claim.",
        )
        self.set_gauge(
            "mem_ai_worker_last_success_timestamp_seconds",
            float(last_success_ts) if last_success_ts is not None else 0.0,
            help_text="Unix timestamp of the latest Worker job completion in succeeded status.",
        )

    def render_prometheus(self, extra_gauges: Sequence[GaugeSample] = ()) -> str:
        """Render all recorded metrics plus point-in-time DB gauges in Prometheus text format."""
        with self._lock:
            counters_copy = dict(self._counters)
            gauges_copy = dict(self._gauges)
            histograms_copy = {k: v.snapshot() for k, v in self._histograms.items()}
            help_copy = dict(self._help)
            types_copy = dict(self._types)

        for sample in extra_gauges:
            label_tuple = validate_metric_labels(sample.labels)
            gauges_copy[(sample.name, label_tuple)] = float(sample.value)
            types_copy.setdefault(sample.name, "gauge")
            if sample.help_text and sample.name not in help_copy:
                help_copy[sample.name] = sample.help_text

        metric_names = sorted(
            {name for name, _ in counters_copy}
            | {name for name, _ in gauges_copy}
            | {name for name, _ in histograms_copy}
        )
        lines: list[str] = []
        for name in metric_names:
            metric_type = types_copy.get(name, "gauge")
            help_text = help_copy.get(name)
            if help_text:
                lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")

            if metric_type == "counter":
                series = sorted(
                    ((lbls, val) for (n, lbls), val in counters_copy.items() if n == name),
                    key=lambda item: item[0],
                )
                for lbls, val in series:
                    lines.append(f"{name}{_format_prom_labels(lbls)} {_format_prom_number(val)}")
            elif metric_type == "gauge":
                series = sorted(
                    ((lbls, val) for (n, lbls), val in gauges_copy.items() if n == name),
                    key=lambda item: item[0],
                )
                for lbls, val in series:
                    lines.append(f"{name}{_format_prom_labels(lbls)} {_format_prom_number(val)}")
            elif metric_type == "histogram":
                hist_series = sorted(
                    ((lbls, snap) for (n, lbls), snap in histograms_copy.items() if n == name),
                    key=lambda item: item[0],
                )
                for lbls, snap in hist_series:
                    for upper, count in snap.buckets:
                        lbl_str = _format_prom_labels(
                            lbls, extra=("le", _format_prom_number(upper))
                        )
                        lines.append(f"{name}_bucket{lbl_str} {count}")
                    inf_str = _format_prom_labels(lbls, extra=("le", "+Inf"))
                    lines.append(f"{name}_bucket{inf_str} {snap.count}")
                    lines.append(
                        f"{name}_sum{_format_prom_labels(lbls)} {_format_prom_number(snap.sum)}"
                    )
                    lines.append(f"{name}_count{_format_prom_labels(lbls)} {snap.count}")

        return "\n".join(lines) + ("\n" if lines else "")


# ---- OpenTelemetry-compatible tracing seam (zero mandatory SDK dependency) ----


@runtime_checkable
class Span(Protocol):
    """Minimal span interface matching OpenTelemetry's core span contract."""

    def set_attribute(self, key: str, value: Any) -> None: ...

    def record_exception(self, exc: BaseException) -> None: ...

    def set_status(self, status: SpanStatus, description: str | None = None) -> None: ...


@runtime_checkable
class Tracer(Protocol):
    """Replaceable tracing seam supporting no-op, in-memory tests, and OpenTelemetry SDKs."""

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, Any] | None = None,
    ) -> AbstractContextManager[Span]: ...


class NoopSpan:
    """Zero-overhead span implementation used when tracing export is not configured."""

    def set_attribute(self, key: str, value: Any) -> None:
        _ = (key, value)

    def record_exception(self, exc: BaseException) -> None:
        _ = exc

    def set_status(self, status: SpanStatus, description: str | None = None) -> None:
        _ = (status, description)


class NoopTracer:
    """Default no-op tracer reserving the OpenTelemetry integration seam."""

    _SPAN = NoopSpan()

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, Any] | None = None,
    ) -> Iterator[Span]:
        _ = (name, attributes)
        yield self._SPAN


@dataclass
class RecordedSpan:
    """Captured span record for deterministic tracing tests."""

    name: str
    attributes: dict[str, Any] = field(default_factory=dict)
    status: SpanStatus = "unset"
    status_description: str | None = None
    exception_type: str | None = None

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[str(key)] = sanitize_log_value(str(key), value)

    def record_exception(self, exc: BaseException) -> None:
        self.exception_type = type(exc).__name__

    def set_status(self, status: SpanStatus, description: str | None = None) -> None:
        self.status = status
        self.status_description = description


class InMemoryTracer:
    """In-memory tracer recording sanitized spans for unit, API, and integration verification."""

    def __init__(self) -> None:
        self.spans: list[RecordedSpan] = []

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, Any] | None = None,
    ) -> Iterator[Span]:
        span = RecordedSpan(name=name)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        try:
            yield span
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status("error", type(exc).__name__)
            raise
        finally:
            self.spans.append(span)


class _OtelSpanWrapper:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def set_attribute(self, key: str, value: Any) -> None:
        self._inner.set_attribute(str(key), sanitize_log_value(str(key), value))

    def record_exception(self, exc: BaseException) -> None:
        fn = getattr(self._inner, "record_exception", None)
        if callable(fn):
            fn(exc)

    def set_status(self, status: SpanStatus, description: str | None = None) -> None:
        fn = getattr(self._inner, "set_status", None)
        if callable(fn):
            fn(status, description)


class OtelTracerAdapter:
    """Bridge adapting any OpenTelemetry `Tracer` (`start_as_current_span`) to Mem-AI's seam."""

    def __init__(self, otel_tracer: Any) -> None:
        self._otel_tracer = otel_tracer

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, Any] | None = None,
    ) -> Iterator[Span]:
        with self._otel_tracer.start_as_current_span(name) as raw_span:
            wrapped = _OtelSpanWrapper(raw_span)
            if attributes:
                for key, value in attributes.items():
                    wrapped.set_attribute(key, value)
            yield wrapped


# ---- Per-app / per-worker ContextVar bindings (zero global cross-test leakage) ----

_DEFAULT_TELEMETRY = TelemetryRegistry()
_DEFAULT_TRACER: Tracer = NoopTracer()
_CURRENT_TELEMETRY: ContextVar[TelemetryRegistry | None] = ContextVar(
    "mem_ai_current_telemetry", default=None
)
_CURRENT_TRACER: ContextVar[Tracer | None] = ContextVar("mem_ai_current_tracer", default=None)


def current_telemetry() -> TelemetryRegistry:
    """Return the active `TelemetryRegistry` bound to this app/worker context."""
    bound = _CURRENT_TELEMETRY.get()
    return bound if bound is not None else _DEFAULT_TELEMETRY


def current_tracer() -> Tracer:
    """Return the active `Tracer` bound to this app/worker context."""
    bound = _CURRENT_TRACER.get()
    return bound if bound is not None else _DEFAULT_TRACER


def bind_telemetry(
    *,
    telemetry: TelemetryRegistry | None = None,
    tracer: Tracer | None = None,
) -> tuple[Token[TelemetryRegistry | None] | None, Token[Tracer | None] | None]:
    """Bind per-app or per-worker telemetry registry and tracer into the current async context."""
    tel_token = _CURRENT_TELEMETRY.set(telemetry) if telemetry is not None else None
    tracer_token = _CURRENT_TRACER.set(tracer) if tracer is not None else None
    return tel_token, tracer_token


def reset_telemetry(
    tokens: tuple[Token[TelemetryRegistry | None] | None, Token[Tracer | None] | None],
) -> None:
    """Restore previous telemetry registry and tracer bindings."""
    tel_token, tracer_token = tokens
    if tel_token is not None:
        _CURRENT_TELEMETRY.reset(tel_token)
    if tracer_token is not None:
        _CURRENT_TRACER.reset(tracer_token)
