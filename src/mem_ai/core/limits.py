"""Replaceable rate limiting boundary and process-level concurrency budgets.

Ingress / API gateway owns distributed rate limiting in V1 deployments; this module
provides the replaceable `RateLimiter` protocol (defaulting to `NoopRateLimiter`), a
bounded in-process `FixedWindowRateLimiter` for tests or single-node budgets, and a
non-blocking `ConcurrencyLimiter` protecting in-process HTTP and generation budgets.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol, runtime_checkable

from mem_ai.core.exceptions import RateLimitError


def compute_retry_after_seconds(
    value: int | float | None,
    *,
    default: int = 5,
) -> int:
    """Compute a positive integer `Retry-After` hint in seconds."""
    raw = float(value) if value is not None else float(default)
    return max(1, math.ceil(raw))


@dataclass(frozen=True, slots=True)
class RateLimitRequest:
    """Normalized request descriptor passed to a replaceable `RateLimiter`."""

    method: str
    path: str
    client_ip: str | None
    key: str


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Outcome of a rate-limit evaluation with optional retry guidance."""

    allowed: bool
    limit: int | None = None
    remaining: int | None = None
    retry_after_seconds: float | None = None
    scope: str = "app"


@runtime_checkable
class RateLimiter(Protocol):
    """Replaceable rate-limiting seam (ingress-delegated, Redis, or in-process)."""

    async def check(self, request: RateLimitRequest) -> RateLimitDecision:
        """Evaluate whether `request` is within the configured rate budget."""
        ...


class NoopRateLimiter:
    """Default pass-through limiter when distributed rate limiting lives at the ingress gateway."""

    async def check(self, request: RateLimitRequest) -> RateLimitDecision:
        _ = request
        return RateLimitDecision(allowed=True, scope="ingress")


class FixedWindowRateLimiter:
    """Bounded in-process fixed-window rate limiter for single-node limits and tests."""

    def __init__(
        self,
        *,
        max_requests: int,
        window_seconds: float,
        scope: str = "app",
        max_keys: int = 10_000,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be at least 1")
        if window_seconds <= 0.0:
            raise ValueError("window_seconds must be positive")
        if max_keys < 1:
            raise ValueError("max_keys must be at least 1")
        self._max_requests = max_requests
        self._window_seconds = float(window_seconds)
        self._scope = scope
        self._max_keys = max_keys
        self._clock = clock if clock is not None else time.monotonic
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, int]] = {}

    async def check(self, request: RateLimitRequest) -> RateLimitDecision:
        now = float(self._clock())
        with self._lock:
            entry = self._buckets.get(request.key)
            if entry is None or (now - entry[0]) >= self._window_seconds:
                if len(self._buckets) >= self._max_keys and request.key not in self._buckets:
                    self._evict_locked(now)
                self._buckets[request.key] = (now, 1)
                return RateLimitDecision(
                    allowed=True,
                    limit=self._max_requests,
                    remaining=self._max_requests - 1,
                    scope=self._scope,
                )

            window_start, count = entry
            if count >= self._max_requests:
                retry_after = max(0.001, (window_start + self._window_seconds) - now)
                return RateLimitDecision(
                    allowed=False,
                    limit=self._max_requests,
                    remaining=0,
                    retry_after_seconds=retry_after,
                    scope=self._scope,
                )

            new_count = count + 1
            self._buckets[request.key] = (window_start, new_count)
            return RateLimitDecision(
                allowed=True,
                limit=self._max_requests,
                remaining=self._max_requests - new_count,
                scope=self._scope,
            )

    def _evict_locked(self, now: float) -> None:
        expired = [
            key
            for key, (window_start, _) in self._buckets.items()
            if (now - window_start) >= self._window_seconds
        ]
        for key in expired:
            del self._buckets[key]
        if len(self._buckets) >= self._max_keys:
            oldest_key = min(self._buckets, key=lambda key: self._buckets[key][0])
            del self._buckets[oldest_key]


class ConcurrencyPermit:
    """Idempotent lease handle returned by `ConcurrencyLimiter`."""

    def __init__(self, limiter: ConcurrencyLimiter) -> None:
        self._limiter = limiter
        self._released = False

    def release(self) -> None:
        """Release the concurrency slot at most once."""
        if not self._released:
            self._released = True
            self._limiter._release_one()

    def __enter__(self) -> ConcurrencyPermit:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)
        self.release()


class ConcurrencyLimiter:
    """Non-blocking process-level concurrency budget for HTTP requests or generations."""

    def __init__(self, *, max_in_flight: int, name: str = "concurrency") -> None:
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1")
        self._max_in_flight = max_in_flight
        self._name = name
        self._active = 0
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def max_in_flight(self) -> int:
        return self._max_in_flight

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    def try_acquire(self) -> ConcurrencyPermit | None:
        """Atomically acquire a concurrency slot or return None when saturated."""
        with self._lock:
            if self._active >= self._max_in_flight:
                return None
            self._active += 1
        return ConcurrencyPermit(self)

    def acquire_or_raise(
        self,
        *,
        code: str = "rate_limit_exceeded",
        message: str = "process concurrency budget exceeded",
        retry_after_seconds: int | float = 5,
    ) -> ConcurrencyPermit:
        """Acquire a concurrency slot or raise a 429 `RateLimitError` with Retry-After."""
        permit = self.try_acquire()
        if permit is None:
            raise RateLimitError(
                code=code,
                message=message,
                retry_after_seconds=retry_after_seconds,
                details={
                    "limit_scope": self._name,
                    "max_in_flight": self._max_in_flight,
                },
            )
        return permit

    def _release_one(self) -> None:
        with self._lock:
            if self._active > 0:
                self._active -= 1
