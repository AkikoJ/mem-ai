"""Shared HTTPX async client builder with explicit timeouts and connection limits."""

from __future__ import annotations

import httpx

from mem_ai.core.config import Settings


def create_http_client(settings: Settings) -> httpx.AsyncClient:
    """Create a pooled asynchronous HTTPX client governed by application settings."""
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_seconds,
        read=settings.http_read_timeout_seconds,
        write=settings.http_write_timeout_seconds,
        pool=settings.http_pool_timeout_seconds,
    )
    limits = httpx.Limits(
        max_connections=settings.http_max_connections,
        max_keepalive_connections=settings.http_max_keepalive_connections,
    )
    return httpx.AsyncClient(timeout=timeout, limits=limits)
