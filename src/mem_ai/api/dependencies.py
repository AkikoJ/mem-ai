"""FastAPI dependency accessors for settings, DB sessions, HTTP client, and providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, TypeVar, cast

import httpx
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.contracts.model_provider import ModelProvider
from mem_ai.contracts.persona_provider import PersonaProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ServiceUnavailableError
from mem_ai.core.limits import ConcurrencyLimiter
from mem_ai.core.telemetry import TelemetryRegistry
from mem_ai.modules.operations.audit import DeletionAuditArchiver, NoopDeletionAuditArchiver

_ProviderT = TypeVar("_ProviderT")
_DEFAULT_DELETION_AUDIT_ARCHIVER = NoopDeletionAuditArchiver()


def get_deletion_audit_archiver(request: Request) -> DeletionAuditArchiver:
    """Return the assembled independent deletion audit sink shared by resource APIs."""
    configured = getattr(request.app.state, "deletion_audit_archiver", None)
    if isinstance(configured, DeletionAuditArchiver):
        return configured
    return _DEFAULT_DELETION_AUDIT_ARCHIVER


def _require_provider(provider: _ProviderT | None, *, name: str) -> _ProviderT:  # noqa: UP047
    if provider is None:
        raise ServiceUnavailableError(
            code="provider_not_configured",
            message=f"{name} provider is not configured",
        )
    return provider


@dataclass(slots=True)
class ProviderRegistry:
    """Typed container of application-assembled provider implementations."""

    model_provider: ModelProvider | None = None
    memory_provider: MemoryProvider | None = None
    persona_provider: PersonaProvider | None = None


def get_settings(request: Request) -> Settings:
    """Return validated application settings from application state."""
    return cast(Settings, request.app.state.settings)


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """Return the runtime SQLAlchemy async session factory."""
    return cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the shared outbound HTTPX async client."""
    return cast(httpx.AsyncClient, request.app.state.http_client)


def get_model_provider(request: Request) -> ModelProvider:
    """Return the configured ModelProvider (overridable via `app.dependency_overrides`)."""
    registry = cast(ProviderRegistry, request.app.state.providers)
    return _require_provider(registry.model_provider, name="model")


def get_memory_provider(request: Request) -> MemoryProvider:
    """Return the configured MemoryProvider (overridable via `app.dependency_overrides`)."""
    registry = cast(ProviderRegistry, request.app.state.providers)
    return _require_provider(registry.memory_provider, name="memory")


def get_optional_memory_provider(request: Request) -> MemoryProvider | None:
    """Return the configured MemoryProvider or None when memory is disabled for chat."""
    registry = cast(ProviderRegistry, request.app.state.providers)
    return registry.memory_provider


def get_persona_provider(request: Request) -> PersonaProvider:
    """Return the configured PersonaProvider (overridable via `app.dependency_overrides`)."""
    registry = cast(ProviderRegistry, request.app.state.providers)
    return _require_provider(registry.persona_provider, name="persona")


def get_telemetry(request: Request) -> TelemetryRegistry:
    """Return the application's low-cardinality `TelemetryRegistry`."""
    configured = getattr(request.app.state, "telemetry", None)
    if isinstance(configured, TelemetryRegistry):
        return configured
    created = TelemetryRegistry()
    request.app.state.telemetry = created
    return created


def get_optional_session_factory(request: Request) -> async_sessionmaker[AsyncSession] | None:
    """Return the runtime async session factory if initialized, else None."""
    return cast(
        async_sessionmaker[AsyncSession] | None,
        getattr(request.app.state, "session_factory", None),
    )


def get_app_ready(request: Request) -> bool:
    """Return True when application startup initialization has completed and is not draining."""
    return bool(getattr(request.app.state, "ready", True))


def get_generation_concurrency_limiter(request: Request) -> ConcurrencyLimiter | None:
    """Return the process-level generation concurrency budget limiter, if configured."""
    configured = getattr(request.app.state, "generation_concurrency_limiter", None)
    if isinstance(configured, ConcurrencyLimiter):
        return configured
    return None


SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionFactoryDep = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]
OptionalSessionFactoryDep = Annotated[
    async_sessionmaker[AsyncSession] | None, Depends(get_optional_session_factory)
]
AppReadyDep = Annotated[bool, Depends(get_app_ready)]
HttpClientDep = Annotated[httpx.AsyncClient, Depends(get_http_client)]
ModelProviderDep = Annotated[ModelProvider, Depends(get_model_provider)]
MemoryProviderDep = Annotated[MemoryProvider, Depends(get_memory_provider)]
OptionalMemoryProviderDep = Annotated[MemoryProvider | None, Depends(get_optional_memory_provider)]
PersonaProviderDep = Annotated[PersonaProvider, Depends(get_persona_provider)]
DeletionAuditArchiverDep = Annotated[DeletionAuditArchiver, Depends(get_deletion_audit_archiver)]
TelemetryDep = Annotated[TelemetryRegistry, Depends(get_telemetry)]
GenerationLimiterDep = Annotated[
    ConcurrencyLimiter | None, Depends(get_generation_concurrency_limiter)
]
