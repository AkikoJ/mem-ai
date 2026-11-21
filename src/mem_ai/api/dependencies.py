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

_ProviderT = TypeVar("_ProviderT")


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


def get_persona_provider(request: Request) -> PersonaProvider:
    """Return the configured PersonaProvider (overridable via `app.dependency_overrides`)."""
    registry = cast(ProviderRegistry, request.app.state.providers)
    return _require_provider(registry.persona_provider, name="persona")


SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionFactoryDep = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]
HttpClientDep = Annotated[httpx.AsyncClient, Depends(get_http_client)]
ModelProviderDep = Annotated[ModelProvider, Depends(get_model_provider)]
MemoryProviderDep = Annotated[MemoryProvider, Depends(get_memory_provider)]
PersonaProviderDep = Annotated[PersonaProvider, Depends(get_persona_provider)]
