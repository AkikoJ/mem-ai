"""SQLAlchemy async/sync engine builders and session factory."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from mem_ai.core.config import Settings


def _connect_args(settings: Settings) -> dict[str, object]:
    return {
        "connect_timeout": max(1, int(settings.db_connect_timeout_seconds)),
        "options": f"-c timezone=UTC -c statement_timeout={settings.db_statement_timeout_ms}",
    }


def create_async_engine_from_settings(settings: Settings) -> AsyncEngine:
    """Create the runtime asynchronous PostgreSQL engine with bounded pool settings."""
    return create_async_engine(
        settings.database_url(),
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        connect_args=_connect_args(settings),
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create the runtime async session maker with `expire_on_commit=False`."""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


def create_sync_engine_from_settings(settings: Settings) -> Engine:
    """Create a synchronous psycopg 3 engine for Alembic migrations and test setup."""
    return create_engine(
        settings.database_url(),
        pool_pre_ping=True,
        connect_args=_connect_args(settings),
    )
