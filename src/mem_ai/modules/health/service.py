"""Bounded database connectivity and Alembic migration readiness checks."""

from __future__ import annotations

import asyncio
from functools import lru_cache
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ServiceUnavailableError


class LivenessStatus(BaseModel):
    """Response payload for `/health/live`."""

    status: str = "ok"


class ReadinessStatus(BaseModel):
    """Response payload for `/health/ready`."""

    status: str = "ready"
    database: str = "ok"
    migration_revision: str


@lru_cache(maxsize=1)
def _discover_alembic_head() -> str | None:
    repo_root = Path(__file__).resolve().parents[4]
    alembic_ini = repo_root / "alembic.ini"
    migrations_dir = repo_root / "migrations"
    if alembic_ini.exists():
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
    elif migrations_dir.exists():
        cfg = Config()
        cfg.set_main_option("script_location", str(migrations_dir))
    else:
        return None
    script = ScriptDirectory.from_config(cfg)
    return script.get_current_head()


def resolve_expected_migration_revision(settings: Settings) -> str | None:
    """Return the required migration revision from settings or local Alembic scripts."""
    if settings.expected_alembic_revision:
        return settings.expected_alembic_revision
    return _discover_alembic_head()


async def check_readiness(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> ReadinessStatus:
    """Verify PostgreSQL connectivity and applied Alembic revision within a bounded timeout."""
    expected_revision = resolve_expected_migration_revision(settings)

    try:
        async with asyncio.timeout(settings.db_health_check_timeout_seconds):
            async with session_factory() as session:
                await session.execute(text("SELECT 1"))
                has_alembic_table = (
                    await session.execute(text("SELECT to_regclass('public.alembic_version')"))
                ).scalar_one_or_none()
                if has_alembic_table is None:
                    version_row = None
                else:
                    version_row = (
                        await session.execute(
                            text("SELECT version_num FROM public.alembic_version LIMIT 1")
                        )
                    ).scalar_one_or_none()
    except (TimeoutError, SQLAlchemyError, OSError) as exc:
        raise ServiceUnavailableError(
            code="database_unavailable",
            message="database dependency is unavailable",
        ) from exc

    actual_revision = str(version_row) if version_row is not None else None
    if expected_revision is not None and actual_revision != expected_revision:
        raise ServiceUnavailableError(
            code="migration_version_mismatch",
            message="database migration revision does not match required version",
            details={
                "expected_revision": expected_revision,
                "actual_revision": actual_revision,
            },
        )

    return ReadinessStatus(
        status="ready",
        database="ok",
        migration_revision=actual_revision or "unknown",
    )
