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


class ReadinessStatus(BaseModel):
    """Diagnostic payload for `/health/ready`."""

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
                    applied_revisions: list[str] = []
                else:
                    rows = (
                        (
                            await session.execute(
                                text("SELECT version_num FROM public.alembic_version")
                            )
                        )
                        .scalars()
                        .all()
                    )
                    applied_revisions = [str(row) for row in rows]
    except (TimeoutError, SQLAlchemyError, OSError) as exc:
        raise ServiceUnavailableError(
            code="database_unavailable",
            message="database dependency is unavailable",
        ) from exc

    actual_revision = applied_revisions[0] if len(applied_revisions) == 1 else None
    if expected_revision is None or actual_revision is None or actual_revision != expected_revision:
        raise ServiceUnavailableError(
            code="migration_version_mismatch",
            message="database migration revision does not match required version",
            details={
                "expected_revision": expected_revision,
                "actual_revision": actual_revision,
            },
        )

    return ReadinessStatus(
        database="ok",
        migration_revision=actual_revision,
    )
