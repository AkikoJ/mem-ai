"""Bounded database/migration readiness checks and controlled Prometheus metrics exposition."""

from __future__ import annotations

import asyncio
import hmac
import logging
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi import Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.config import Settings, is_ip_in_networks
from mem_ai.core.exceptions import (
    ForbiddenError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
)
from mem_ai.core.telemetry import GaugeSample, TelemetryRegistry
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import service as memories_service

logger = logging.getLogger("mem_ai.health")

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_JOB_STATUS_LABELS = (
    "pending",
    "running",
    "retry_wait",
    "reconciling",
    "failed",
    "cancelled",
    "succeeded",
)


class ReadinessStatus(BaseModel):
    """Diagnostic payload for `/health/ready`."""

    database: str = "ok"
    migration_revision: str


@lru_cache(maxsize=1)
def _discover_alembic_head() -> str | None:
    candidates = (Path(__file__).resolve().parents[4], Path.cwd())
    for root in candidates:
        alembic_ini = root / "alembic.ini"
        migrations_dir = root / "migrations"
        if alembic_ini.exists() and migrations_dir.exists():
            cfg = Config(str(alembic_ini))
            cfg.set_main_option("script_location", str(migrations_dir))
            return ScriptDirectory.from_config(cfg).get_current_head()
        if migrations_dir.exists():
            cfg = Config()
            cfg.set_main_option("script_location", str(migrations_dir))
            return ScriptDirectory.from_config(cfg).get_current_head()
    return None


def resolve_expected_migration_revision(settings: Settings) -> str | None:
    """Return the required migration revision from settings or local Alembic scripts."""
    if settings.expected_alembic_revision:
        return settings.expected_alembic_revision
    return _discover_alembic_head()


def resolve_compatible_migration_revisions(settings: Settings) -> tuple[str, ...]:
    """Return all Alembic revisions accepted as schema-compatible for rolling upgrades."""
    revisions: list[str] = []
    primary = resolve_expected_migration_revision(settings)
    if primary:
        revisions.append(primary)
    for rev in settings.compatible_alembic_revisions:
        cleaned = rev.strip()
        if cleaned and cleaned not in revisions:
            revisions.append(cleaned)
    return tuple(revisions)


async def check_readiness(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    app_ready: bool = True,
) -> ReadinessStatus:
    """Verify startup completion, PostgreSQL connectivity, and compatible Alembic revision."""
    if not app_ready:
        raise ServiceUnavailableError(
            code="service_not_ready",
            message="application startup not complete or shutting down",
        )

    expected_revision = resolve_expected_migration_revision(settings)
    compatible_revisions = resolve_compatible_migration_revisions(settings)

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
    if (
        not compatible_revisions
        or actual_revision is None
        or actual_revision not in compatible_revisions
    ):
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


def authorize_metrics_access(
    settings: Settings,
    *,
    client_host: str | None,
    authorization_header: str | None,
) -> None:
    """Enforce controlled network allowlist and optional Bearer token for `/metrics`."""
    if not settings.metrics_enabled:
        raise NotFoundError(code="not_found", message="resource not found")

    allowed_networks = settings.metrics_allowed_proxy_networks()
    if allowed_networks and not is_ip_in_networks(client_host, allowed_networks):
        raise ForbiddenError(
            code="metrics_access_denied",
            message="metrics endpoint access denied",
        )

    if settings.metrics_bearer_token is not None:
        expected = settings.metrics_bearer_token.get_secret_value().strip()
        raw = (authorization_header or "").strip()
        parts = raw.split(None, 1)
        if (
            len(parts) != 2
            or parts[0].lower() != "bearer"
            or not hmac.compare_digest(parts[1].strip(), expected)
        ):
            raise UnauthorizedError(
                code="invalid_metrics_credentials",
                message="invalid metrics credentials",
            )


async def collect_prometheus_metrics(
    session_factory: async_sessionmaker[AsyncSession] | None,
    telemetry: TelemetryRegistry,
    *,
    timeout_seconds: float = 2.0,
) -> str:
    """Render in-process metrics enriched with bounded point-in-time PostgreSQL gauges."""
    extra_gauges: list[GaugeSample] = []
    if session_factory is not None:
        now = datetime.now(UTC)
        try:
            async with asyncio.timeout(timeout_seconds):
                job_obs, gen_obs, barrier_obs = await asyncio.gather(
                    jobs_service.get_job_observability(session_factory),
                    conversation_queries.get_generation_observability(session_factory),
                    memories_service.get_memory_barrier_observability(session_factory),
                )
        except Exception:
            logger.warning("Failed to collect PostgreSQL gauges for /metrics", exc_info=True)
            extra_gauges.append(
                GaugeSample(
                    name="mem_ai_metrics_db_scrape_error",
                    value=1.0,
                    help_text="1 when live PostgreSQL metrics collection failed during scrape.",
                )
            )
        else:
            extra_gauges.append(
                GaugeSample(
                    name="mem_ai_metrics_db_scrape_error",
                    value=0.0,
                    help_text="1 when live PostgreSQL metrics collection failed during scrape.",
                )
            )
            for job_status in _JOB_STATUS_LABELS:
                extra_gauges.append(
                    GaugeSample(
                        name="mem_ai_jobs_count",
                        value=float(job_obs.status_counts.get(job_status, 0)),
                        labels={"status": job_status},
                        help_text="Current durable job count in PostgreSQL by status.",
                    )
                )
            backlog = job_obs.pending_count + job_obs.retry_wait_count
            oldest_wait = (
                max(0.0, (now - job_obs.oldest_due_at).total_seconds())
                if job_obs.oldest_due_at is not None
                else 0.0
            )
            last_success_ts = (
                job_obs.last_success_at.timestamp() if job_obs.last_success_at is not None else 0.0
            )
            extra_gauges.extend(
                (
                    GaugeSample(
                        name="mem_ai_jobs_backlog",
                        value=float(backlog),
                        help_text="Number of pending or retry-waiting durable jobs.",
                    ),
                    GaugeSample(
                        name="mem_ai_jobs_reconciling",
                        value=float(job_obs.reconciling_count),
                        help_text="Number of durable jobs awaiting external reconciliation.",
                    ),
                    GaugeSample(
                        name="mem_ai_jobs_failed",
                        value=float(job_obs.failed_count),
                        help_text="Number of durable jobs in terminal failed status.",
                    ),
                    GaugeSample(
                        name="mem_ai_jobs_oldest_wait_seconds",
                        value=oldest_wait,
                        help_text="Age in seconds of the oldest waiting or reconciling job.",
                    ),
                    GaugeSample(
                        name="mem_ai_jobs_last_success_timestamp_seconds",
                        value=last_success_ts,
                        help_text="Unix timestamp of the latest succeeded durable job.",
                    ),
                    GaugeSample(
                        name="mem_ai_generations_running",
                        value=float(gen_obs.running_count),
                        help_text="Current number of persisted running generations.",
                    ),
                    GaugeSample(
                        name="mem_ai_generations_expired_leases",
                        value=float(gen_obs.expired_lease_count),
                        help_text="Running generations whose lease has expired awaiting recovery.",
                    ),
                    GaugeSample(
                        name="mem_ai_memory_barriers_active",
                        value=float(barrier_obs.clearing_count),
                        labels={"barrier_state": "clearing"},
                        help_text="Active memory deletion barriers by barrier_state.",
                    ),
                    GaugeSample(
                        name="mem_ai_memory_barriers_active",
                        value=float(barrier_obs.reconciling_count),
                        labels={"barrier_state": "reconciling"},
                        help_text="Active memory deletion barriers by barrier_state.",
                    ),
                    GaugeSample(
                        name="mem_ai_memory_barrier_oldest_duration_seconds",
                        value=(
                            max(0.0, (now - barrier_obs.oldest_active_since).total_seconds())
                            if barrier_obs.oldest_active_since is not None
                            else 0.0
                        ),
                        help_text="Elapsed seconds for the oldest active memory clearing barrier.",
                    ),
                )
            )

    return telemetry.render_prometheus(extra_gauges=extra_gauges)


async def build_metrics_response(
    request: Request,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None,
    settings: Settings,
    telemetry: TelemetryRegistry,
) -> PlainTextResponse:
    """Shared controlled `/metrics` HTTP handler for API and standalone Worker probes."""
    client_host = request.client.host if request.client is not None else None
    authorize_metrics_access(
        settings,
        client_host=client_host,
        authorization_header=request.headers.get("authorization"),
    )
    body = await collect_prometheus_metrics(
        session_factory,
        telemetry,
        timeout_seconds=settings.db_health_check_timeout_seconds,
    )
    return PlainTextResponse(
        content=body,
        media_type=PROMETHEUS_CONTENT_TYPE,
        headers={"Cache-Control": "no-store"},
    )
