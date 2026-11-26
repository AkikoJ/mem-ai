"""Production operational, budget, recovery replay, and provider verification CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.contracts.memory_provider import (
    MemoryListQuery,
    MemoryProvider,
    MemoryRecallQuery,
    MemoryWriteInput,
)
from mem_ai.contracts.model_provider import ModelMessage, ModelProvider, ModelRequest
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import AppError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.integrations.http import create_http_client
from mem_ai.modules.auth.adapters import (
    AuthRequest,
    DevelopmentAuthAdapter,
    StaticTokenAuthAdapter,
    build_credential_verifier,
)
from mem_ai.modules.conversations import cleanup as conversation_cleanup
from mem_ai.modules.conversations import queries as conversation_queries
from mem_ai.modules.conversations.queries import DatabaseRecalledMemoryReferenceMarker
from mem_ai.modules.health import service as health_service
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.modules.operations.audit import (
    AuditReplayPlan,
    RecoveryReplayStatus,
    load_deletion_audit_events,
    plan_deletion_audit_replay,
)
from mem_ai.modules.users import cleanup as user_cleanup
from mem_ai.modules.users import service as users_service


class ConnectionBudgetReport(BaseModel):
    """
    计算统一单进程后端副本的 PostgreSQL 连接池预算。

    在统一单进程架构中，每个副本同时处理 HTTP 请求与内嵌后台任务。
    """

    model_config = ConfigDict(frozen=True)

    replicas: int
    per_process_max_connections: int
    total_replica_connections: int
    reserved_connections: int
    total_required_connections: int
    postgres_max_connections: int
    max_concurrent_requests: int
    max_concurrent_generations: int
    worker_concurrency: int
    worker_loop_enabled: bool
    within_budget: bool
    pool_adequate: bool


class AuditReplayReport(BaseModel):
    """Execution summary of a post-backup deletion audit replay run."""

    model_config = ConfigDict(frozen=True)

    archive_path: str
    since: datetime | None
    until: datetime | None
    dry_run: bool
    applied: bool
    plan: AuditReplayPlan
    enqueued_count: int = 0
    already_applied_count: int = 0
    skipped_missing_count: int = 0


class OperationalHealthReport(BaseModel):
    """Point-in-time operational readiness, job queue, generation lease, and barrier snapshot."""

    model_config = ConfigDict(frozen=True)

    database: str
    migration_revision: str
    jobs_pending: int
    jobs_running: int
    jobs_retry_wait: int
    jobs_reconciling: int
    jobs_failed: int
    jobs_oldest_due_at: datetime | None
    jobs_last_success_at: datetime | None
    generations_running: int
    generations_expired_leases: int
    barriers_clearing: int
    barriers_reconciling: int
    barriers_oldest_active_since: datetime | None
    alerts: tuple[str, ...] = ()


class ProviderVerificationReport(BaseModel):
    """Sanitized verification summary for limited-privilege staging/production provider probes."""

    model_config = ConfigDict(frozen=True)

    app_env: str
    auth_mode: str
    auth_checked: bool = False
    auth_ok: bool | None = None
    model_checked: bool = False
    model_sync_ok: bool | None = None
    model_stream_ok: bool | None = None
    model_cancel_ok: bool | None = None
    memory_checked: bool = False
    memory_create_status: str | None = None
    memory_idempotency_ok: bool | None = None
    memory_recall_ok: bool | None = None
    memory_pagination_ok: bool | None = None
    memory_delete_status: str | None = None
    memory_reconcile_status: str | None = None
    memory_reconcile_ok: bool | None = None


def compute_connection_budget(
    settings: Settings,
    *,
    replicas: int,
    reserved_connections: int = 10,
    postgres_max_connections: int = 100,
) -> ConnectionBudgetReport:
    """
    验证统一单进程后端的 PostgreSQL 连接总预算。

    统一单进程架构下，每个副本同时承载 HTTP 请求与内嵌后台任务执行器。
    """
    if replicas < 0 or reserved_connections < 0:
        raise ValueError("replica count and reserved connections must be non-negative")
    if postgres_max_connections < 1:
        raise ValueError("postgres_max_connections must be at least 1")

    per_process = settings.db_pool_size + settings.db_max_overflow
    replica_conns = replicas * per_process
    total = replica_conns + reserved_connections

    # 当内嵌后台任务执行器启用时，每个进程的连接池必须足以支持在线请求、生成和后台任务并发
    worker_enabled = settings.worker_loop_enabled
    if worker_enabled:
        min_required = (
            settings.max_concurrent_requests
            + settings.max_concurrent_generations
            + settings.worker_concurrency
            + 1  # 至少预留 1 个连接给健康检查等其他操作
        )
        pool_adequate = per_process >= min_required
    else:
        pool_adequate = True

    return ConnectionBudgetReport(
        replicas=replicas,
        per_process_max_connections=per_process,
        total_replica_connections=replica_conns,
        reserved_connections=reserved_connections,
        total_required_connections=total,
        postgres_max_connections=postgres_max_connections,
        max_concurrent_requests=settings.max_concurrent_requests,
        max_concurrent_generations=settings.max_concurrent_generations,
        worker_concurrency=settings.worker_concurrency,
        worker_loop_enabled=worker_enabled,
        within_budget=total <= postgres_max_connections,
        pool_adequate=pool_adequate,
    )


def _configured_memory_provider_id(
    settings: Settings,
    memory_provider: MemoryProvider | None,
) -> str | None:
    if memory_provider is not None:
        return memory_provider.provider_id
    if settings.memory_provider_mode != "disabled":
        return settings.memory_provider_mode
    return None


async def replay_deletion_audit_archive(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    archive_path: str | Path,
    settings: Settings,
    since: datetime | None = None,
    until: datetime | None = None,
    dry_run: bool = False,
    memory_provider: MemoryProvider | None = None,
) -> AuditReplayReport:
    """Replay post-backup deletions and epoch advancements through resource-owner entry points."""
    events = load_deletion_audit_events(archive_path, since=since, until=until)
    plan = plan_deletion_audit_replay(events)
    if dry_run:
        return AuditReplayReport(
            archive_path=str(archive_path),
            since=since,
            until=until,
            dry_run=True,
            applied=False,
            plan=plan,
        )

    counts: dict[RecoveryReplayStatus, int] = {
        "enqueued": 0,
        "already_applied": 0,
        "skipped_missing": 0,
    }
    marker = DatabaseRecalledMemoryReferenceMarker()
    configured_provider_id = _configured_memory_provider_id(settings, memory_provider)

    for user_id in plan.user_deletes:
        outcome = await user_cleanup.replay_user_deletion_for_recovery(
            session_factory,
            owner_id=user_id,
            settings=settings,
            memory_provider=memory_provider,
            provider_id=configured_provider_id,
        )
        counts[outcome] += 1

    for user_id, conversation_id in plan.conversation_deletes:
        outcome = await conversation_cleanup.replay_conversation_deletion_for_recovery(
            session_factory,
            owner_id=user_id,
            conversation_id=conversation_id,
            memory_provider=memory_provider,
            provider_id=configured_provider_id,
        )
        counts[outcome] += 1

    for user_id, provider_id, target_epoch in plan.memory_clears:
        outcome, _ = await memories_service.replay_clear_scope_for_recovery(
            session_factory,
            user_id=user_id,
            provider_id=provider_id,
            target_epoch=target_epoch,
            recalled_memory_marker=marker,
            max_attempts=settings.worker_max_attempts,
            attempt_retention_days=settings.job_attempt_retention_days,
        )
        counts[outcome] += 1

    for user_id, provider_id, memory_id in plan.memory_deletes:
        outcome = await memories_service.replay_single_memory_delete_for_recovery(
            session_factory,
            user_id=user_id,
            provider_id=provider_id,
            memory_id=memory_id,
            recalled_memory_marker=marker,
            max_attempts=settings.worker_max_attempts,
            attempt_retention_days=settings.job_attempt_retention_days,
        )
        counts[outcome] += 1

    return AuditReplayReport(
        archive_path=str(archive_path),
        since=since,
        until=until,
        dry_run=False,
        applied=True,
        plan=plan,
        enqueued_count=counts["enqueued"],
        already_applied_count=counts["already_applied"],
        skipped_missing_count=counts["skipped_missing"],
    )


async def inspect_operational_health(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> OperationalHealthReport:
    """Collect readiness, job queue, generation lease, and memory barrier operational signals."""
    readiness = await health_service.check_readiness(session_factory, settings)
    job_obs, gen_obs, barrier_obs = await asyncio.gather(
        jobs_service.get_job_observability(session_factory),
        conversation_queries.get_generation_observability(session_factory),
        memories_service.get_memory_barrier_observability(session_factory),
    )
    alerts: list[str] = []
    if job_obs.failed_count > 0:
        alerts.append("exhausted_failed_jobs")
    if job_obs.reconciling_count > 0:
        alerts.append("reconciling_jobs_pending")
    if gen_obs.expired_lease_count > 0:
        alerts.append("expired_generation_leases")
    if barrier_obs.clearing_count > 0 or barrier_obs.reconciling_count > 0:
        alerts.append("active_memory_barriers")

    return OperationalHealthReport(
        database=readiness.database,
        migration_revision=readiness.migration_revision,
        jobs_pending=job_obs.pending_count,
        jobs_running=job_obs.running_count,
        jobs_retry_wait=job_obs.retry_wait_count,
        jobs_reconciling=job_obs.reconciling_count,
        jobs_failed=job_obs.failed_count,
        jobs_oldest_due_at=job_obs.oldest_due_at,
        jobs_last_success_at=job_obs.last_success_at,
        generations_running=gen_obs.running_count,
        generations_expired_leases=gen_obs.expired_lease_count,
        barriers_clearing=barrier_obs.clearing_count,
        barriers_reconciling=barrier_obs.reconciling_count,
        barriers_oldest_active_since=barrier_obs.oldest_active_since,
        alerts=tuple(alerts),
    )


async def verify_providers(
    settings: Settings,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    check_auth: bool = False,
    auth_headers: dict[str, str] | None = None,
    client_host: str | None = None,
    check_model: bool = False,
    model_id: str | None = None,
    model_provider: ModelProvider | None = None,
    check_memory: bool = False,
    memory_provider: MemoryProvider | None = None,
    probe_user_id: UUID | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> ProviderVerificationReport:
    """Run controlled limited-privilege checks for production auth, model, and MemOS adapters."""
    auth_ok: bool | None = None
    model_sync_ok: bool | None = None
    model_stream_ok: bool | None = None
    model_cancel_ok: bool | None = None
    memory_create_status: str | None = None
    memory_idempotency_ok: bool | None = None
    memory_recall_ok: bool | None = None
    memory_pagination_ok: bool | None = None
    memory_delete_status: str | None = None
    memory_reconcile_status: str | None = None
    memory_reconcile_ok: bool | None = None

    if check_auth:
        verifier = build_credential_verifier(settings)
        if settings.app_env == "production" and isinstance(
            verifier, (DevelopmentAuthAdapter, StaticTokenAuthAdapter)
        ):
            raise ValueError("development/static auth adapter is forbidden in production")
        if auth_headers is None:
            raise ValueError("auth_headers (--bearer-token) is required when --check-auth is set")
        principal = await verifier.verify(
            AuthRequest(headers=auth_headers, client_host=client_host)
        )
        auth_ok = bool(principal.issuer and principal.subject)

    owned_http = http_client is None
    client = http_client if http_client is not None else create_http_client(settings)
    try:
        if check_model:
            resolved_model = model_provider
            if resolved_model is None:
                if (
                    settings.model_provider_mode != "openai_compatible"
                    or settings.openai_compat_api_key is None
                ):
                    raise ValueError("openai_compatible credentials are not configured")
                seeds = [
                    ModelConfigSeed.model_validate(entry)
                    for entry in settings.model_registry_entries
                ]
                upstream_map = {seed.model_id: seed.resolved_upstream_model() for seed in seeds}
                resolved_model = OpenAICompatibleModelProvider(
                    http_client=client,
                    base_url=settings.openai_compat_base_url,
                    api_key=settings.openai_compat_api_key,
                    timeout_seconds=settings.openai_compat_request_timeout_seconds,
                    upstream_model_map=upstream_map,
                )
            target_model = model_id or settings.model_default_id
            if not target_model:
                raise ValueError("model_id or MODEL_DEFAULT_ID is required for model verification")

            req = ModelRequest(
                model_id=target_model,
                messages=(ModelMessage(role="user", content="Reply with OK."),),
            )
            sync_resp = await resolved_model.generate(req)
            model_sync_ok = bool(sync_resp.content.strip())

            chunks = []
            async for chunk in resolved_model.stream_generate(req):
                chunks.append(chunk)
            model_stream_ok = bool(chunks) and any(chunk.is_final for chunk in chunks)

            stream_iter = resolved_model.stream_generate(req).__aiter__()
            try:
                await stream_iter.__anext__()
            finally:
                aclose = getattr(stream_iter, "aclose", None)
                if callable(aclose):
                    await aclose()
            model_cancel_ok = True

        if check_memory:
            resolved_mem = memory_provider
            if resolved_mem is None:
                if (
                    settings.memory_provider_mode != "memos"
                    or settings.memos_api_key is None
                    or session_factory is None
                ):
                    raise ValueError("memos credentials and DB session are required")
                resolved_mem = MemOSMemoryProvider(
                    http_client=client,
                    session_factory=session_factory,
                    api_key=settings.memos_api_key,
                    base_url=settings.memos_base_url,
                    timeout_seconds=settings.memos_request_timeout_seconds,
                    list_page_size=settings.memos_list_page_size,
                    list_max_items=settings.memos_list_max_items,
                    snapshot_ttl_seconds=settings.memory_list_snapshot_ttl_seconds,
                )
            if probe_user_id is not None:
                test_uid = probe_user_id
            elif session_factory is not None:
                probe_user = await users_service.resolve_or_create_user(
                    session_factory,
                    issuer="mem-ai:ops-probe",
                    subject="provider-verification",
                )
                test_uid = probe_user.id
            else:
                test_uid = uuid4()

            # 1. Explicit create + repeated write_id idempotency probe + second item for pagination
            write_id_1 = f"probe_{uuid4().hex[:12]}"
            write_input_1 = MemoryWriteInput(
                write_id=write_id_1,
                user_id=test_uid,
                scope="user",
                content=f"mem-ai staging verification probe A {write_id_1}",
                memory_epoch=0,
            )
            write_res_1 = await resolved_mem.create_memory(write_input_1)
            memory_create_status = write_res_1.status

            retry_res = await resolved_mem.create_memory(write_input_1)
            memory_idempotency_ok = retry_res.status in ("succeeded", "accepted", "reconciling")

            write_id_2 = f"probe_{uuid4().hex[:12]}"
            write_res_2 = await resolved_mem.create_memory(
                MemoryWriteInput(
                    write_id=write_id_2,
                    user_id=test_uid,
                    scope="user",
                    content=f"mem-ai staging verification probe B {write_id_2}",
                    memory_epoch=0,
                )
            )

            # 2. Recall probe
            recalled = await resolved_mem.recall(
                MemoryRecallQuery(
                    user_id=test_uid,
                    scope="user",
                    query="staging verification probe",
                    limit=5,
                )
            )
            memory_recall_ok = isinstance(recalled, list)

            # 3. Real multi-page cursor pagination probe (limit=1 across >= 2 items)
            first_page = await resolved_mem.list_memories(
                MemoryListQuery(user_id=test_uid, scope="user", limit=1)
            )
            if first_page.has_more and first_page.next_cursor is not None:
                second_page = await resolved_mem.list_memories(
                    MemoryListQuery(
                        user_id=test_uid,
                        scope="user",
                        limit=1,
                        cursor=first_page.next_cursor,
                    )
                )
                first_ids = {item.memory_id for item in first_page.items}
                second_ids = {item.memory_id for item in second_page.items}
                memory_pagination_ok = bool(second_page.items) and first_ids.isdisjoint(second_ids)
            else:
                memory_pagination_ok = False

            # 4. External operation status / unknown-result verification probe
            if write_res_1.external_ref:
                op_status = await resolved_mem.verify_operation(
                    user_id=test_uid,
                    external_ref=write_res_1.external_ref,
                )
                memory_reconcile_status = op_status.status
                memory_reconcile_ok = op_status.status in ("succeeded", "accepted", "reconciling")
            else:
                memory_reconcile_ok = False

            # 5. Cleanup probe memories via delete_memory
            probe_ids = [
                mid
                for mid in dict.fromkeys(
                    (write_res_1.memory_id, retry_res.memory_id, write_res_2.memory_id)
                )
                if mid is not None
            ]
            if probe_ids:
                statuses = [
                    (await resolved_mem.delete_memory(user_id=test_uid, memory_id=mid)).status
                    for mid in probe_ids
                ]
                memory_delete_status = statuses[0] if len(set(statuses)) == 1 else "partial"
            else:
                memory_delete_status = "skipped_no_memory_id"
    finally:
        if owned_http:
            await client.aclose()

    return ProviderVerificationReport(
        app_env=settings.app_env,
        auth_mode=settings.auth_mode,
        auth_checked=check_auth,
        auth_ok=auth_ok,
        model_checked=check_model,
        model_sync_ok=model_sync_ok,
        model_stream_ok=model_stream_ok,
        model_cancel_ok=model_cancel_ok,
        memory_checked=check_memory,
        memory_create_status=memory_create_status,
        memory_idempotency_ok=memory_idempotency_ok,
        memory_recall_ok=memory_recall_ok,
        memory_pagination_ok=memory_pagination_ok,
        memory_delete_status=memory_delete_status,
        memory_reconcile_status=memory_reconcile_status,
        memory_reconcile_ok=memory_reconcile_ok,
    )


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m mem_ai.ops",
        description="Mem-AI production deployment, recovery replay, and operational CLI.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    budget = sub.add_parser("check-budget", help="Validate PostgreSQL connection pool budget.")
    budget.add_argument("--replicas", type=int, required=True, help="Number of unified backend service replicas.")
    budget.add_argument("--reserved-connections", type=int, default=10)
    budget.add_argument("--postgres-max-connections", type=int, default=100)

    replay = sub.add_parser(
        "replay-audit",
        help="Replay post-backup deletion & epoch audit events into a restored PostgreSQL DB.",
    )
    replay.add_argument("--archive-path", type=Path, default=None)
    replay.add_argument("--since", type=str, default=None, help="ISO-8601 backup timestamp (UTC).")
    replay.add_argument("--until", type=str, default=None, help="Optional upper ISO-8601 bound.")
    replay.add_argument("--dry-run", action="store_true", help="Plan actions without DB writes.")

    requeue = sub.add_parser("requeue-job", help="Controlled requeue of an exhausted failed job.")
    requeue.add_argument("--job-id", type=UUID, required=True)
    requeue.add_argument("--extra-attempts", type=int, default=3)
    requeue.add_argument("--reason", type=str, default=None)

    sub.add_parser("inspect-health", help="Inspect DB readiness, jobs, leases, and barriers.")

    verify = sub.add_parser(
        "verify-providers",
        help="Run limited-privilege staging/production provider verification.",
    )
    verify.add_argument("--check-auth", action="store_true")
    verify.add_argument("--bearer-token", type=str, default=None)
    verify.add_argument("--check-model", action="store_true")
    verify.add_argument("--model-id", type=str, default=None)
    verify.add_argument("--check-memory", action="store_true")
    verify.add_argument("--probe-user-id", type=UUID, default=None)
    return parser


async def _run_async_command(
    args: argparse.Namespace,
    settings: Settings,
) -> tuple[int, dict[str, Any]]:
    if args.command == "check-budget":
        report = compute_connection_budget(
            settings,
            replicas=args.replicas,
            reserved_connections=args.reserved_connections,
            postgres_max_connections=args.postgres_max_connections,
        )
        ok = report.within_budget and report.pool_adequate
        return (0 if ok else 1), report.model_dump(mode="json")

    engine = create_async_engine_from_settings(settings)
    session_factory = create_session_factory(engine)
    try:
        if args.command == "replay-audit":
            archive = args.archive_path or (
                Path(settings.deletion_audit_archive_path)
                if settings.deletion_audit_archive_path
                else None
            )
            if archive is None:
                raise ValueError("--archive-path or DELETION_AUDIT_ARCHIVE_PATH is required")
            replay_report = await replay_deletion_audit_archive(
                session_factory,
                archive_path=archive,
                settings=settings,
                since=_parse_iso_datetime(args.since),
                until=_parse_iso_datetime(args.until),
                dry_run=bool(args.dry_run),
            )
            return 0, replay_report.model_dump(mode="json")

        if args.command == "requeue-job":
            job = await jobs_service.requeue_failed_job(
                session_factory,
                job_id=args.job_id,
                extra_attempts=args.extra_attempts,
                reason=args.reason,
            )
            return 0, {
                "job_id": str(job.id),
                "job_type": job.job_type,
                "status": job.status,
                "attempt_count": job.attempt_count,
                "max_attempts": job.max_attempts,
            }

        if args.command == "inspect-health":
            health_report = await inspect_operational_health(session_factory, settings)
            return 0, health_report.model_dump(mode="json")

        if args.command == "verify-providers":
            headers = (
                {"Authorization": f"Bearer {args.bearer_token}"} if args.bearer_token else None
            )
            verify_report = await verify_providers(
                settings,
                session_factory=session_factory,
                check_auth=bool(args.check_auth),
                auth_headers=headers,
                check_model=bool(args.check_model),
                model_id=args.model_id,
                check_memory=bool(args.check_memory),
                probe_user_id=args.probe_user_id,
            )
            return 0, verify_report.model_dump(mode="json")
    finally:
        await engine.dispose()

    raise ValueError(f"unsupported ops command: {args.command}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    """CLI entrypoint printing structured JSON output without exposing secrets."""
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    resolved_settings = settings if settings is not None else Settings()
    try:
        exit_code, payload = asyncio.run(_run_async_command(args, resolved_settings))
    except (AppError, ValueError, FileNotFoundError) as exc:
        code = exc.code if isinstance(exc, AppError) else "ops_error"
        print(json.dumps({"ok": False, "code": code, "message": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, default=str))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
