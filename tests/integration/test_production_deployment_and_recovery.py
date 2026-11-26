"""Integration drills for unified single-process deployment, audit replay, and contracts."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import text

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import MemoryRecallQuery, MemoryWriteInput
from mem_ai.core.config import Settings
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.adapters.memos import MemOSMemoryProvider
from mem_ai.modules.models.adapters.openai_compatible import OpenAICompatibleModelProvider
from mem_ai.modules.operations.audit import JsonlDeletionAuditArchiver
from mem_ai.ops import (
    compute_connection_budget,
    inspect_operational_health,
    replay_deletion_audit_archive,
    verify_providers,
)
from mem_ai.workers.assembly import build_worker_registry
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.conftest import (
    FakeMemoryProvider,
    FakeModelProvider,
    parse_sse_text,
    sign_hs256_jwt,
)

_PROD_JWT_SECRET = "prod-drill-jwt-signing-secret-32chars!!"
_PROD_RECEIPT_SECRET = "prod-drill-receipt-signing-key-32ch!!"
_PROD_METRICS_TOKEN = "prod-drill-metrics-bearer-token-32ch!!"
_PROD_ISSUER = "https://idp.prod.mem-ai.example"
_PROD_AUDIENCE = "mem-ai-prod-api"
_PROD_HOST = "api.prod.mem-ai.example"


def _production_settings(
    base_settings: Settings,
    *,
    archive_path: Path | None = None,
    heartbeat_file: Path | None = None,
    worker_loop_enabled: bool = True,
    worker_poll_interval_seconds: float | None = None,
    worker_heartbeat_interval_seconds: float | None = None,
) -> Settings:
    updates: dict[str, Any] = {
        "app_env": "production",
        "auth_mode": "jwt_hs256",
        "auth_trusted_issuers": [_PROD_ISSUER],
        "auth_trusted_audience": _PROD_AUDIENCE,
        "auth_jwt_hs256_secret": SecretStr(_PROD_JWT_SECRET),
        "deletion_receipt_secret": SecretStr(_PROD_RECEIPT_SECRET),
        "allowed_hosts": [_PROD_HOST],
        "cors_allowed_origins": ["https://app.prod.mem-ai.example"],
        "metrics_enabled": True,
        "metrics_allowed_networks": ["127.0.0.1/32"],
        "metrics_bearer_token": SecretStr(_PROD_METRICS_TOKEN),
        "deletion_audit_archive_path": str(archive_path) if archive_path else None,
        "worker_loop_enabled": worker_loop_enabled,
        "worker_heartbeat_file": str(heartbeat_file) if heartbeat_file else None,
        "model_sync_on_startup": True,
        "model_registry_entries": [
            {
                "model_id": "gpt-4o-mini",
                "provider_id": "openai_compatible",
                "upstream_model": "gpt-4o-mini",
                "display_name": "GPT-4o Mini",
                "enabled": True,
                "supports_streaming": True,
            }
        ],
        "model_default_id": "gpt-4o-mini",
    }
    if worker_poll_interval_seconds is not None:
        updates["worker_poll_interval_seconds"] = worker_poll_interval_seconds
    if worker_heartbeat_interval_seconds is not None:
        updates["worker_heartbeat_interval_seconds"] = worker_heartbeat_interval_seconds
    return base_settings.model_copy(update=updates)


def _make_jwt(subject: str, *, permissions: list[str] | None = None) -> str:
    now_ts = int(datetime.now(UTC).timestamp())
    return sign_hs256_jwt(
        {
            "iss": _PROD_ISSUER,
            "aud": _PROD_AUDIENCE,
            "sub": subject,
            "iat": now_ts,
            "exp": now_ts + 3600,
            "permissions": permissions or ["chat:write"],
        },
        secret=_PROD_JWT_SECRET,
    )


def _auth_header(subject: str, *, permissions: list[str] | None = None) -> dict[str, str]:
    token = _make_jwt(subject, permissions=permissions)
    return {"Authorization": f"Bearer {token}", "Host": _PROD_HOST}


@pytest.mark.integration
@pytest.mark.anyio
async def test_unified_single_process_lifecycle_probes_recovery_and_budget(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    """Verify unified single-process app runs probes, embedded job recovery, drain, and budget."""
    hb_file = tmp_path / "worker-heartbeat.json"
    prod_settings = _production_settings(
        migrated_pg_settings,
        heartbeat_file=hb_file,
        worker_loop_enabled=True,
        worker_poll_interval_seconds=0.05,
        worker_heartbeat_interval_seconds=0.05,
    )

    budget = compute_connection_budget(
        prod_settings,
        replicas=3,
        reserved_connections=10,
        postgres_max_connections=100,
    )
    assert budget.within_budget is True
    assert budget.pool_adequate is True
    assert budget.total_replica_connections == 3 * (
        prod_settings.db_pool_size + prod_settings.db_max_overflow
    )

    memory_provider = FakeMemoryProvider()
    model_provider = FakeModelProvider(prefix="prod:")
    app = create_app(
        prod_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
    )

    async with app.router.lifespan_context(app):
        sf = app.state.session_factory
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 40001))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://{_PROD_HOST}"
        ) as client:
            # Unified liveness, readiness, authenticated /metrics, and atomic heartbeat file
            assert (await client.get("/health/live")).status_code == 200
            ready_resp = await client.get("/health/ready")
            assert ready_resp.status_code == 200
            assert ready_resp.json()["data"]["migration_revision"] == "20261124_0012"

            for _ in range(40):
                if hb_file.exists():
                    break
                await asyncio.sleep(0.05)
            assert hb_file.exists()
            hb_payload = json.loads(hb_file.read_text(encoding="utf-8"))
            assert hb_payload["worker_id"] == app.state.worker_runner.config.worker_id
            assert hb_payload["shutting_down"] is False

            metrics_resp = await client.get(
                "/metrics",
                headers={"Authorization": f"Bearer {_PROD_METRICS_TOKEN}"},
            )
            assert metrics_resp.status_code == 200
            assert "mem_ai_jobs_backlog" in metrics_resp.text
            assert "mem_ai_worker_last_heartbeat_timestamp_seconds " in metrics_resp.text

            # Enqueue an expired-lease job (simulating crashed replica) and confirm recovery
            me = await client.get("/v1/users/me", headers=_auth_header("ops-recover-user"))
            user_id = UUID(me.json()["data"]["userId"])
            async with sf.begin() as session:
                stale_job = await jobs_service.register_and_claim_sync_job_in_session(
                    session,
                    job_type="generation_lease_recovery",
                    business_id="crash-recovery-probe",
                    user_id=user_id,
                    dedupe_key=f"crash-probe:{uuid4().hex}",
                    metadata_json={},
                    lease_duration_seconds=1,
                    worker_id="crashed-replica",
                    now=datetime.now(UTC) - timedelta(seconds=120),
                )
            for _ in range(40):
                refreshed = await jobs_service.get_job(sf, job_id=stale_job.id)
                if refreshed is not None and refreshed.status == "succeeded":
                    break
                await asyncio.sleep(0.05)
            final_job = await jobs_service.get_job(sf, job_id=stale_job.id)
            assert final_job is not None and final_job.status == "succeeded"

            # Controlled requeue of exhausted failed job + operational health inspection
            async with sf.begin() as session:
                failed_job = await jobs_service.register_and_claim_sync_job_in_session(
                    session,
                    job_type="generation_lease_recovery",
                    business_id="exhausted-job-probe",
                    user_id=user_id,
                    dedupe_key=f"exhausted:{uuid4().hex}",
                    metadata_json={},
                    max_attempts=1,
                )
                assert failed_job.lease_token is not None
                await jobs_service.complete_job_in_session(
                    session,
                    job_id=failed_job.id,
                    lease_token=failed_job.lease_token,
                    status="failed",
                    error_code="transient_upstream_failure",
                    error_summary="exhausted attempts during outage",
                )
            health_before = await inspect_operational_health(sf, prod_settings)
            assert "exhausted_failed_jobs" in health_before.alerts

            requeued = await jobs_service.requeue_failed_job(
                sf,
                job_id=failed_job.id,
                extra_attempts=2,
                reason="upstream restored in drill",
            )
            assert requeued.status == "pending"
            for _ in range(40):
                refreshed = await jobs_service.get_job(sf, job_id=failed_job.id)
                if refreshed is not None and refreshed.status == "succeeded":
                    break
                await asyncio.sleep(0.05)
            final_requeued = await jobs_service.get_job(sf, job_id=failed_job.id)
            assert final_requeued is not None and final_requeued.status == "succeeded"

            health_after = await inspect_operational_health(sf, prod_settings)
            assert health_after.jobs_failed == 0
            assert "exhausted_failed_jobs" not in health_after.alerts

    assert app.state.ready is False
    final_hb = json.loads(hb_file.read_text(encoding="utf-8"))
    assert final_hb["shutting_down"] is True


@pytest.mark.integration
@pytest.mark.anyio
async def test_backup_restore_audit_replay_prevents_memory_resurrection_and_is_idempotent(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    """Drill Baseline A: restore DB to pre-deletion backup T0, replay audit archive, then resume."""
    archive_path = tmp_path / "independent-deletion-audit.jsonl"
    prod_settings = _production_settings(
        migrated_pg_settings,
        archive_path=archive_path,
        worker_loop_enabled=False,
    )
    memory_provider = FakeMemoryProvider()
    model_provider = FakeModelProvider(prefix="drill:")
    app = create_app(
        prod_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
    )

    engine = create_async_engine_from_settings(prod_settings)
    sf = create_session_factory(engine)
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 40010))
            async with httpx.AsyncClient(
                transport=transport, base_url=f"http://{_PROD_HOST}"
            ) as client:
                h_single = _auth_header("user-single-del")
                h_clear = _auth_header("user-clear-all")
                h_conv = _auth_header("user-conv-del")
                h_user = _auth_header("user-account-del")

                # 1. Pre-backup state (before T0): create users, memories, conversations.
                u_single = UUID(
                    (await client.get("/v1/users/me", headers=h_single)).json()["data"]["userId"]
                )
                u_clear = UUID(
                    (await client.get("/v1/users/me", headers=h_clear)).json()["data"]["userId"]
                )
                u_conv = UUID(
                    (await client.get("/v1/users/me", headers=h_conv)).json()["data"]["userId"]
                )
                u_deleted = UUID(
                    (await client.get("/v1/users/me", headers=h_user)).json()["data"]["userId"]
                )

                created_mem = await client.post(
                    "/v1/memories",
                    headers=h_single,
                    json={"content": "must stay deleted after restore"},
                )
                assert created_mem.status_code == 201
                deleted_memory_id = created_mem.json()["data"]["memoryId"]

                clear_mem = await client.post(
                    "/v1/memories",
                    headers=h_clear,
                    json={"content": "must stay cleared after restore"},
                )
                assert clear_mem.status_code == 201

                # Create and publish an agent & conversation for u_conv
                ag = await client.post(
                    "/v1/agents",
                    headers=h_conv,
                    json={
                        "name": "Conv Agent",
                        "systemPrompt": "Helpful.",
                        "defaultModelId": "gpt-4o-mini",
                    },
                )
                assert ag.status_code == 201
                agent_id = ag.json()["data"]["agentId"]
                pub = await client.post(f"/v1/agents/{agent_id}/publish", headers=h_conv)
                assert pub.status_code == 200
                chat = await client.post(
                    "/v1/chat",
                    headers={**h_conv, "Idempotency-Key": f"conv-seed-{uuid4().hex}"},
                    json={"agentId": agent_id, "message": "hello before backup"},
                )
                assert chat.status_code == 200
                conv_id = UUID(chat.json()["data"]["conversationId"])

                # Record backup point T0 before post-backup deletions occur.
                backup_t0 = datetime.now(UTC)
                await asyncio.sleep(0.05)

                # 2. Post-backup deletions (after T0): exported to independent JSONL archive!
                del_single = await client.delete(
                    f"/v1/memories/{deleted_memory_id}",
                    headers=h_single,
                )
                assert del_single.status_code in (202, 204)

                del_clear = await client.delete(
                    "/v1/memories?confirm=true",
                    headers=h_clear,
                )
                assert del_clear.status_code in (202, 204)

                del_conv = await client.delete(
                    f"/v1/conversations/{conv_id}",
                    headers=h_conv,
                )
                assert del_conv.status_code == 202

                del_user = await client.delete("/v1/users/me", headers=h_user)
                assert del_user.status_code == 202

        # 3. Simulate restoring primary PostgreSQL DB back to backup point T0!
        #    (Writes & Worker are paused; all post-T0 DB mutations are rolled back and a
        #    pre-backup queued write job from epoch 0 reappears in `jobs`!)
        async with sf.begin() as session:
            await session.execute(text("DELETE FROM memory_tombstones"))
            await session.execute(text("DELETE FROM operations"))
            await session.execute(text("DELETE FROM jobs"))
            await session.execute(
                text(
                    "UPDATE memory_scopes SET epoch = 0, barrier_state = 'idle', "
                    "barrier_operation_id = NULL"
                )
            )
            await session.execute(
                text(
                    "UPDATE conversations SET status = 'active', deleted_at = NULL WHERE id = :cid"
                ),
                {"cid": conv_id},
            )
            await session.execute(
                text("UPDATE users SET status = 'active', deleted_at = NULL WHERE id = :uid"),
                {"uid": u_deleted},
            )
            # Re-introduce a pre-backup queued memory_turn_write job at epoch 0 for u_clear
            # that WOULD resurrect cleared memory if Worker started without audit replay!
            pre_backup_write_job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="memory_turn_write",
                business_id="pre-backup-turn-1",
                user_id=u_clear,
                dedupe_key=f"pre-backup-turn:{uuid4().hex}",
                metadata_json={
                    "write_id": "mw_pre_backup_resurrection_risk",
                    "provider_id": memory_provider.provider_id,
                    "scope": "user",
                    "conversation_id": str(uuid4()),
                    "generation_id": str(uuid4()),
                    "user_message_id": str(uuid4()),
                    "assistant_message_id": str(uuid4()),
                    "memory_epoch": 0,
                    "dispatch_state": "pending",
                },
            )

        # Put the single memory back into provider to prove recall is blocked immediately
        await memory_provider.create_memory(
            MemoryWriteInput(
                write_id="reseeded_for_recall_check",
                user_id=u_clear,
                scope="user",
                content="stale remote memory before clear replay",
                memory_epoch=0,
            )
        )

        # 4. Run dry-run audit replay first -> verify plan without mutating restored DB.
        dry_report = await replay_deletion_audit_archive(
            sf,
            archive_path=archive_path,
            settings=prod_settings,
            since=backup_t0,
            dry_run=True,
            memory_provider=memory_provider,
        )
        assert dry_report.applied is False
        assert u_deleted in dry_report.plan.user_deletes
        assert (u_conv, conv_id) in dry_report.plan.conversation_deletes
        assert (u_clear, memory_provider.provider_id, 1) in dry_report.plan.memory_clears
        assert (
            u_single,
            memory_provider.provider_id,
            deleted_memory_id,
        ) in dry_report.plan.memory_deletes

        # 5. Apply audit replay while writes and Worker are paused.
        applied_report = await replay_deletion_audit_archive(
            sf,
            archive_path=archive_path,
            settings=prod_settings,
            since=backup_t0,
            dry_run=False,
            memory_provider=memory_provider,
        )
        assert applied_report.applied is True
        assert applied_report.enqueued_count == 4

        # Verify pre-backup queued write job for u_clear was IMMEDIATELY cancelled!
        cancelled_write = await jobs_service.get_job(sf, job_id=pre_backup_write_job.id)
        assert cancelled_write is not None
        assert cancelled_write.status == "cancelled"
        assert cancelled_write.last_error_code == "memory_scope_clearing"

        # Verify recall for u_clear degrades to empty under replayed clearing barrier!
        recall_under_barrier = await memories_service.recall_relevant_memories(
            sf,
            memory_provider=memory_provider,
            user_id=u_clear,
            conversation_id=uuid4(),
            query="stale remote memory",
        )
        assert recall_under_barrier.items == ()
        assert recall_under_barrier.degraded is True

        # 6. Run audit replay a SECOND time -> prove idempotency (zero duplicate work).
        second_report = await replay_deletion_audit_archive(
            sf,
            archive_path=archive_path,
            settings=prod_settings,
            since=backup_t0,
            dry_run=False,
            memory_provider=memory_provider,
        )
        assert second_report.enqueued_count == 0
        assert second_report.already_applied_count == 4

        # 7. Resume Worker -> converge all replayed deletions/clears cleanly!
        worker_registry = build_worker_registry(
            sf,
            memory_provider=memory_provider,
            audit_archiver=JsonlDeletionAuditArchiver(archive_path),
        )
        runner = WorkerRunner(
            session_factory=sf,
            registry=worker_registry,
            config=WorkerConfig(
                worker_id="recovery-worker",
                concurrency=4,
                poll_interval_seconds=0.05,
                shutdown_grace_seconds=5.0,
            ),
        )
        stop_event = asyncio.Event()
        worker_task = asyncio.create_task(runner.run_forever(stop_event=stop_event))
        try:
            for _ in range(80):
                obs = await jobs_service.get_job_observability(sf)
                if (
                    obs.pending_count == 0
                    and obs.running_count == 0
                    and obs.retry_wait_count == 0
                    and obs.reconciling_count == 0
                ):
                    break
                await asyncio.sleep(0.05)
        finally:
            stop_event.set()
            await asyncio.wait_for(worker_task, timeout=5.0)

        # Confirm deleted user, deleted conversation, and cleared memories are all gone.
        async with sf() as session:
            u_exists = (
                await session.execute(
                    text("SELECT count(*) FROM users WHERE id = :id"), {"id": u_deleted}
                )
            ).scalar_one()
            c_exists = (
                await session.execute(
                    text("SELECT count(*) FROM conversations WHERE id = :id"), {"id": conv_id}
                )
            ).scalar_one()
            clear_scope = (
                await session.execute(
                    text(
                        "SELECT epoch, barrier_state FROM memory_scopes "
                        "WHERE user_id = :uid AND provider_id = :pid"
                    ),
                    {"uid": u_clear, "pid": memory_provider.provider_id},
                )
            ).one()
        assert u_exists == 0
        assert c_exists == 0
        assert clear_scope.epoch >= 1
        assert clear_scope.barrier_state == "idle"
        # Confirm no memories survived for u_clear in provider
        remaining_clear = await memory_provider.recall(
            MemoryRecallQuery(user_id=u_clear, scope="user", query="stale", limit=10)
        )
        assert remaining_clear == []
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_verify_providers_harness_validates_auth_model_and_memos_capabilities(
    migrated_pg_settings: Settings,
) -> None:
    """Verify prod auth, OpenAI-compatible sync/stream/cancel, and MemOS pagination/reconcile."""
    prod_settings = _production_settings(migrated_pg_settings)
    engine = create_async_engine_from_settings(prod_settings)
    sf = create_session_factory(engine)

    # Controlled HTTP transport simulating OpenAI-compatible API & MemOS Cloud API
    memos_store: list[dict[str, Any]] = []
    memos_writes_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        body = json.loads(request.content.decode("utf-8")) if request.content else {}

        if url.endswith("/chat/completions"):
            if body.get("stream"):
                sse_body = (
                    'data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}\n\n'
                    'data: {"choices":[{"delta":{},"finish_reason":"stop"}],'
                    '"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}\n\n'
                    "data: [DONE]\n\n"
                )
                return httpx.Response(
                    200, text=sse_body, headers={"content-type": "text/event-stream"}
                )
            return httpx.Response(
                200,
                json={
                    "id": "chatcmpl-probe",
                    "choices": [
                        {"message": {"role": "assistant", "content": "OK"}, "finish_reason": "stop"}
                    ],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 1, "total_tokens": 5},
                },
            )

        if url.endswith("/add/message"):
            mem_id = f"memos_{uuid4().hex[:10]}"
            info = body.get("info") or {}
            biz_id = str(info.get("biz_id", ""))
            memos_writes_seen.append(biz_id)
            memos_store.append(
                {
                    "id": mem_id,
                    "user_id": body["user_id"],
                    "memory_value": body["messages"][0]["content"],
                    "tags": body.get("tags", []),
                    "info": info,
                    "create_time": int(datetime.now(UTC).timestamp() * 1000),
                }
            )
            return httpx.Response(
                200,
                json={"code": 0, "data": {"success": True, "task_id": biz_id, "memory_id": mem_id}},
            )

        if url.endswith("/search/memory"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "memory_detail_list": [
                            {**item, "relativity": 0.92}
                            for item in memos_store
                            if item["user_id"] == body["user_id"]
                        ]
                    },
                },
            )

        if url.endswith("/get/memory"):
            uid = body["user_id"]
            # Seed a second record so pagination spans 2 pages when limit=1
            items = [item for item in memos_store if item["user_id"] == uid]
            if len(items) == 1:
                items = [
                    *items,
                    {
                        "id": "memos_extra_page2",
                        "user_id": uid,
                        "memory_value": "second page item",
                        "tags": ["memai:scope:user", "memai:epoch:0"],
                        "info": {},
                        "create_time": int(datetime.now(UTC).timestamp() * 1000) - 1000,
                    },
                ]
            return httpx.Response(
                200,
                json={"code": 0, "data": {"memory_detail_list": items, "total": len(items)}},
            )

        if url.endswith("/get/status"):
            return httpx.Response(200, json={"code": 0, "data": {"status": "COMPLETED"}})

        if url.endswith("/delete/memory"):
            ids = set(body.get("memory_ids") or [])
            memos_store[:] = [item for item in memos_store if item["id"] not in ids]
            return httpx.Response(200, json={"code": 0, "data": {"success": True}})

        return httpx.Response(404, json={"error": "not_found"})

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as mock_http:
            model_adapter = OpenAICompatibleModelProvider(
                http_client=mock_http,
                base_url="https://llm.staging.example/v1",
                api_key=SecretStr("sk-staging-test-key"),
                timeout_seconds=10.0,
            )
            memos_adapter = MemOSMemoryProvider(
                http_client=mock_http,
                session_factory=sf,
                api_key=SecretStr("memos-staging-test-key"),
                base_url="https://memos.staging.example/api/openmem/v1",
                timeout_seconds=10.0,
            )
            token = _make_jwt("staging-probe-user")
            report = await verify_providers(
                prod_settings,
                session_factory=sf,
                check_auth=True,
                auth_headers={"Authorization": f"Bearer {token}"},
                check_model=True,
                model_id="gpt-4o-mini",
                model_provider=model_adapter,
                check_memory=True,
                memory_provider=memos_adapter,
                http_client=mock_http,
            )
            assert report.auth_ok is True
            assert report.model_sync_ok is True
            assert report.model_stream_ok is True
            assert report.model_cancel_ok is True
            assert report.memory_create_status == "succeeded"
            assert report.memory_idempotency_ok is True
            assert report.memory_recall_ok is True
            assert report.memory_pagination_ok is True
            assert report.memory_delete_status == "succeeded"
            assert report.memory_reconcile_status == "succeeded"
            assert report.memory_reconcile_ok is True
            # Two unique probe writes + one repeated write_id idempotency probe
            assert len(memos_writes_seen) == 3
            assert memos_writes_seen[0] == memos_writes_seen[1]
            # Verify --check-auth requires credentials (never silently succeeds)
            with pytest.raises(ValueError, match="auth_headers"):
                await verify_providers(prod_settings, check_auth=True)
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_audit_replay_handles_disabled_user_and_failed_prebackup_tombstone(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    """Verify replay still fences disabled users and re-enqueues failed pre-backup tombstones."""
    archive_path = tmp_path / "edge-replay.jsonl"
    prod_settings = _production_settings(migrated_pg_settings, archive_path=archive_path)
    engine = create_async_engine_from_settings(prod_settings)
    sf = create_session_factory(engine)
    try:
        from mem_ai.modules.operations import service as ops_service
        from mem_ai.modules.operations.audit import DeletionAuditEvent
        from mem_ai.modules.users import service as users_service

        user = await users_service.resolve_or_create_user(
            sf, issuer=_PROD_ISSUER, subject="disabled-replay-user"
        )
        await users_service.set_user_status(sf, user_id=user.id, status="disabled")

        # Seed a pre-backup failed tombstone + failed op for `mem_failed_before_t0`
        async with sf.begin() as session:
            failed_op = await ops_service.create_operation_in_session(
                session,
                owner_id=user.id,
                operation_type="memory_delete",
                status="failed",
                target_type="memory",
                target_id="mem_failed_before_t0",
                error_code="memory_provider_error",
            )
            await session.execute(
                text(
                    "INSERT INTO memory_tombstones "
                    "(id, user_id, provider_id, target_kind, memory_id, memory_epoch, "
                    "operation_id, status) "
                    "VALUES (:id, :uid, 'memos', 'memory', 'mem_failed_before_t0', 0, "
                    ":op_id, 'pending')"
                ),
                {"id": uuid4(), "uid": user.id, "op_id": failed_op.id},
            )

        t0 = datetime.now(UTC) - timedelta(seconds=10)
        archiver = JsonlDeletionAuditArchiver(archive_path)
        await archiver.export_event(
            DeletionAuditEvent(
                event_type="memory_tombstone_created",
                occurred_at=datetime.now(UTC),
                user_id=user.id,
                provider_id="memos",
                target_type="memory",
                target_id="mem_failed_before_t0",
                memory_epoch=0,
            )
        )

        report = await replay_deletion_audit_archive(
            sf,
            archive_path=archive_path,
            settings=prod_settings,
            since=t0,
            dry_run=False,
        )
        assert report.enqueued_count == 1
        assert report.skipped_missing_count == 0
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_v1_public_contract_end_to_end_acceptance(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    """Exercise all V1 public API contracts end-to-end under production JWT & Host rules."""
    prod_settings = _production_settings(
        migrated_pg_settings,
        archive_path=tmp_path / "v1-audit.jsonl",
    )
    memory_provider = FakeMemoryProvider()
    model_provider = FakeModelProvider(prefix="v1:")
    app = create_app(
        prod_settings,
        providers=ProviderRegistry(
            model_provider=model_provider,
            memory_provider=memory_provider,
        ),
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 40020))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://{_PROD_HOST}"
        ) as client:
            headers = _auth_header("v1-acceptance-user")

            # 1. Health & models
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).status_code == 200
            models_resp = await client.get("/v1/models", headers=headers)
            assert models_resp.status_code == 200
            assert models_resp.json()["data"]["items"][0]["modelId"] == "gpt-4o-mini"

            # 2. User identity
            me_resp = await client.get("/v1/users/me", headers=headers)
            assert me_resp.status_code == 200

            # 3. Private Agent create -> patch draft -> create version -> publish -> retire
            ag_resp = await client.post(
                "/v1/agents",
                headers=headers,
                json={
                    "name": "V1 Assistant",
                    "systemPrompt": "Persona v1.",
                    "defaultModelId": "gpt-4o-mini",
                },
            )
            assert ag_resp.status_code == 201
            agent_id = ag_resp.json()["data"]["agentId"]
            assert (
                await client.patch(
                    f"/v1/agents/{agent_id}",
                    headers=headers,
                    json={"systemPrompt": "Persona v2."},
                )
            ).status_code == 200
            ver_resp = await client.post(f"/v1/agents/{agent_id}/versions", headers=headers)
            assert ver_resp.status_code == 201
            v2_id = ver_resp.json()["data"]["versionId"]
            assert (
                await client.post(
                    f"/v1/agents/{agent_id}/publish",
                    headers=headers,
                    json={"versionId": v2_id},
                )
            ).status_code == 200

            # 4. Memories create, cursor list, delete, clear
            m1 = await client.post(
                "/v1/memories", headers=headers, json={"content": "User likes dark roast coffee."}
            )
            assert m1.status_code == 201
            m1_id = m1.json()["data"]["memoryId"]
            m_list = await client.get("/v1/memories?limit=10", headers=headers)
            assert m_list.status_code == 200
            assert any(item["memoryId"] == m1_id for item in m_list.json()["data"]["items"])
            assert (await client.delete(f"/v1/memories/{m1_id}", headers=headers)).status_code in (
                202,
                204,
            )
            assert (
                await client.delete("/v1/memories?confirm=true", headers=headers)
            ).status_code in (202, 204)

            # 5. Sync chat + idempotent replay + generation query
            sync_key = f"v1-sync-{uuid4().hex}"
            chat_sync = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": sync_key},
                json={"agentId": agent_id, "message": "sync hello"},
            )
            assert chat_sync.status_code == 200
            conv_id = chat_sync.json()["data"]["conversationId"]
            gen_id = chat_sync.json()["data"]["generationId"]

            replay = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": sync_key},
                json={"agentId": agent_id, "message": "sync hello", "stream": True},
            )
            assert replay.status_code == 200
            assert replay.json()["data"]["generationId"] == gen_id

            gen_get = await client.get(
                f"/v1/conversations/{conv_id}/generations/{gen_id}",
                headers=headers,
            )
            assert gen_get.status_code == 200
            assert gen_get.json()["data"]["status"] == "succeeded"

            # 6. SSE stream chat
            stream_resp = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": f"v1-sse-{uuid4().hex}"},
                json={
                    "agentId": agent_id,
                    "conversationId": conv_id,
                    "message": "stream hello",
                    "stream": True,
                },
            )
            assert stream_resp.status_code == 200
            events = parse_sse_text(stream_resp.text)
            assert events[0].event == "conversation.started"
            assert any(e.event == "message.delta" for e in events)
            assert events[-1].event == "conversation.completed"

            # 7. Conversations list, detail, messages, delete, and Agent retire
            assert (await client.get("/v1/conversations", headers=headers)).status_code == 200
            assert (
                await client.get(f"/v1/conversations/{conv_id}", headers=headers)
            ).status_code == 200
            msgs = await client.get(f"/v1/conversations/{conv_id}/messages", headers=headers)
            assert msgs.status_code == 200
            assert len(msgs.json()["data"]["items"]) == 4

            conv_del = await client.delete(f"/v1/conversations/{conv_id}", headers=headers)
            assert conv_del.status_code == 202
            conv_op_id = conv_del.json()["data"]["operationId"]
            assert (
                await client.get(f"/v1/operations/{conv_op_id}", headers=headers)
            ).status_code == 200
            assert (
                await client.delete(f"/v1/agents/{agent_id}", headers=headers)
            ).status_code == 200

            # 8. Account deletion + DeletionReceipt query + normal JWT rejected with 403
            user_del = await client.delete("/v1/users/me", headers=headers)
            assert user_del.status_code == 202
            del_data = user_del.json()["data"]
            assert (await client.get("/v1/users/me", headers=headers)).status_code == 403
            receipt_resp = await client.get(
                del_data["statusUrl"],
                headers={
                    "Authorization": f"DeletionReceipt {del_data['deletionReceipt']}",
                    "Host": _PROD_HOST,
                },
            )
            assert receipt_resp.status_code == 200
            assert receipt_resp.json()["data"]["operationType"] == "user_delete"
