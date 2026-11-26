"""API and PostgreSQL tests for /metrics access control, telemetry, logs, tracing, and Worker."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import update
from starlette.types import ASGIApp, Receive, Scope, Send

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import MemoryRecallItem, MemoryRecallQuery
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import BadGatewayError
from mem_ai.core.logging import JsonLogFormatter
from mem_ai.core.telemetry import InMemoryTracer, TelemetryRegistry
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.conversations import generation as conversations_generation
from mem_ai.modules.conversations.models import Generation
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import JobExecutionContext, JobHandlerOutcome
from mem_ai.workers.runner import JobHandlerRegistry, WorkerConfig, WorkerRunner
from tests.conftest import FakeMemoryProvider, FakeModelProvider, parse_sse_text


def _with_client_ip(app: ASGIApp, client_ip: str) -> ASGIApp:
    async def _wrapped(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            scope = dict(scope)
            scope["client"] = (client_ip, 54321)
        await app(scope, receive, send)

    return _wrapped


@pytest.mark.anyio
async def test_metrics_endpoint_access_control_by_network_token_and_toggle() -> None:
    # 1. Network allowlist + Bearer token protection
    protected_settings = Settings(
        _env_file=None,
        app_env="test",
        metrics_enabled=True,
        metrics_allowed_networks=["127.0.0.1/32", "10.0.0.0/8"],
        metrics_bearer_token=SecretStr("scrape-secret-token-for-prometheus-tests"),
    )
    app = create_app(protected_settings)

    # Untrusted external IP -> 403 metrics_access_denied
    external_transport = httpx.ASGITransport(app=_with_client_ip(app, "203.0.113.55"))
    async with httpx.AsyncClient(transport=external_transport, base_url="http://test") as client:
        denied = await client.get("/metrics")
        assert denied.status_code == 403
        assert denied.json()["code"] == "metrics_access_denied"

    # Allowed internal IP without token -> 401 invalid_metrics_credentials
    internal_transport = httpx.ASGITransport(app=_with_client_ip(app, "10.0.1.7"))
    async with httpx.AsyncClient(transport=internal_transport, base_url="http://test") as client:
        no_token = await client.get("/metrics")
        assert no_token.status_code == 401
        assert no_token.json()["code"] == "invalid_metrics_credentials"

        bad_token = await client.get("/metrics", headers={"Authorization": "Bearer wrong"})
        assert bad_token.status_code == 401

        ok_resp = await client.get(
            "/metrics",
            headers={"Authorization": "Bearer scrape-secret-token-for-prometheus-tests"},
        )
        assert ok_resp.status_code == 200
        assert ok_resp.headers["content-type"].startswith("text/plain; version=0.0.4")
        assert ok_resp.headers["cache-control"] == "no-store"

    # 2. Disabled metrics -> 404
    disabled_app = create_app(Settings(_env_file=None, app_env="test", metrics_enabled=False))
    transport = httpx.ASGITransport(app=disabled_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/metrics")
        assert resp.status_code == 404


@pytest.mark.integration
@pytest.mark.anyio
async def test_metrics_logs_and_tracing_cover_chat_sse_degradation_lease_jobs_and_barriers(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    class _DegradingMemoryProvider(FakeMemoryProvider):
        def __init__(self) -> None:
            super().__init__()
            self.fail_recall = False

        async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
            if self.fail_recall:
                raise BadGatewayError(code="memory_provider_unavailable", message="upstream down")
            return await super().recall(query)

    memory_provider = _DegradingMemoryProvider()
    telemetry = TelemetryRegistry()
    tracer = InMemoryTracer()
    settings = migrated_pg_settings.model_copy(
        update={
            "model_registry_entries": [
                {
                    "model_id": "test-model",
                    "provider_id": "fake",
                    "upstream_model": "test-model",
                    "enabled": True,
                    "supports_streaming": True,
                }
            ],
        }
    )
    app = create_app(
        settings,
        providers=ProviderRegistry(
            model_provider=FakeModelProvider(),
            memory_provider=memory_provider,
        ),
        credential_verifier=test_auth_adapter,
        telemetry=telemetry,
        tracer=tracer,
    )

    log_buffer = StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger("mem_ai")
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)

    headers = {"Authorization": "Bearer token-alice"}
    secret_user_text = "PRIVATE_DIARY_CONTENT_SHOULD_NEVER_LOG_98765"

    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                # 1. Create & publish agent
                create_resp = await client.post(
                    "/v1/agents",
                    headers=headers,
                    json={
                        "name": "Observed Agent",
                        "systemPrompt": "Reply briefly.",
                        "defaultModelId": "test-model",
                    },
                )
                assert create_resp.status_code == 201
                agent_id = UUID(create_resp.json()["data"]["agentId"])
                assert (
                    await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
                ).status_code == 200

                # 2. Sync chat with degrading memory recall -> succeeds, degrades memory
                memory_provider.fail_recall = True
                sync_resp = await client.post(
                    "/v1/chat",
                    headers={
                        **headers,
                        "Idempotency-Key": f"idem_{uuid4().hex}",
                        "X-Request-Id": "req_obs_sync",
                    },
                    json={"agentId": str(agent_id), "message": secret_user_text},
                )
                assert sync_resp.status_code == 200
                conv_id = sync_resp.json()["data"]["conversationId"]
                gen_id = sync_resp.json()["data"]["generationId"]

                # Readiness stays 200 even while memory provider is degraded (per §12.2)
                ready_resp = await client.get("/health/ready")
                assert ready_resp.status_code == 200

                # 3. Streaming chat with healthy memory recall -> captures TTFT & stream duration
                memory_provider.fail_recall = False
                stream_resp = await client.post(
                    "/v1/chat",
                    headers={
                        **headers,
                        "Idempotency-Key": f"idem_{uuid4().hex}",
                        "X-Request-Id": "req_obs_stream",
                    },
                    json={
                        "agentId": str(agent_id),
                        "conversationId": conv_id,
                        "message": "stream ping",
                        "stream": True,
                    },
                )
                assert stream_resp.status_code == 200
                events = parse_sse_text(stream_resp.text)
                assert any(e.event == "conversation.completed" for e in events)

                # 4. Trigger memory clear to observe memory barrier duration histogram
                clear_resp = await client.delete(
                    "/v1/memories",
                    params={"confirm": "true"},
                    headers=headers,
                )
                assert clear_resp.status_code in (202, 204)

                # 5. Seed an expired running generation and recover via shared recovery entry
                async with app.state.session_factory.begin() as session:
                    stale_gen = Generation(
                        conversation_id=UUID(conv_id),
                        owner_id=UUID(
                            (await client.get("/v1/users/me", headers=headers)).json()["data"][
                                "userId"
                            ]
                        ),
                        agent_id=agent_id,
                        persona_version_id=UUID(
                            (await client.get(f"/v1/agents/{agent_id}", headers=headers)).json()[
                                "data"
                            ]["currentPublishedVersionId"]
                        ),
                        model_id="test-model",
                        status="running",
                        lease_token="genlease_expired_test",
                        lease_expires_at=datetime.now(UTC) - timedelta(minutes=5),
                    )
                    session.add(stale_gen)
                recovered = await conversations_generation.recover_expired_generations_phase(
                    app.state.session_factory
                )
                assert len(recovered) == 1

                # 6. Seed a reconciling job so backlog/oldest_wait/reconciling gauges are non-zero
                async with app.state.session_factory.begin() as session:
                    job, _ = await jobs_service.enqueue_job_in_session(
                        session,
                        job_type="memory_turn_write",
                        business_id="obs_test",
                        user_id=None,
                        dedupe_key=f"obs_{uuid4().hex}",
                        metadata_json={},
                        next_run_at=datetime.now(UTC) - timedelta(seconds=30),
                    )
                async with app.state.session_factory.begin() as session:
                    from mem_ai.modules.jobs.models import Job

                    await session.execute(
                        update(Job).where(Job.id == job.id).values(status="reconciling")
                    )

                # 7. Scrape /metrics and assert comprehensive low-cardinality coverage
                metrics_resp = await client.get("/metrics")
                assert metrics_resp.status_code == 200
                text = metrics_resp.text

                assert 'mem_ai_http_requests_total{method="POST",route="/v1/chat"' in text
                assert (
                    'mem_ai_http_request_duration_seconds_count{method="POST",route="/v1/chat"}'
                    in text
                )
                assert (
                    'mem_ai_model_request_duration_seconds_count{mode="sync",model_id="test-model",'
                    'outcome="succeeded"} 1'
                ) in text
                assert (
                    'mem_ai_model_request_duration_seconds_count{mode="stream",'
                    'model_id="test-model",outcome="succeeded"} 1'
                ) in text
                assert (
                    'mem_ai_model_first_token_duration_seconds_count{model_id="test-model"} 1'
                    in text
                )
                assert (
                    'mem_ai_generations_total{error_code="none",mode="sync",outcome="succeeded"} 1'
                ) in text
                assert (
                    'mem_ai_generations_total{error_code="none",mode="stream",'
                    'outcome="succeeded"} 1'
                ) in text
                assert (
                    'mem_ai_memory_degradations_total{provider="fake-memory",'
                    'reason="memory_provider_unavailable"} 1'
                ) in text
                assert 'mem_ai_memory_barrier_duration_seconds_count{outcome="succeeded",' in text
                assert 'mem_ai_jobs_count{status="reconciling"} 1' in text
                assert "mem_ai_jobs_reconciling 1" in text
                assert "mem_ai_jobs_oldest_wait_seconds " in text
                assert "mem_ai_jobs_last_success_timestamp_seconds " in text
                assert "mem_ai_generations_running 0" in text
                assert 'mem_ai_memory_barriers_active{barrier_state="clearing"} 0' in text
                assert "mem_ai_memory_barrier_oldest_duration_seconds 0" in text

                # Business IDs MUST NOT appear anywhere in Prometheus metrics exposition
                assert conv_id not in text
                assert gen_id not in text
                assert "token-alice" not in text
                assert secret_user_text not in text
    finally:
        root_logger.removeHandler(log_handler)

    # Verify structured JSON logs correlate request_id, user_id, conversation_id, generation_id
    # and NEVER leak message body or Bearer token.
    raw_logs = log_buffer.getvalue()
    assert secret_user_text not in raw_logs
    assert "token-alice" not in raw_logs
    entries = [json.loads(line) for line in raw_logs.splitlines() if line.strip()]
    sync_entries = [e for e in entries if e.get("request_id") == "req_obs_sync"]
    assert any(
        e.get("conversation_id") == conv_id
        and e.get("generation_id") == gen_id
        and e.get("user_id")
        for e in sync_entries
    )

    # Verify OpenTelemetry tracing seam recorded expected spans with no raw content
    span_names = {s.name for s in tracer.spans}
    expected_spans = {
        "http.request",
        "chat.generation",
        "model.generate",
        "model.stream_generate",
        "memory.recall",
    }
    assert expected_spans <= span_names


@pytest.mark.integration
@pytest.mark.anyio
async def test_readiness_checks_startup_lifecycle_and_compatible_migration_revisions(
    migrated_pg_settings: Settings,
) -> None:
    settings = migrated_pg_settings.model_copy(
        update={
            "expected_alembic_revision": "99999999_future",
            "compatible_alembic_revisions": ["20261126_0013"],
        }
    )
    app = create_app(settings)

    async with app.router.lifespan_context(app):
        assert app.state.ready is True
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # Compatible revision list allows rolling upgrade compatibility.
            ok_ready = await client.get("/health/ready")
            assert ok_ready.status_code == 200
            assert ok_ready.json()["data"]["migration_revision"] == "20261126_0013"

            # Simulated pre-ready / shutting-down state returns 503 service_not_ready.
            app.state.ready = False
            draining_ready = await client.get("/health/ready")
            assert draining_ready.status_code == 503
            assert draining_ready.json()["code"] == "service_not_ready"

    assert app.state.ready is False
    assert app.state.http_client.is_closed is True


@pytest.mark.integration
@pytest.mark.anyio
async def test_worker_heartbeat_shared_metrics_and_graceful_shutdown(
    migrated_pg_settings: Settings,
    tmp_path: Path,
) -> None:
    heartbeat_path = tmp_path / "worker-heartbeat.json"
    obs_settings = migrated_pg_settings.model_copy(
        update={
            "metrics_enabled": True,
            "worker_loop_enabled": False,
        }
    )
    tracer = InMemoryTracer()
    telemetry = TelemetryRegistry()
    app = create_app(obs_settings, telemetry=telemetry, tracer=tracer)

    done_event = asyncio.Event()
    slow_started = asyncio.Event()

    class _FastHandler:
        async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
            _ = ctx
            done_event.set()
            return JobHandlerOutcome(outcome="succeeded", result_summary={"ok": True})

    class _SlowStragglerHandler:
        async def handle(self, ctx: JobExecutionContext) -> JobHandlerOutcome:
            _ = ctx
            slow_started.set()
            await asyncio.sleep(30.0)
            return JobHandlerOutcome(outcome="succeeded")

    registry = JobHandlerRegistry()
    registry.register("obs_fast", _FastHandler())
    registry.register("obs_slow", _SlowStragglerHandler())

    async with app.router.lifespan_context(app):
        session_factory = app.state.session_factory
        runner = WorkerRunner(
            session_factory=session_factory,
            registry=registry,
            config=WorkerConfig(
                worker_id="worker-obs-test",
                concurrency=2,
                poll_interval_seconds=0.05,
                lease_ttl_seconds=10.0,
                lease_renew_interval_seconds=5.0,
                shutdown_grace_seconds=0.15,
                heartbeat_interval_seconds=0.05,
                heartbeat_file=str(heartbeat_path),
            ),
            telemetry=telemetry,
            tracer=tracer,
        )

        async with session_factory.begin() as session:
            fast_job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="obs_fast",
                business_id="fast-1",
                user_id=None,
                dedupe_key=f"fast_{uuid4().hex}",
                metadata_json={"provider_id": "fake-memory"},
            )
            slow_job, _ = await jobs_service.enqueue_job_in_session(
                session,
                job_type="obs_slow",
                business_id="slow-1",
                user_id=None,
                dedupe_key=f"slow_{uuid4().hex}",
                metadata_json={},
            )

        stop_event = asyncio.Event()
        worker_task = asyncio.create_task(runner.run_forever(stop_event=stop_event))
        await asyncio.wait_for(done_event.wait(), timeout=3.0)
        await asyncio.wait_for(slow_started.wait(), timeout=3.0)
        for _ in range(50):
            if runner.health_snapshot().succeeded_count >= 1:
                break
            await asyncio.sleep(0.02)

        # Verify atomic heartbeat JSON file was written with live fields
        assert heartbeat_path.exists()
        hb_payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        assert hb_payload["worker_id"] == "worker-obs-test"
        assert hb_payload["last_poll_at"] is not None
        assert hb_payload["last_claim_at"] is not None
        assert hb_payload["shutting_down"] is False

        # Verify unified application exposes /health/live, /health/ready, and shared /metrics
        transport = httpx.ASGITransport(app=_with_client_ip(app, "127.0.0.1"))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            live = await client.get("/health/live")
            assert live.status_code == 200
            assert live.json()["request_id"].startswith("req_")

            ready = await client.get("/health/ready")
            assert ready.status_code == 200

            metrics = await client.get("/metrics")
            assert metrics.status_code == 200
            assert (
                'mem_ai_worker_jobs_processed_total{job_type="obs_fast",status="succeeded"} 1'
                in metrics.text
            )
            assert "mem_ai_worker_last_heartbeat_timestamp_seconds " in metrics.text
            assert "mem_ai_worker_last_success_timestamp_seconds " in metrics.text
            assert "mem_ai_jobs_last_success_timestamp_seconds " in metrics.text

        # Graceful shutdown stops claiming, drains within grace, and leaves straggler recoverable.
        stop_event.set()
        await asyncio.wait_for(worker_task, timeout=3.0)

        final_hb = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        assert final_hb["shutting_down"] is True

        fast_row = await jobs_service.get_job(session_factory, job_id=fast_job.id)
        slow_row = await jobs_service.get_job(session_factory, job_id=slow_job.id)
        assert fast_row is not None and fast_row.status == "succeeded"
        assert slow_row is not None and slow_row.status == "running"
        assert any(s.name == "worker.job" for s in tracer.spans)
