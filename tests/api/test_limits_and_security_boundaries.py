"""API and PostgreSQL tests for resource limits, 429/413 envelopes, and security boundaries."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import Request
from pydantic import SecretStr
from sqlalchemy import func, select
from starlette.types import ASGIApp, Receive, Scope, Send

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.core.limits import FixedWindowRateLimiter
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.conversations.models import Generation
from tests.conftest import (
    FakeMemoryProvider,
    FakeModelProvider,
    parse_sse_text,
)


def _with_client_ip(app: ASGIApp, client_ip: str) -> ASGIApp:
    """Wrap an ASGI app to override `scope['client']` for deterministic proxy-IP tests."""

    async def _wrapped(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            scope = dict(scope)
            scope["client"] = (client_ip, 54321)
        await app(scope, receive, send)

    return _wrapped


async def _setup_agent_and_model(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
) -> UUID:
    create_resp = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": "Limits Agent",
            "systemPrompt": "Be concise.",
            "defaultModelId": "test-model",
        },
    )
    assert create_resp.status_code == 201
    agent_id = UUID(create_resp.json()["data"]["agentId"])
    pub_resp = await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)
    assert pub_resp.status_code == 200
    return agent_id


@pytest.mark.anyio
async def test_replaceable_rate_limiter_returns_429_with_retry_after_and_preserves_health() -> None:
    now = [1000.0]
    limiter = FixedWindowRateLimiter(
        max_requests=2,
        window_seconds=12.0,
        scope="edge_test",
        clock=lambda: now[0],
    )
    app = create_app(Settings(_env_file=None, app_env="test"), rate_limiter=limiter)

    @app.get("/_test/ping")
    async def ping() -> dict[str, bool]:
        return {"ok": True}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.get("/_test/ping")).status_code == 200
        assert (await client.get("/_test/ping")).status_code == 200

        limited = await client.get("/_test/ping", headers={"X-Request-Id": "req_rl_1"})
        assert limited.status_code == 429
        assert limited.headers["retry-after"] == "12"
        assert limited.headers["x-request-id"] == "req_rl_1"
        body = limited.json()
        assert body["success"] is False
        assert body["code"] == "rate_limit_exceeded"
        assert body["data"] is None
        assert body["request_id"] == "req_rl_1"
        assert body["details"]["limit_scope"] == "edge_test"
        assert body["details"]["retry_after_seconds"] == 12

        # Operational health probe remains exempt so probes stay healthy under rate limiting.
        live = await client.get("/health/live")
        assert live.status_code == 200


@pytest.mark.anyio
async def test_process_request_concurrency_and_body_size_limits() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        max_concurrent_requests=1,
        max_request_body_bytes=1024,
        rate_limit_retry_after_seconds=4,
    )
    app = create_app(settings)
    entered = asyncio.Event()
    release = asyncio.Event()

    @app.get("/_test/slow")
    async def slow() -> dict[str, bool]:
        entered.set()
        await release.wait()
        return {"done": True}

    @app.post("/_test/echo")
    async def echo(payload: dict[str, Any]) -> dict[str, int]:
        return {"size": len(str(payload))}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Process request concurrency budget
        first_task = asyncio.create_task(client.get("/_test/slow"))
        await asyncio.wait_for(entered.wait(), timeout=2.0)

        rejected = await client.get("/_test/slow", headers={"X-Request-Id": "req_conc_1"})
        assert rejected.status_code == 429
        assert rejected.headers["retry-after"] == "4"
        assert rejected.json()["code"] == "request_concurrency_limit_exceeded"
        assert rejected.json()["details"]["retry_after_seconds"] == 4

        release.set()
        first_resp = await first_task
        assert first_resp.status_code == 200

        # After first completes, concurrency slot is released cleanly.
        after_resp = await client.get("/_test/slow")
        assert after_resp.status_code == 200

        # 2. Request body byte budget via Content-Length
        oversized = {"blob": "x" * 1500}
        too_big = await client.post(
            "/_test/echo",
            json=oversized,
            headers={"X-Request-Id": "req_big_1"},
        )
        assert too_big.status_code == 413
        assert too_big.headers["x-request-id"] == "req_big_1"
        assert too_big.json()["code"] == "payload_too_large"
        assert too_big.json()["details"]["max_request_body_bytes"] == 1024

        # 3. Request body byte budget via chunked stream (no Content-Length)
        async def _chunked_body() -> AsyncIterator[bytes]:
            yield b'{"blob":"'
            yield b"a" * 700
            yield b"b" * 700
            yield b'"}'

        chunked_too_big = await client.post(
            "/_test/echo",
            content=_chunked_body(),
            headers={"Content-Type": "application/json", "X-Request-Id": "req_big_2"},
        )
        assert chunked_too_big.status_code == 413
        assert chunked_too_big.json()["code"] == "payload_too_large"


@pytest.mark.integration
@pytest.mark.anyio
async def test_generation_concurrency_limit_blocks_new_turn_but_allows_idempotent_retry(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _BlockingModelProvider(FakeModelProvider):
        async def generate(self, request: ModelRequest) -> ModelResponse:
            started.set()
            await release.wait()
            return await super().generate(request)

    settings = migrated_pg_settings.model_copy(
        update={
            "max_concurrent_generations": 1,
            "rate_limit_retry_after_seconds": 6,
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
            model_provider=_BlockingModelProvider(),
            memory_provider=FakeMemoryProvider(),
        ),
        credential_verifier=test_auth_adapter,
    )
    alice_headers = {"Authorization": "Bearer token-alice"}

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent_id = await _setup_agent_and_model(client, headers=alice_headers)
            key_1 = f"idem_{uuid4().hex}"
            key_2 = f"idem_{uuid4().hex}"

            first_task = asyncio.create_task(
                client.post(
                    "/v1/chat",
                    headers={**alice_headers, "Idempotency-Key": key_1},
                    json={"agentId": str(agent_id), "message": "hello 1"},
                )
            )
            await asyncio.wait_for(started.wait(), timeout=3.0)

            # New generation on another key hits 429 generation_concurrency_limit_exceeded.
            overflow = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": key_2},
                json={"agentId": str(agent_id), "message": "hello 2"},
            )
            assert overflow.status_code == 429
            assert overflow.headers["retry-after"] == "6"
            assert overflow.json()["code"] == "generation_concurrency_limit_exceeded"

            # Retrying the IN-FLIGHT key_1 must still return 202 (not 429!).
            retry_in_flight = await client.post(
                "/v1/chat",
                headers={**alice_headers, "Idempotency-Key": key_1},
                json={"agentId": str(agent_id), "message": "hello 1"},
            )
            assert retry_in_flight.status_code == 202
            assert retry_in_flight.json()["data"]["status"] == "running"

            # No dangling second generation row was created for key_2.
            async with app.state.session_factory() as session:
                gen_count = (await session.execute(select(func.count(Generation.id)))).scalar_one()
                assert gen_count == 1

            release.set()
            first_resp = await first_task
            assert first_resp.status_code == 200
            assert app.state.generation_concurrency_limiter.active == 0


@pytest.mark.integration
@pytest.mark.anyio
async def test_input_length_and_generation_and_stream_timeouts_converge_via_state_machine(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    class _SlowTimeoutModelProvider:
        async def generate(self, request: ModelRequest) -> ModelResponse:
            _ = request
            await asyncio.sleep(5.0)
            return ModelResponse(model_id="test-model", content="late", usage=ModelUsage())

        async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
            _ = request
            yield ModelStreamChunk(sequence=1, text="part ")
            await asyncio.sleep(5.0)
            yield ModelStreamChunk(sequence=2, text="late", is_final=True)

    settings = migrated_pg_settings.model_copy(
        update={
            "chat_message_max_chars": 20,
            "chat_generation_timeout_seconds": 0.15,
            "chat_stream_timeout_seconds": 0.25,
            "chat_stream_heartbeat_interval_seconds": 0.1,
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
            model_provider=_SlowTimeoutModelProvider(),
            memory_provider=FakeMemoryProvider(),
        ),
        credential_verifier=test_auth_adapter,
    )
    headers = {"Authorization": "Bearer token-alice"}

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            agent_id = await _setup_agent_and_model(client, headers=headers)

            # 1. Input exceeding chat_message_max_chars -> 422
            too_long = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": f"idem_{uuid4().hex}"},
                json={"agentId": str(agent_id), "message": "a" * 25},
            )
            assert too_long.status_code == 422
            assert too_long.json()["code"] == "validation_error"

            # 2. Sync generation timeout -> 504 generation_timeout and converged failed row
            sync_timeout = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": f"idem_{uuid4().hex}"},
                json={"agentId": str(agent_id), "message": "trigger sync"},
            )
            assert sync_timeout.status_code == 504
            assert sync_timeout.json()["code"] == "generation_timeout"

            # 3. Stream duration timeout -> conversation.failed with generation_timeout & DB failed
            stream_resp = await client.post(
                "/v1/chat",
                headers={**headers, "Idempotency-Key": f"idem_{uuid4().hex}"},
                json={"agentId": str(agent_id), "message": "trigger stream", "stream": True},
            )
            assert stream_resp.status_code == 200
            events = parse_sse_text(stream_resp.text)
            started_evt = events[0]
            assert started_evt.event == "conversation.started"
            failed_evts = [e for e in events if e.event == "conversation.failed"]
            assert len(failed_evts) == 1
            assert failed_evts[0].data["code"] == "generation_timeout"
            assert failed_evts[0].data["partial_output_persisted"] is False

            cid = started_evt.data["conversation_id"]
            gid = started_evt.data["generation_id"]
            status_resp = await client.get(
                f"/v1/conversations/{cid}/generations/{gid}",
                headers=headers,
            )
            assert status_resp.status_code == 200
            assert status_resp.json()["data"]["status"] == "failed"
            assert status_resp.json()["data"]["error"]["code"] == "generation_timeout"
            assert app.state.generation_concurrency_limiter.active == 0

            # Verify model timeouts are recorded as `outcome="timeout"`, never `"cancelled"`.
            metrics_resp = await client.get("/metrics")
            assert metrics_resp.status_code == 200
            assert (
                'mem_ai_model_requests_total{mode="sync",model_id="test-model",outcome="timeout"} 1'
            ) in metrics_resp.text
            assert (
                'mem_ai_model_requests_total{mode="stream",model_id="test-model",'
                'outcome="timeout"} 1'
            ) in metrics_resp.text


@pytest.mark.anyio
async def test_allowed_hosts_cors_trusted_proxy_request_id_and_forged_headers() -> None:
    prod_settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_password=SecretStr("strong-prod-password-123"),
        allowed_hosts=["api.example.com", "*.internal.example.com"],
        cors_allowed_origins=["https://app.example.com"],
        auth_mode="trusted_gateway",
        auth_trusted_proxies=["10.0.0.0/8"],
        auth_trusted_issuers=["https://gw.example.com"],
        auth_gateway_shared_secret=SecretStr("gateway-shared-secret-value-at-least-32-chars"),
    )
    app = create_app(
        prod_settings,
        providers=ProviderRegistry(
            model_provider=FakeModelProvider(),
            memory_provider=FakeMemoryProvider(),
        ),
    )

    @app.get("/_test/headers")
    async def inspect_headers(request: Request) -> dict[str, Any]:
        return {
            "forwarded_for": request.headers.get("x-forwarded-for"),
            "user_id": request.headers.get("x-user-id"),
            "gateway_auth": request.headers.get("x-gateway-auth"),
        }

    # 1. Untrusted external peer (203.0.113.9): Host reject, CORS, forged Request-ID & headers
    untrusted_transport = httpx.ASGITransport(app=_with_client_ip(app, "203.0.113.9"))
    async with httpx.AsyncClient(
        transport=untrusted_transport, base_url="http://api.example.com"
    ) as client:
        # Disallowed host -> 400 invalid_host in unified envelope
        bad_host = await client.get("/health/live", headers={"Host": "evil.example.net"})
        assert bad_host.status_code == 400
        assert bad_host.json()["code"] == "invalid_host"
        assert bad_host.json()["success"] is False

        # Duplicate Host headers must also be rejected with 400 invalid_host
        dup_host = await client.get(
            "/health/live",
            headers=[("Host", "api.example.com"), ("Host", "evil.example.net")],
        )
        assert dup_host.status_code == 400
        assert dup_host.json()["code"] == "invalid_host"

        # Wildcard subdomain allowed
        sub_host = await client.get("/health/live", headers={"Host": "svc.internal.example.com"})
        assert sub_host.status_code == 200

        # Forged X-Request-Id from untrusted peer MUST be ignored in production
        forged_rid = await client.get(
            "/health/live",
            headers={"X-Request-Id": "forged_client_id_999"},
        )
        assert forged_rid.status_code == 200
        assert forged_rid.headers["x-request-id"] != "forged_client_id_999"
        assert forged_rid.headers["x-request-id"].startswith("req_")

        # CORS: allowed origin vs disallowed origin
        cors_ok = await client.get(
            "/health/live",
            headers={"Origin": "https://app.example.com"},
        )
        assert cors_ok.headers.get("access-control-allow-origin") == "https://app.example.com"

        cors_denied = await client.get(
            "/health/live",
            headers={"Origin": "https://evil.example.com"},
        )
        assert "access-control-allow-origin" not in cors_denied.headers

        # Forged identity & proxy forwarding headers stripped from untrusted peer
        sanitized = await client.get(
            "/_test/headers",
            headers={
                "X-Forwarded-For": "10.0.0.5",
                "X-User-Id": "spoofed-user",
                "X-Gateway-Auth": "gateway-shared-secret-value-at-least-32-chars",
            },
        )
        assert sanitized.status_code == 200
        assert sanitized.json() == {
            "forwarded_for": None,
            "user_id": None,
            "gateway_auth": None,
        }

    # 2. Trusted proxy peer (10.0.0.42): safe X-Request-Id preserved, unsafe X-Request-Id replaced
    trusted_transport = httpx.ASGITransport(app=_with_client_ip(app, "10.0.0.42"))
    async with httpx.AsyncClient(
        transport=trusted_transport, base_url="http://api.example.com"
    ) as client:
        ok_rid = await client.get(
            "/health/live",
            headers={"X-Request-Id": "gw_req_valid_123"},
        )
        assert ok_rid.status_code == 200
        assert ok_rid.headers["x-request-id"] == "gw_req_valid_123"

        unsafe_rid = await client.get(
            "/health/live",
            headers={"X-Request-Id": "bad id with spaces;drop"},
        )
        assert unsafe_rid.status_code == 200
        assert unsafe_rid.headers["x-request-id"] != "bad id with spaces;drop"
        assert unsafe_rid.headers["x-request-id"].startswith("req_")
