"""API tests for health checks, request IDs, JSON logs, error envelopes, and provider overrides."""

from __future__ import annotations

import json
import logging
from io import StringIO
from typing import Annotated

import httpx
import pytest
from fastapi import Depends, Query

from mem_ai.api.dependencies import (
    get_memory_provider,
    get_model_provider,
    get_persona_provider,
)
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import MemoryProvider
from mem_ai.contracts.model_provider import (
    ModelMessage,
    ModelProvider,
    ModelRequest,
    ModelResponse,
    ModelUsage,
)
from mem_ai.contracts.persona_provider import PersonaProvider
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ConflictError
from mem_ai.core.logging import JsonLogFormatter


@pytest.mark.integration
@pytest.mark.anyio
async def test_health_live_and_ready_with_request_id_and_json_access_log(
    migrated_pg_settings: Settings,
) -> None:
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    app = create_app(migrated_pg_settings)
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                live_resp = await client.get("/health/live")
                assert live_resp.status_code == 200
                live_req_id = live_resp.headers["x-request-id"]
                assert live_req_id.startswith("req_")
                assert live_resp.json() == {
                    "success": True,
                    "code": "ok",
                    "message": "ok",
                    "data": None,
                    "request_id": live_req_id,
                }

                ready_resp = await client.get(
                    "/health/ready",
                    headers={"X-Request-Id": "req_custom_123"},
                )
                assert ready_resp.status_code == 200
                assert ready_resp.headers["x-request-id"] == "req_custom_123"
                assert ready_resp.json() == {
                    "success": True,
                    "code": "ok",
                    "message": "ok",
                    "data": {
                        "database": "ok",
                        "migration_revision": "20261122_0006",
                    },
                    "request_id": "req_custom_123",
                }

        # Lifespan exit must close outbound HTTP client cleanly.
        assert app.state.http_client.is_closed is True
    finally:
        logger.removeHandler(handler)

    log_lines = [json.loads(line) for line in log_buffer.getvalue().splitlines() if line.strip()]
    ready_logs = [entry for entry in log_lines if entry.get("request_id") == "req_custom_123"]
    assert ready_logs, "expected structured JSON log correlated with X-Request-Id"
    assert any(
        entry.get("path") == "/health/ready" and entry.get("status_code") == 200
        for entry in ready_logs
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_health_ready_returns_503_when_database_not_migrated(
    empty_pg_settings: Settings,
) -> None:
    app = create_app(empty_pg_settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health/ready", headers={"X-Request-Id": "req_unmigrated"})

    assert resp.status_code == 503
    payload = resp.json()
    assert payload["success"] is False
    assert payload["code"] == "migration_version_mismatch"
    assert payload["data"] is None
    assert payload["request_id"] == "req_unmigrated"


@pytest.mark.integration
@pytest.mark.anyio
async def test_error_envelopes_for_404_409_422_and_500(
    migrated_pg_settings: Settings,
) -> None:
    app = create_app(migrated_pg_settings)

    @app.get("/_test/conflict")
    async def raise_conflict() -> None:
        raise ConflictError(
            code="conversation_generation_conflict",
            message="conversation already has an active generation",
            details={"conversation_id": "conv_42"},
        )

    @app.get("/_test/validate")
    async def validate_limit(limit: int = Query(..., ge=1, le=100)) -> dict[str, int]:
        return {"limit": limit}

    @app.get("/_test/crash")
    async def raise_unhandled() -> None:
        raise RuntimeError("sensitive internal traceback detail")

    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(JsonLogFormatter())
    logger = logging.getLogger("mem_ai")
    logger.addHandler(handler)

    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                not_found = await client.get(
                    "/v1/does-not-exist", headers={"X-Request-Id": "req_404"}
                )
                assert not_found.status_code == 404
                not_found_body = not_found.json()
                assert not_found_body["success"] is False
                assert not_found_body["code"] == "not_found"
                assert not_found_body["data"] is None
                assert not_found_body["request_id"] == "req_404"

                conflict = await client.get("/_test/conflict", headers={"X-Request-Id": "req_409"})
                assert conflict.status_code == 409
                assert conflict.json() == {
                    "success": False,
                    "code": "conversation_generation_conflict",
                    "message": "conversation already has an active generation",
                    "data": None,
                    "request_id": "req_409",
                    "details": {"conversation_id": "conv_42"},
                }

                invalid = await client.get(
                    "/_test/validate",
                    params={"limit": "secret_raw_input"},
                    headers={"X-Request-Id": "req_422"},
                )
                assert invalid.status_code == 422
                invalid_body = invalid.json()
                assert invalid_body["success"] is False
                assert invalid_body["code"] == "validation_error"
                assert invalid_body["data"] is None
                assert invalid_body["request_id"] == "req_422"
                assert "secret_raw_input" not in json.dumps(invalid_body)

                crash = await client.get("/_test/crash", headers={"X-Request-Id": "req_500"})
                assert crash.status_code == 500
                assert crash.headers["x-request-id"] == "req_500"
                crash_body = crash.json()
                assert crash_body["success"] is False
                assert crash_body["code"] == "internal_server_error"
                assert crash_body["data"] is None
                assert crash_body["request_id"] == "req_500"
                assert "sensitive internal traceback" not in json.dumps(crash_body)
    finally:
        logger.removeHandler(handler)

    error_logs = [
        json.loads(line)
        for line in log_buffer.getvalue().splitlines()
        if line.strip() and json.loads(line).get("logger") == "mem_ai.errors"
    ]
    assert any(
        entry.get("request_id") == "req_500" and entry.get("error_code") == "internal_server_error"
        for entry in error_logs
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_provider_assembly_and_dependency_override_seam(
    migrated_pg_settings: Settings,
    fake_model_provider: ModelProvider,
    fake_memory_provider: MemoryProvider,
    fake_persona_provider: PersonaProvider,
) -> None:
    app = create_app(migrated_pg_settings)

    @app.get("/_test/providers")
    async def inspect_providers(
        model: Annotated[ModelProvider, Depends(get_model_provider)],
        memory: Annotated[MemoryProvider, Depends(get_memory_provider)],
        persona: Annotated[PersonaProvider, Depends(get_persona_provider)],
    ) -> dict[str, str]:
        resp = await model.generate(
            ModelRequest(
                model_id="test-model",
                messages=[ModelMessage(role="user", content="ping")],
            )
        )
        assert isinstance(persona, PersonaProvider)
        return {"reply": resp.content, "memory_provider_id": memory.provider_id}

    app.dependency_overrides[get_model_provider] = lambda: fake_model_provider
    app.dependency_overrides[get_memory_provider] = lambda: fake_memory_provider
    app.dependency_overrides[get_persona_provider] = lambda: fake_persona_provider

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/_test/providers")
            assert resp.status_code == 200
            assert resp.json() == {
                "reply": "fake:ping",
                "memory_provider_id": "fake-memory",
            }

            class _OverrideModelProvider:
                async def generate(self, request: ModelRequest) -> ModelResponse:
                    return ModelResponse(
                        model_id=request.model_id,
                        content="overridden",
                        usage=ModelUsage(),
                    )

                def stream_generate(self, request: ModelRequest):  # type: ignore[no-untyped-def]
                    raise NotImplementedError

            app.dependency_overrides[get_model_provider] = lambda: _OverrideModelProvider()
            resp2 = await client.get("/_test/providers")
            assert resp2.status_code == 200
            assert resp2.json()["reply"] == "overridden"


@pytest.mark.integration
@pytest.mark.anyio
async def test_shared_async_client_fixture_and_main_asgi_entrypoint(
    async_client: httpx.AsyncClient,
) -> None:
    from mem_ai.main import app as asgi_app

    assert asgi_app.title == "mem-ai"

    resp = await async_client.get("/health/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["code"] == "ok"
    assert body["data"] == {
        "database": "ok",
        "migration_revision": "20261122_0006",
    }


@pytest.mark.anyio
async def test_health_ready_returns_503_when_database_unreachable() -> None:
    unreachable = Settings(
        _env_file=None,
        app_env="test",
        postgres_host="127.0.0.1",
        postgres_port=1,
        db_connect_timeout_seconds=1.0,
        db_health_check_timeout_seconds=1.0,
    )
    app = create_app(unreachable)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health/ready", headers={"X-Request-Id": "req_down"})

    assert resp.status_code == 503
    payload = resp.json()
    assert payload["success"] is False
    assert payload["code"] == "database_unavailable"
    assert payload["data"] is None
    assert payload["request_id"] == "req_down"


@pytest.mark.anyio
async def test_api_docs_serve_local_offline_assets_without_external_cdn() -> None:
    app = create_app(Settings(_env_file=None, app_env="development"))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        docs_resp = await client.get("/docs")
        assert docs_resp.status_code == 200
        assert "/static/docs/swagger-ui-bundle.js" in docs_resp.text
        assert "/static/docs/swagger-ui.css" in docs_resp.text
        assert "/static/docs/favicon.png" in docs_resp.text
        assert "cdn.jsdelivr.net" not in docs_resp.text
        assert "fastapi.tiangolo.com" not in docs_resp.text

        redoc_resp = await client.get("/redoc")
        assert redoc_resp.status_code == 200
        assert "/static/docs/redoc.standalone.js" in redoc_resp.text
        assert "/static/docs/favicon.png" in redoc_resp.text
        assert "cdn.jsdelivr.net" not in redoc_resp.text
        assert "fonts.googleapis.com" not in redoc_resp.text
        assert "fastapi.tiangolo.com" not in redoc_resp.text

        for asset in (
            "/static/docs/swagger-ui-bundle.js",
            "/static/docs/swagger-ui.css",
            "/static/docs/redoc.standalone.js",
            "/static/docs/favicon.png",
        ):
            asset_resp = await client.get(asset)
            assert asset_resp.status_code == 200
            assert len(asset_resp.content) > 0


@pytest.mark.anyio
async def test_api_docs_are_disabled_in_production() -> None:
    prod_settings = Settings(
        _env_file=None,
        app_env="production",
        postgres_password="strong-prod-password-123",
        auth_mode="jwt_hs256",
        auth_jwt_hs256_secret="super-secret-jwt-signing-key-at-least-32-chars",
        auth_trusted_issuers=["https://idp.example.com"],
        auth_trusted_audience="mem-ai-api",
    )
    app = create_app(prod_settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            resp = await client.get(path)
            assert resp.status_code == 404
