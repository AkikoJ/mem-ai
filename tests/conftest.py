"""Shared pytest fixtures for PostgreSQL isolation, FastAPI app, HTTP client, and providers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy import text

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.api.pagination import CursorPayload, decode_cursor, encode_cursor, hash_filters
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryListPage,
    MemoryListQuery,
    MemoryOperationStatus,
    MemoryRecallItem,
    MemoryRecallQuery,
    MemoryRecord,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.contracts.model_provider import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ModelUsage,
)
from mem_ai.core.config import Settings
from mem_ai.db.engine import create_sync_engine_from_settings
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.auth.context import VerifiedPrincipal


@dataclass(frozen=True)
class ParsedSseEvent:
    """Decoded SSE frame used across unit, integration, and API streaming tests."""

    event_id: str | None
    event: str
    data: dict[str, Any]


def parse_sse_text(raw: str) -> list[ParsedSseEvent]:
    """Parse one or more SSE frames separated by blank lines into structured events."""
    events: list[ParsedSseEvent] = []
    for block in raw.split("\n\n"):
        if not block.strip():
            continue
        event_id: str | None = None
        event_name: str | None = None
        data_lines: list[str] = []
        for line in block.split("\n"):
            if line.startswith("id: "):
                event_id = line[4:]
            elif line.startswith("event: "):
                event_name = line[7:]
            elif line.startswith("data: "):
                data_lines.append(line[6:])
            else:
                raise AssertionError(f"Unexpected SSE line: {line!r}")
        assert event_name is not None, f"Missing event name in SSE block: {block!r}"
        assert len(data_lines) == 1, f"Expected single data line in SSE block: {block!r}"
        payload = json.loads(data_lines[0])
        assert isinstance(payload, dict)
        events.append(ParsedSseEvent(event_id=event_id, event=event_name, data=payload))
    return events


class FakeModelProvider:
    """Controlled in-memory ModelProvider double for deterministic tests."""

    def __init__(self, *, prefix: str = "fake:") -> None:
        self.prefix = prefix
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        last_content = request.messages[-1].content
        return ModelResponse(
            model_id=request.model_id,
            content=f"{self.prefix}{last_content}",
            usage=ModelUsage(input_tokens=5, output_tokens=3, total_tokens=8),
            finish_reason="stop",
        )

    async def stream_generate(self, request: ModelRequest) -> AsyncIterator[ModelStreamChunk]:
        self.requests.append(request)
        last_content = request.messages[-1].content
        yield ModelStreamChunk(sequence=1, text=self.prefix)
        yield ModelStreamChunk(
            sequence=2,
            text=last_content,
            is_final=True,
            usage=ModelUsage(input_tokens=5, output_tokens=3, total_tokens=8),
            finish_reason="stop",
        )


class FakeMemoryProvider:
    """Controlled in-memory MemoryProvider double for deterministic tests."""

    provider_id = "fake-memory"

    def __init__(self) -> None:
        self.records: dict[str, MemoryRecord] = {}

    async def recall(self, query: MemoryRecallQuery) -> list[MemoryRecallItem]:
        matched: list[MemoryRecallItem] = []
        for record in self.records.values():
            if record.user_id != query.user_id or record.scope != query.scope:
                continue
            if query.scope == "conversation" and record.conversation_id != query.conversation_id:
                continue
            matched.append(
                MemoryRecallItem(
                    memory_id=record.memory_id,
                    provider_id=self.provider_id,
                    scope=record.scope,
                    content=record.content,
                    relevance=0.9,
                    conversation_id=record.conversation_id,
                    memory_epoch=record.memory_epoch,
                )
            )
        return matched[: query.limit]

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        memory_id = f"mem_{uuid4().hex[:12]}"
        self.records[memory_id] = MemoryRecord(
            memory_id=memory_id,
            provider_id=self.provider_id,
            user_id=item.user_id,
            scope=item.scope,
            content=item.content,
            conversation_id=item.conversation_id,
            memory_epoch=item.memory_epoch,
            created_at=datetime.now(UTC),
        )
        return MemoryWriteResult(
            status="succeeded",
            provider_id=self.provider_id,
            memory_id=memory_id,
            external_ref=item.write_id,
        )

    async def write_turn_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        return await self.create_memory(item)

    async def list_memories(self, query: MemoryListQuery) -> MemoryListPage:
        filters_hash = hash_filters(
            {
                "user_id": str(query.user_id),
                "provider_id": self.provider_id,
                "scope": query.scope or "",
                "conversation_id": str(query.conversation_id) if query.conversation_id else "",
            }
        )
        items = [
            record
            for record in self.records.values()
            if record.user_id == query.user_id
            and (query.scope is None or record.scope == query.scope)
            and (query.conversation_id is None or record.conversation_id == query.conversation_id)
        ]
        items.sort(key=lambda r: (r.created_at, r.memory_id), reverse=True)
        if query.cursor is not None:
            payload = decode_cursor(
                query.cursor,
                expected_direction="desc",
                expected_filters_hash=filters_hash,
            )
            cursor_key = (payload.created_at, str(payload.id))
            items = [r for r in items if (r.created_at, r.memory_id) < cursor_key]

        sliced = items[: query.limit]
        has_more = len(items) > query.limit
        next_cursor = (
            encode_cursor(
                CursorPayload(
                    created_at=sliced[-1].created_at,
                    id=sliced[-1].memory_id,
                    direction="desc",
                    filters_hash=filters_hash,
                )
            )
            if has_more and sliced
            else None
        )
        return MemoryListPage(
            items=sliced,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        record = self.records.get(memory_id)
        if record is None or record.user_id != user_id:
            return MemoryDeleteResult(
                status="failed",
                external_ref=memory_id,
                error_code="memory_not_found",
            )
        del self.records[memory_id]
        return MemoryDeleteResult(status="succeeded", external_ref=memory_id)

    async def clear_memories(self, *, user_id: UUID) -> MemoryDeleteResult:
        to_delete = [mid for mid, rec in self.records.items() if rec.user_id == user_id]
        for mid in to_delete:
            del self.records[mid]
        return MemoryDeleteResult(status="succeeded", external_ref=str(user_id))

    async def verify_operation(
        self,
        *,
        user_id: UUID,
        external_ref: str,
    ) -> MemoryOperationStatus:
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


def make_alembic_config(settings: Settings) -> Config:
    """Build an Alembic Config pointing at an isolated test database."""
    config = Config("alembic.ini")
    config.attributes["settings"] = settings
    config.attributes["skip_logging_config"] = True
    return config


@pytest.fixture
def anyio_backend() -> str:
    """Run all anyio async tests on asyncio (required by psycopg 3 async)."""
    return "asyncio"


@pytest.fixture
def empty_pg_settings() -> Iterator[Settings]:
    """Create and drop an isolated empty PostgreSQL database per test."""
    base_settings = Settings()
    test_db = f"mem_ai_test_{uuid4().hex[:12]}"
    admin_engine = create_sync_engine_from_settings(base_settings)
    try:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'CREATE DATABASE "{test_db}"'))
        yield Settings(
            _env_file=None,
            app_env="test",
            postgres_host=base_settings.postgres_host,
            postgres_port=base_settings.postgres_port,
            postgres_user=base_settings.postgres_user,
            postgres_password=base_settings.postgres_password,
            postgres_db=test_db,
            worker_loop_enabled=False,
        )
    finally:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{test_db}" WITH (FORCE)'))
        admin_engine.dispose()


@pytest.fixture
def migrated_pg_settings(empty_pg_settings: Settings) -> Settings:
    """Provide an isolated PostgreSQL database upgraded to Alembic head."""
    command.upgrade(make_alembic_config(empty_pg_settings), "head")
    return empty_pg_settings


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_hs256_jwt(
    payload: dict[str, Any],
    *,
    secret: str,
    header: dict[str, Any] | None = None,
) -> str:
    """Sign an HS256 JWT token for auth unit and API tests."""
    header_obj = header if header is not None else {"alg": "HS256", "typ": "JWT"}
    header_seg = _b64url(json.dumps(header_obj, separators=(",", ":")).encode("utf-8"))
    payload_seg = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_seg}.{payload_seg}".encode("ascii")
    sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_seg}.{payload_seg}.{_b64url(sig)}"


def make_test_principal(
    subject: str = "test-user",
    *,
    issuer: str = "https://test.mem-ai.local",
    actor_id: str | None = None,
    auth_method: str = "test_token",
    permissions: tuple[str, ...] = (
        "chat:use",
        "agents:manage",
        "memories:manage",
        "users:self",
    ),
) -> VerifiedPrincipal:
    """Build a deterministic verified principal for API and integration tests."""
    return VerifiedPrincipal(
        issuer=issuer,
        subject=subject,
        actor_id=actor_id if actor_id is not None else f"{issuer}:{subject}",
        auth_method=auth_method,
        permissions=frozenset(permissions),
    )


@pytest.fixture
def test_auth_adapter() -> StaticTokenAuthAdapter:
    """Provide a two-subject Bearer token adapter (`token-alice`, `token-bob`) for tests."""
    return StaticTokenAuthAdapter(
        {
            "token-alice": make_test_principal("alice"),
            "token-bob": make_test_principal("bob"),
        }
    )


@pytest.fixture
def fake_model_provider() -> FakeModelProvider:
    return FakeModelProvider()


@pytest.fixture
def fake_memory_provider() -> FakeMemoryProvider:
    return FakeMemoryProvider()


@pytest.fixture
def app(
    migrated_pg_settings: Settings,
    fake_model_provider: FakeModelProvider,
    fake_memory_provider: FakeMemoryProvider,
) -> FastAPI:
    """Construct a FastAPI app wired with isolated PostgreSQL and controlled providers."""
    return create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(
            model_provider=fake_model_provider,
            memory_provider=fake_memory_provider,
        ),
    )


@pytest.fixture
async def async_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """Provide an HTTPX AsyncClient inside the FastAPI application lifespan."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
