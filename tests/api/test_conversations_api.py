"""Owned conversation history and resumable deletion through HTTP and PostgreSQL workers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from mem_ai.api.dependencies import ProviderRegistry
from mem_ai.application import create_app
from mem_ai.contracts.memory_provider import (
    MemoryDeleteResult,
    MemoryOperationStatus,
    MemoryWriteInput,
    MemoryWriteResult,
)
from mem_ai.core.config import Settings
from mem_ai.core.exceptions import BadGatewayError
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigSeed
from mem_ai.workers.main import build_worker_registry
from mem_ai.workers.runner import WorkerConfig, WorkerRunner
from tests.api.test_chat_idempotency_and_generations_api import _ControllableModelProvider
from tests.conftest import FakeMemoryProvider

ALICE = {"Authorization": "Bearer token-alice"}
BOB = {"Authorization": "Bearer token-bob"}


class GatedMemoryProvider(FakeMemoryProvider):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.defer_delete = False
        self.allow_verification = False
        self.pending_deletes: dict[str, tuple[UUID, str]] = {}

    async def create_memory(self, item: MemoryWriteInput) -> MemoryWriteResult:
        if item.content == "gated":
            self.started.set()
            await self.release.wait()
        return await super().create_memory(item)

    async def delete_memory(self, *, user_id: UUID, memory_id: str) -> MemoryDeleteResult:
        if self.defer_delete:
            ref = f"deferred:{memory_id}"
            self.pending_deletes[ref] = (user_id, memory_id)
            return MemoryDeleteResult(status="reconciling", external_ref=ref)
        return await super().delete_memory(user_id=user_id, memory_id=memory_id)

    async def verify_operation(self, *, user_id: UUID, external_ref: str) -> MemoryOperationStatus:
        if external_ref in self.pending_deletes:
            if not self.allow_verification:
                return MemoryOperationStatus(status="reconciling", external_ref=external_ref)
            owner, memory_id = self.pending_deletes.pop(external_ref)
            assert owner == user_id
            await super().delete_memory(user_id=user_id, memory_id=memory_id)
        return MemoryOperationStatus(status="succeeded", external_ref=external_ref)


@pytest.fixture
async def conversation_app(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> AsyncIterator[
    tuple[FastAPI, httpx.AsyncClient, _ControllableModelProvider, FakeMemoryProvider]
]:
    model = _ControllableModelProvider()
    memory = GatedMemoryProvider()
    app = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(model_provider=model, memory_provider=memory),
        credential_verifier=test_auth_adapter,
    )
    async with app.router.lifespan_context(app):
        await models_service.sync_model_configs(
            app.state.session_factory,
            seeds=[
                ModelConfigSeed(
                    model_id="gpt-4o-mini",
                    provider_id="openai_compatible",
                    upstream_model="gpt-4o-mini",
                    display_name="Test model",
                    enabled=True,
                )
            ],
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client, model, memory


async def create_agent(client: httpx.AsyncClient, headers: dict[str, str] = ALICE) -> str:
    response = await client.post(
        "/v1/agents",
        headers=headers,
        json={
            "name": "History",
            "systemPrompt": "Be helpful",
            "defaultModelId": "gpt-4o-mini",
        },
    )
    assert response.status_code == 201
    agent_id: str = response.json()["data"]["agentId"]
    assert (await client.post(f"/v1/agents/{agent_id}/publish", headers=headers)).status_code == 200
    return agent_id


async def chat(
    client: httpx.AsyncClient,
    agent_id: str,
    key: str,
    conversation_id: str | None = None,
    headers: dict[str, str] = ALICE,
) -> httpx.Response:
    return await client.post(
        "/v1/chat",
        headers={**headers, "Idempotency-Key": key},
        json={
            "agentId": agent_id,
            "message": key,
            "conversationId": conversation_id,
        },
    )


@pytest.mark.integration
@pytest.mark.anyio
async def test_owned_history_pages_keep_failed_turns_and_bind_cursors(conversation_app) -> None:
    _, client, model, _ = conversation_app
    agent_id = await create_agent(client)
    first = await chat(client, agent_id, "first")
    assert first.status_code == 200
    cid = first.json()["data"]["conversationId"]
    model.errors["failed"] = BadGatewayError(code="model_unavailable", message="upstream failed")
    assert (await chat(client, agent_id, "failed", cid)).status_code == 502
    assert (await chat(client, agent_id, "last", cid)).status_code == 200
    second = await chat(client, agent_id, "another-conversation")
    other_cid = second.json()["data"]["conversationId"]

    page = await client.get("/v1/conversations?limit=1", headers=ALICE)
    assert page.status_code == 200
    data = page.json()["data"]
    assert data["has_more"] is True
    next_page = (
        await client.get(
            "/v1/conversations",
            headers=ALICE,
            params={
                "limit": 1,
                "cursor": data["next_cursor"],
            },
        )
    ).json()["data"]
    assert next_page["has_more"] is False
    assert {data["items"][0]["conversationId"], next_page["items"][0]["conversationId"]} == {
        cid,
        other_cid,
    }
    assert (
        await client.get(
            "/v1/conversations",
            headers=BOB,
            params={
                "cursor": data["next_cursor"],
            },
        )
    ).status_code == 422
    detail = (await client.get(f"/v1/conversations/{cid}", headers=ALICE)).json()["data"]
    assert detail["conversationId"] == cid
    assert detail["generation"]["status"] == "succeeded"

    items = []
    cursor = None
    first_cursor = None
    while True:
        params = {"limit": 1}
        if cursor:
            params["cursor"] = cursor
        response = await client.get(
            f"/v1/conversations/{cid}/messages", headers=ALICE, params=params
        )
        assert response.status_code == 200
        data = response.json()["data"]
        items.extend(data["items"])
        cursor = data["next_cursor"]
        first_cursor = first_cursor or cursor
        if not data["has_more"]:
            assert cursor is None
            break
    assert [item["sequence"] for item in items] == [1, 2, 3, 4, 5]
    assert [item["content"] for item in items] == [
        "first",
        "reply:first",
        "failed",
        "last",
        "reply:last",
    ]
    assert [item["generationStatus"] for item in items] == [
        "succeeded",
        "succeeded",
        "failed",
        "succeeded",
        "succeeded",
    ]
    assert items[2]["error"]["code"] == "model_unavailable"
    assert "leaseToken" not in str(items)
    for path in (f"/v1/conversations/{cid}", f"/v1/conversations/{cid}/messages"):
        assert (await client.get(path, headers=BOB)).status_code == 404
    assert (
        await client.get(
            f"/v1/conversations/{other_cid}/messages",
            headers=ALICE,
            params={
                "cursor": first_cursor,
            },
        )
    ).status_code == 422


@pytest.mark.integration
@pytest.mark.anyio
async def test_delete_hides_immediately_reuses_operation_and_resumes_bounded_cleanup(
    conversation_app,
) -> None:
    app, client, _, memory = conversation_app
    agent = await create_agent(client)
    reply = (await chat(client, agent, "delete-me")).json()["data"]
    cid = reply["conversationId"]
    for scope, conversation in (("user", None), ("conversation", cid)):
        for index in range(3):
            response = await client.post(
                "/v1/memories",
                headers=ALICE,
                json={
                    "content": f"{scope}-{index}",
                    "scope": scope,
                    "conversationId": conversation,
                },
            )
            assert response.status_code == 201
    other = (await chat(client, agent, "keep-me")).json()["data"]["conversationId"]
    assert (await client.delete(f"/v1/conversations/{cid}", headers=BOB)).status_code == 404
    responses = await asyncio.gather(
        *[client.delete(f"/v1/conversations/{cid}", headers=ALICE) for _ in range(2)]
    )
    assert [response.status_code for response in responses] == [202, 202]
    accepted = responses[0].json()["data"]
    assert responses[1].json()["data"]["operationId"] == accepted["operationId"]
    url = accepted["statusUrl"]
    assert responses[0].headers["Location"] == url
    assert (await client.get(url, headers=BOB)).status_code == 404
    assert [
        item["conversationId"]
        for item in (await client.get("/v1/conversations", headers=ALICE)).json()["data"]["items"]
    ] == [other]
    for suffix in ("", "/messages", f"/generations/{reply['generationId']}"):
        assert (
            await client.get(f"/v1/conversations/{cid}{suffix}", headers=ALICE)
        ).status_code == 404
    blocked = await chat(client, agent, "blocked", cid)
    assert blocked.status_code == 409
    assert blocked.json()["code"] == "conversation_deleting"
    visible = (await client.get("/v1/memories", headers=ALICE)).json()["data"]["items"]
    assert all(item["scope"] == "user" for item in visible)

    now = datetime.now(UTC)
    for step in range(30):
        # Recreate the registry and runner every pass to exercise durable restart boundaries.
        runner = WorkerRunner(
            session_factory=app.state.session_factory,
            registry=build_worker_registry(
                app.state.session_factory, memory_provider=memory, conversation_cleanup_batch_size=1
            ),
            config=WorkerConfig(concurrency=1, claim_batch_size=1),
            periodic_jobs=(),
        )
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
        operation = (await client.get(url, headers=ALICE)).json()["data"]
        if operation["status"] == "succeeded":
            break
    assert operation["status"] == "succeeded", operation
    assert all(record.scope == "user" for record in memory.records.values())
    assert {f"user-{index}" for index in range(3)} <= {
        record.content for record in memory.records.values()
    }
    assert (await client.get(f"/v1/conversations/{other}", headers=ALICE)).status_code == 200
    repeated = await client.delete(f"/v1/conversations/{cid}", headers=ALICE)
    assert repeated.status_code == 202
    assert repeated.json()["data"]["operationId"] == accepted["operationId"]


@pytest.mark.integration
@pytest.mark.anyio
async def test_delete_waits_for_dispatched_explicit_write_and_removes_late_memory(
    conversation_app,
) -> None:
    app, client, _, memory = conversation_app
    agent = await create_agent(client)
    cid = (await chat(client, agent, "inflight-memory")).json()["data"]["conversationId"]
    pending = asyncio.create_task(
        client.post(
            "/v1/memories",
            headers=ALICE,
            json={"content": "gated", "scope": "conversation", "conversationId": cid},
        )
    )
    await asyncio.wait_for(memory.started.wait(), timeout=5)
    deleted = await client.delete(f"/v1/conversations/{cid}", headers=ALICE)
    url = deleted.json()["data"]["statusUrl"]
    runner = WorkerRunner(
        session_factory=app.state.session_factory,
        registry=build_worker_registry(app.state.session_factory, memory_provider=memory),
        config=WorkerConfig(concurrency=1, claim_batch_size=1),
        periodic_jobs=(),
    )
    try:
        await runner.run_once()
        assert (await client.get(url, headers=ALICE)).json()["data"]["status"] != "succeeded"
    finally:
        memory.release.set()
    response = await asyncio.wait_for(pending, timeout=5)
    assert response.status_code == 409
    assert response.json()["code"] == "conversation_deleting"
    now = datetime.now(UTC)
    for step in range(12):
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
        operation = (await client.get(url, headers=ALICE)).json()["data"]
        if operation["status"] == "succeeded":
            break
    assert operation["status"] == "succeeded", operation
    assert not any(record.conversation_id for record in memory.records.values())


@pytest.mark.integration
@pytest.mark.anyio
async def test_delete_waits_for_running_generation(conversation_app) -> None:
    app, client, model, memory = conversation_app
    agent = await create_agent(client)
    cid = (await chat(client, agent, "before-delete")).json()["data"]["conversationId"]
    started, release, _ = model.gate("during-delete")
    pending = asyncio.create_task(chat(client, agent, "during-delete", cid))
    await asyncio.wait_for(started.wait(), timeout=5)
    deleted = await client.delete(f"/v1/conversations/{cid}", headers=ALICE)
    url = deleted.json()["data"]["statusUrl"]
    runner = WorkerRunner(
        session_factory=app.state.session_factory,
        registry=build_worker_registry(app.state.session_factory, memory_provider=memory),
        config=WorkerConfig(concurrency=3, claim_batch_size=3),
        periodic_jobs=(),
    )
    try:
        await runner.run_once()
        assert (await client.get(url, headers=ALICE)).json()["data"]["status"] != "succeeded"
    finally:
        release.set()
    response = await asyncio.wait_for(pending, timeout=5)
    assert response.status_code != 200
    now = datetime.now(UTC)
    for step in range(20):
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
        operation = (await client.get(url, headers=ALICE)).json()["data"]
        if operation["status"] == "succeeded":
            break
    assert operation["status"] == "succeeded", operation


@pytest.mark.integration
@pytest.mark.anyio
async def test_delete_waits_for_unknown_remote_result_to_be_verified(conversation_app) -> None:
    app, client, _, memory = conversation_app
    agent = await create_agent(client)
    cid = (await chat(client, agent, "remote-uncertain")).json()["data"]["conversationId"]
    created = await client.post(
        "/v1/memories",
        headers=ALICE,
        json={"content": "uncertain", "scope": "conversation", "conversationId": cid},
    )
    assert created.status_code == 201
    memory.defer_delete = True
    deleted = await client.delete(f"/v1/conversations/{cid}", headers=ALICE)
    url = deleted.json()["data"]["statusUrl"]
    runner = WorkerRunner(
        session_factory=app.state.session_factory,
        registry=build_worker_registry(app.state.session_factory, memory_provider=memory),
        config=WorkerConfig(concurrency=3, claim_batch_size=3),
        periodic_jobs=(),
    )
    now = datetime.now(UTC)
    for step in range(8):
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
        if memory.pending_deletes:
            break
    assert memory.pending_deletes
    for step in range(8, 12):
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
    assert (await client.get(url, headers=ALICE)).json()["data"]["status"] != "succeeded"
    memory.allow_verification = True
    for step in range(12, 35):
        await runner.run_once(now=now + timedelta(seconds=60 * (step + 1)))
        operation = (await client.get(url, headers=ALICE)).json()["data"]
        if operation["status"] == "succeeded":
            break
    assert operation["status"] == "succeeded", operation
    assert not any(record.conversation_id for record in memory.records.values())
