"""API tests for private Agent creation, draft updates, versioning, publishing, and pagination."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import update

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.modules.agents.models import Agent
from mem_ai.modules.auth.adapters import StaticTokenAuthAdapter
from mem_ai.modules.users import service as users_service


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.integration
@pytest.mark.anyio
async def test_agent_create_publish_list_detail_patch_and_versions_persist_across_restart(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app1 = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app1.router.lifespan_context(app1):
        transport1 = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport1, base_url="http://test") as client:
            me_resp = await client.get("/v1/users/me", headers=_auth_headers("token-alice"))
            assert me_resp.status_code == 200
            alice_id = me_resp.json()["data"]["userId"]

            # 1. Create private Agent (initial draft stored on agents row; not yet published).
            create_resp = await client.post(
                "/v1/agents",
                headers=_auth_headers("token-alice"),
                json={
                    "name": "Architecture Tutor",
                    "systemPrompt": "Guide the user through clean modular designs.",
                    "behaviorRules": ["Ask clarifying questions", "Keep examples grounded"],
                    "tone": "constructive",
                    "language": "zh-CN",
                    "defaultModelId": "gpt-4.1-mini",
                    "memoryPolicy": {
                        "enabled": True,
                        "writeEnabled": True,
                        "writeScope": "conversation",
                        "recallLimit": 6,
                    },
                },
            )
            assert create_resp.status_code == 201
            create_body = create_resp.json()
            assert create_body["success"] is True
            assert create_body["code"] == "ok"
            assert create_body["request_id"] == create_resp.headers["X-Request-Id"]
            agent_data = create_body["data"]
            agent_id = agent_data["agentId"]
            assert agent_data["ownerId"] == alice_id
            assert agent_data["status"] == "active"
            assert agent_data["currentPublishedVersionId"] is None
            assert agent_data["currentPublishedVersion"] is None
            assert agent_data["publishedVersion"] is None
            assert agent_data["latestVersion"] == 0
            assert agent_data["draft"]["defaultModelId"] == "gpt-4.1-mini"
            assert agent_data["draft"]["memoryPolicy"]["writeScope"] == "conversation"

            # 2. Publish first persona version from draft (empty body supported).
            publish_resp = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-alice"),
            )
            assert publish_resp.status_code == 200
            pub_data = publish_resp.json()["data"]
            v1_id = pub_data["currentPublishedVersionId"]
            assert v1_id is not None
            assert pub_data["currentPublishedVersion"] == 1
            assert pub_data["latestVersion"] == 1
            assert pub_data["publishedVersion"]["versionId"] == v1_id
            assert pub_data["publishedVersion"]["version"] == 1
            assert (
                pub_data["publishedVersion"]["systemPrompt"]
                == "Guide the user through clean modular designs."
            )
            assert pub_data["publishedVersion"]["defaultModelId"] == "gpt-4.1-mini"
            assert pub_data["publishedVersion"]["memoryPolicy"]["writeScope"] == "conversation"

            # 3. PATCH draft does not alter published v1.
            patch_resp = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
                json={
                    "systemPrompt": "Draft prompt v2",
                    "tone": None,
                    "memoryPolicy": {"writeScope": "user"},
                },
            )
            assert patch_resp.status_code == 200
            patch_data = patch_resp.json()["data"]
            assert patch_data["draft"]["systemPrompt"] == "Draft prompt v2"
            assert patch_data["draft"]["tone"] is None
            assert patch_data["draft"]["memoryPolicy"]["writeScope"] == "user"
            assert patch_data["draft"]["memoryPolicy"]["recallLimit"] == 6
            assert patch_data["currentPublishedVersionId"] == v1_id
            assert (
                patch_data["publishedVersion"]["systemPrompt"]
                == "Guide the user through clean modular designs."
            )

            # 4. POST /versions creates v2 snapshot without switching currentPublishedVersionId.
            ver_resp = await client.post(
                f"/v1/agents/{agent_id}/versions",
                headers=_auth_headers("token-alice"),
            )
            assert ver_resp.status_code == 201
            v2_data = ver_resp.json()["data"]
            v2_id = v2_data["versionId"]
            assert v2_data["version"] == 2
            assert v2_data["publishedAt"] is None
            assert v2_data["systemPrompt"] == "Draft prompt v2"

            detail_before_switch = await client.get(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
            )
            assert detail_before_switch.status_code == 200
            assert detail_before_switch.json()["data"]["currentPublishedVersionId"] == v1_id
            assert detail_before_switch.json()["data"]["latestVersion"] == 2

            # 5. Publish explicit v2 snapshot and confirm publishedAt is stamped.
            switch_resp = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-alice"),
                json={"versionId": v2_id},
            )
            assert switch_resp.status_code == 200
            assert switch_resp.json()["data"]["currentPublishedVersionId"] == v2_id
            assert switch_resp.json()["data"]["currentPublishedVersion"] == 2
            assert switch_resp.json()["data"]["publishedVersion"]["publishedAt"] is not None

    # 7. Simulate full application restart and verify list & detail still return persisted state.
    app2 = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app2.router.lifespan_context(app2):
        transport2 = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport2, base_url="http://test") as client:
            list_resp = await client.get("/v1/agents", headers=_auth_headers("token-alice"))
            assert list_resp.status_code == 200
            list_data = list_resp.json()["data"]
            assert list_data["has_more"] is False
            assert list_data["next_cursor"] is None
            assert len(list_data["items"]) == 1
            assert list_data["items"][0]["agentId"] == agent_id
            assert list_data["items"][0]["currentPublishedVersionId"] == v2_id
            assert list_data["items"][0]["currentPublishedVersion"] == 2

            detail_resp = await client.get(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
            )
            assert detail_resp.status_code == 200
            detail_data = detail_resp.json()["data"]
            assert detail_data["publishedVersion"]["versionId"] == v2_id
            assert detail_data["publishedVersion"]["systemPrompt"] == "Draft prompt v2"

            # Verify default LocalPersonaProvider resolves both v2 (latest) and v1 (pinned).
            persona_provider = app2.state.providers.persona_provider
            latest_snap = await persona_provider.get_latest_published(
                owner_id=UUID(alice_id),
                agent_id=UUID(agent_id),
            )
            pinned_v1 = await persona_provider.get_version(
                owner_id=UUID(alice_id),
                agent_id=UUID(agent_id),
                version_id=UUID(v1_id),
            )
            assert latest_snap is not None and latest_snap.version == 2
            assert pinned_v1 is not None and pinned_v1.version == 1


@pytest.mark.integration
@pytest.mark.anyio
async def test_cross_user_access_returns_uniform_404_and_client_user_id_cannot_alter_ownership(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            alice_id = (
                await client.get("/v1/users/me", headers=_auth_headers("token-alice"))
            ).json()["data"]["userId"]
            bob_id = (await client.get("/v1/users/me", headers=_auth_headers("token-bob"))).json()[
                "data"
            ]["userId"]

            # Body ownerId / userId rejected by extra="forbid".
            forged_body_resp = await client.post(
                "/v1/agents",
                headers=_auth_headers("token-alice"),
                json={
                    "name": "Forged",
                    "systemPrompt": "Prompt",
                    "ownerId": bob_id,
                },
            )
            assert forged_body_resp.status_code == 422
            assert forged_body_resp.json()["code"] == "validation_error"

            # Query params and untrusted identity headers cannot change ownership away from Alice.
            create_resp = await client.post(
                f"/v1/agents?ownerId={bob_id}&userId={bob_id}",
                headers={
                    **_auth_headers("token-alice"),
                    "X-User-Id": bob_id,
                    "X-Auth-Subject": "bob",
                },
                json={"name": "Alice Private", "systemPrompt": "Alice prompt"},
            )
            assert create_resp.status_code == 201
            agent_id = create_resp.json()["data"]["agentId"]
            assert create_resp.json()["data"]["ownerId"] == alice_id

            # Bob cannot see Alice's agent in list.
            bob_list = await client.get("/v1/agents", headers=_auth_headers("token-bob"))
            assert bob_list.status_code == 200
            assert bob_list.json()["data"]["items"] == []

            # Compare Bob's error against nonexistent UUID: must be uniform 404 agent_not_found.
            missing_id = str(uuid4())
            missing_resp = await client.get(
                f"/v1/agents/{missing_id}",
                headers=_auth_headers("token-bob"),
            )
            assert missing_resp.status_code == 404
            expected_code = missing_resp.json()["code"]
            assert expected_code == "agent_not_found"

            bob_get = await client.get(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-bob"),
            )
            bob_patch = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-bob"),
                json={"name": "Hijacked"},
            )
            bob_version = await client.post(
                f"/v1/agents/{agent_id}/versions",
                headers=_auth_headers("token-bob"),
            )
            bob_publish = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-bob"),
            )
            bob_delete = await client.delete(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-bob"),
            )

            for resp in (bob_get, bob_patch, bob_version, bob_publish, bob_delete):
                assert resp.status_code == 404
                body = resp.json()
                assert body["success"] is False
                assert body["code"] == expected_code
                assert body["data"] is None

            # Bob also cannot reference Alice's versionId when publishing Bob's own Agent.
            alice_pub = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-alice"),
            )
            alice_v1_id = alice_pub.json()["data"]["currentPublishedVersionId"]

            bob_own = await client.post(
                "/v1/agents",
                headers=_auth_headers("token-bob"),
                json={"name": "Bob Own", "systemPrompt": "Bob prompt"},
            )
            bob_agent_id = bob_own.json()["data"]["agentId"]
            cross_ver_pub = await client.post(
                f"/v1/agents/{bob_agent_id}/publish",
                headers=_auth_headers("token-bob"),
                json={"versionId": alice_v1_id},
            )
            assert cross_ver_pub.status_code == 404
            assert cross_ver_pub.json()["code"] == "persona_version_not_found"

            # Unauthenticated access returns 401; disabled user returns 403.
            unauth_resp = await client.get("/v1/agents")
            assert unauth_resp.status_code == 401

            await users_service.set_user_status(
                app.state.session_factory,
                user_id=UUID(bob_id),
                status="disabled",
            )
            disabled_resp = await client.get("/v1/agents", headers=_auth_headers("token-bob"))
            assert disabled_resp.status_code == 403
            assert disabled_resp.json()["code"] == "user_disabled"


@pytest.mark.integration
@pytest.mark.anyio
async def test_agents_list_http_pagination_handles_identical_timestamps_and_invalid_cursor(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created_ids: list[UUID] = []
            for idx in range(5):
                resp = await client.post(
                    "/v1/agents",
                    headers=_auth_headers("token-alice"),
                    json={"name": f"Agent {idx}", "systemPrompt": f"Prompt {idx}"},
                )
                assert resp.status_code == 201
                created_ids.append(UUID(resp.json()["data"]["agentId"]))

            # Pin identical created_at timestamps across all rows.
            same_ts = datetime(2026, 11, 21, 12, 0, 0, tzinfo=UTC)
            async with app.state.session_factory.begin() as session:
                await session.execute(update(Agent).values(created_at=same_ts))

            seen: list[UUID] = []
            cursor: str | None = None
            page_count = 0
            while True:
                params: dict[str, str | int] = {"limit": 2}
                if cursor is not None:
                    params["cursor"] = cursor
                resp = await client.get(
                    "/v1/agents",
                    headers=_auth_headers("token-alice"),
                    params=params,
                )
                assert resp.status_code == 200
                data = resp.json()["data"]
                page_count += 1
                seen.extend(UUID(item["agentId"]) for item in data["items"])
                if not data["has_more"]:
                    assert data["next_cursor"] is None
                    break
                assert data["next_cursor"] is not None
                cursor = data["next_cursor"]

            assert page_count == 3
            assert seen == sorted(created_ids, reverse=True)

            # Malformed cursor returns 422 invalid_cursor.
            bad_cursor_resp = await client.get(
                "/v1/agents",
                headers=_auth_headers("token-alice"),
                params={"cursor": "not-valid-base64!!!"},
            )
            assert bad_cursor_resp.status_code == 422
            assert bad_cursor_resp.json()["code"] == "invalid_cursor"

            # Reject empty PATCH, empty memoryPolicy, explicit null, snake_case, and null versionId.
            empty_patch_resp = await client.patch(
                f"/v1/agents/{created_ids[0]}",
                headers=_auth_headers("token-alice"),
                json={},
            )
            assert empty_patch_resp.status_code == 422
            assert empty_patch_resp.json()["code"] == "validation_error"

            empty_policy_patch_resp = await client.patch(
                f"/v1/agents/{created_ids[0]}",
                headers=_auth_headers("token-alice"),
                json={"memoryPolicy": {}},
            )
            assert empty_policy_patch_resp.status_code == 422
            assert empty_policy_patch_resp.json()["code"] == "validation_error"

            null_policy_field_resp = await client.patch(
                f"/v1/agents/{created_ids[0]}",
                headers=_auth_headers("token-alice"),
                json={"memoryPolicy": {"writeScope": None}},
            )
            assert null_policy_field_resp.status_code == 422
            assert null_policy_field_resp.json()["code"] == "validation_error"

            snake_case_create_resp = await client.post(
                "/v1/agents",
                headers=_auth_headers("token-alice"),
                json={"name": "Snake", "system_prompt": "Not allowed on wire"},
            )
            assert snake_case_create_resp.status_code == 422
            assert snake_case_create_resp.json()["code"] == "validation_error"

            null_version_publish_resp = await client.post(
                f"/v1/agents/{created_ids[0]}/publish",
                headers=_auth_headers("token-alice"),
                json={"versionId": None},
            )
            assert null_version_publish_resp.status_code == 422
            assert null_version_publish_resp.json()["code"] == "validation_error"


@pytest.mark.integration
@pytest.mark.anyio
async def test_retire_agent_http_route_is_idempotent_blocks_mutations_and_persists_across_restart(
    migrated_pg_settings: Settings,
    test_auth_adapter: StaticTokenAuthAdapter,
) -> None:
    app1 = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app1.router.lifespan_context(app1):
        transport1 = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport1, base_url="http://test") as client:
            alice_id = (
                await client.get("/v1/users/me", headers=_auth_headers("token-alice"))
            ).json()["data"]["userId"]
            bob_id = (await client.get("/v1/users/me", headers=_auth_headers("token-bob"))).json()[
                "data"
            ]["userId"]

            create_resp = await client.post(
                "/v1/agents",
                headers=_auth_headers("token-alice"),
                json={
                    "name": "Lifecycle Agent",
                    "systemPrompt": "Original published prompt",
                    "behaviorRules": ["Stay calm"],
                    "tone": "neutral",
                    "language": "zh-CN",
                    "defaultModelId": "gpt-4o-mini",
                },
            )
            assert create_resp.status_code == 201
            agent_id = create_resp.json()["data"]["agentId"]

            publish_resp = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-alice"),
            )
            assert publish_resp.status_code == 200
            v1_id = publish_resp.json()["data"]["currentPublishedVersionId"]

            # Unauthenticated DELETE returns 401; nonexistent UUID returns 404 agent_not_found.
            assert (await client.delete(f"/v1/agents/{agent_id}")).status_code == 401
            missing_delete = await client.delete(
                f"/v1/agents/{uuid4()}",
                headers=_auth_headers("token-alice"),
            )
            assert missing_delete.status_code == 404
            assert missing_delete.json()["code"] == "agent_not_found"

            # Forged headers/query params cannot redirect ownership on DELETE.
            retire_resp_1 = await client.delete(
                f"/v1/agents/{agent_id}?ownerId={bob_id}&userId={bob_id}",
                headers={
                    **_auth_headers("token-alice"),
                    "X-User-Id": bob_id,
                    "X-Auth-Subject": "bob",
                },
            )
            assert retire_resp_1.status_code == 200
            body_1 = retire_resp_1.json()
            assert body_1["success"] is True
            assert body_1["code"] == "ok"
            assert body_1["request_id"] == retire_resp_1.headers["X-Request-Id"]
            data_1 = body_1["data"]
            assert data_1["agentId"] == agent_id
            assert data_1["ownerId"] == alice_id
            assert data_1["status"] == "retired"
            assert data_1["retiredAt"] is not None
            assert data_1["currentPublishedVersionId"] == v1_id
            assert data_1["publishedVersion"]["systemPrompt"] == "Original published prompt"

            # Repeat DELETE is idempotent 200 with unchanged retiredAt and publishedVersion.
            retire_resp_2 = await client.delete(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
            )
            assert retire_resp_2.status_code == 200
            data_2 = retire_resp_2.json()["data"]
            assert data_2["status"] == "retired"
            assert data_2["retiredAt"] == data_1["retiredAt"]
            assert data_2["currentPublishedVersionId"] == v1_id

            # Cross-user DELETE on an already-retired Agent still returns 404 agent_not_found.
            bob_delete_retired = await client.delete(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-bob"),
            )
            assert bob_delete_retired.status_code == 404
            assert bob_delete_retired.json()["code"] == "agent_not_found"

            # Retired Agent rejects draft update, version snapshot, and publish with 409.
            patch_retired = await client.patch(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
                json={"systemPrompt": "Disallowed edit"},
            )
            assert patch_retired.status_code == 409
            assert patch_retired.json()["code"] == "agent_retired"

            version_retired = await client.post(
                f"/v1/agents/{agent_id}/versions",
                headers=_auth_headers("token-alice"),
            )
            assert version_retired.status_code == 409
            assert version_retired.json()["code"] == "agent_retired"

            publish_retired = await client.post(
                f"/v1/agents/{agent_id}/publish",
                headers=_auth_headers("token-alice"),
            )
            assert publish_retired.status_code == 409
            assert publish_retired.json()["code"] == "agent_retired"

    # Full app restart: retired Agent and its published persona snapshot remain readable.
    app2 = create_app(migrated_pg_settings, credential_verifier=test_auth_adapter)
    async with app2.router.lifespan_context(app2):
        transport2 = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport2, base_url="http://test") as client:
            detail_resp = await client.get(
                f"/v1/agents/{agent_id}",
                headers=_auth_headers("token-alice"),
            )
            assert detail_resp.status_code == 200
            detail_data = detail_resp.json()["data"]
            assert detail_data["status"] == "retired"
            assert detail_data["retiredAt"] == data_1["retiredAt"]
            assert detail_data["publishedVersion"]["versionId"] == v1_id
            assert detail_data["publishedVersion"]["systemPrompt"] == "Original published prompt"

            list_resp = await client.get("/v1/agents", headers=_auth_headers("token-alice"))
            assert list_resp.status_code == 200
            items = list_resp.json()["data"]["items"]
            assert len(items) == 1
            assert items[0]["agentId"] == agent_id
            assert items[0]["status"] == "retired"
            assert items[0]["retiredAt"] == data_1["retiredAt"]
