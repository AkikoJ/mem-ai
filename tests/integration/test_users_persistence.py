"""Built-in users and login sessions persist, isolate accounts, and guard status changes."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from sqlalchemy import text

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.modules.users import service as users_service
from tests.conftest import TEST_PASSWORD, login_builtin_user, register_builtin_user


@pytest.mark.integration
@pytest.mark.anyio
async def test_builtin_login_persists_across_restart_and_isolates_email_accounts(
    migrated_pg_settings: Settings,
) -> None:
    app_1 = create_app(migrated_pg_settings)
    async with (
        app_1.router.lifespan_context(app_1),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_1), base_url="http://test"
        ) as client,
    ):
        alice = await register_builtin_user(client, email="Alice@example.com")
        bob = await register_builtin_user(client, email="bob@example.com")
        assert alice.user_id != bob.user_id
        assert alice.email == "alice@example.com"

    app_2 = create_app(migrated_pg_settings)
    async with (
        app_2.router.lifespan_context(app_2),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_2), base_url="http://test"
        ) as client,
    ):
        logged_in = await login_builtin_user(client, email="ALICE@example.com")
        assert logged_in.user_id == alice.user_id
        for headers in (alice.headers, logged_in.headers):
            profile = await client.get("/v1/users/me", headers=headers)
            assert profile.status_code == 200
            assert profile.json()["data"]["userId"] == str(alice.user_id)
            assert profile.json()["data"]["email"] == "alice@example.com"
        bob_profile = await client.get("/v1/users/me", headers=bob.headers)
        assert bob_profile.status_code == 200
        assert bob_profile.json()["data"]["userId"] == str(bob.user_id)


@pytest.mark.integration
@pytest.mark.anyio
async def test_concurrent_normalized_email_registration_creates_one_user_and_session(
    migrated_pg_settings: Settings,
) -> None:
    app = create_app(migrated_pg_settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        results = await asyncio.gather(
            *[
                client.post(
                    "/v1/auth/register",
                    json={"email": email, "password": TEST_PASSWORD},
                )
                for email in ["concurrent@example.com", "CONCURRENT@example.com"] * 5
            ]
        )
        assert [result.status_code for result in results].count(201) == 1
        conflicts = [result for result in results if result.status_code != 201]
        assert len(conflicts) == 9
        assert all(result.status_code == 409 for result in conflicts)
        assert {result.json()["code"] for result in conflicts} == {"email_already_registered"}
        logged_in = await login_builtin_user(client, email="concurrent@example.com")
        winner = next(result for result in results if result.status_code == 201)
        assert winner.json()["data"]["userId"] == str(logged_in.user_id)
        async with app.state.session_factory() as session:
            assert await session.scalar(text("SELECT COUNT(*) FROM users")) == 1
            # Only registration and this successful login issued sessions; losing signups did not.
            assert await session.scalar(text("SELECT COUNT(*) FROM auth_sessions")) == 2


@pytest.mark.integration
@pytest.mark.anyio
async def test_disabled_and_deleting_users_block_login_and_preserve_email_reservation(
    migrated_pg_settings: Settings,
) -> None:
    app = create_app(migrated_pg_settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        user = await register_builtin_user(client, email="status-check@example.com")
        for status, error_code in [("disabled", "user_disabled"), ("deleting", "user_deleting")]:
            await users_service.set_user_status(
                app.state.session_factory, user_id=user.user_id, status=status
            )
            profile = await client.get("/v1/users/me", headers=user.headers)
            login = await client.post(
                "/v1/auth/login", json={"email": user.email, "password": TEST_PASSWORD}
            )
            for response in (profile, login):
                assert response.status_code == 403
                assert response.json()["code"] == error_code
            duplicate = await client.post(
                "/v1/auth/register", json={"email": user.email.upper(), "password": TEST_PASSWORD}
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["code"] == "email_already_registered"
        async with app.state.session_factory() as session:
            assert await session.scalar(text("SELECT COUNT(*) FROM users")) == 1
            assert await session.scalar(text("SELECT COUNT(*) FROM auth_sessions")) == 1
