"""Built-in session issuance serialized against account deletion in PostgreSQL."""

from __future__ import annotations

import asyncio
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from mem_ai.modules.auth import service as auth_service
from mem_ai.modules.auth.models import AuthSession

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def _wait_for_blocked_request(
    sessions: async_sessionmaker[AsyncSession], blocker_pid: int
) -> None:
    async with asyncio.timeout(5):
        while True:
            async with sessions() as session:
                blocked = await session.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE :blocker_pid = ANY(pg_blocking_pids(pid)))"
                    ),
                    {"blocker_pid": blocker_pid},
                )
            if blocked:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("action", ["login", "refresh"])
async def test_session_issuance_waits_for_concurrent_deletion_and_cannot_escape_revocation(
    migrated_pg_settings: Settings,
    action: str,
) -> None:
    app = create_app(migrated_pg_settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        credentials = {"email": "race@example.com", "password": "correct-horse-1"}
        registered = await client.post("/v1/auth/register", json=credentials)
        assert registered.status_code == 201
        user_id = UUID(registered.json()["data"]["userId"])
        payload = (
            credentials
            if action == "login"
            else {"refreshToken": registered.json()["data"]["tokens"]["refreshToken"]}
        )
        sessions = app.state.session_factory

        # Hold deletion's user lock while a real auth request reaches the transaction fence.
        async with sessions.begin() as deletion:
            await deletion.execute(
                text("SELECT id FROM users WHERE id = :user_id FOR UPDATE"),
                {"user_id": user_id},
            )
            blocker_pid = await deletion.scalar(text("SELECT pg_backend_pid()"))
            request = asyncio.create_task(client.post(f"/v1/auth/{action}", json=payload))
            try:
                await _wait_for_blocked_request(sessions, blocker_pid)
                await deletion.execute(
                    text("UPDATE users SET status = 'deleting' WHERE id = :user_id"),
                    {"user_id": user_id},
                )
                await auth_service.revoke_user_sessions_in_session(deletion, user_id=user_id)
            except BaseException:
                request.cancel()
                await asyncio.gather(request, return_exceptions=True)
                raise

        response = await asyncio.wait_for(request, timeout=5)
        assert response.status_code == (403 if action == "login" else 401)
        assert response.json()["code"] == (
            "user_deleting" if action == "login" else "invalid_credentials"
        )
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuthSession)
                    .where(
                        AuthSession.user_id == user_id,
                        AuthSession.revoked_at.is_(None),
                    )
                )
                == 0
            )
