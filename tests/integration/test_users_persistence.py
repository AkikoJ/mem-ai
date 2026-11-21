"""Integration tests for users & user_identities persistence, concurrency, and status policy."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import ForbiddenError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.users import service as users_service


@pytest.mark.integration
@pytest.mark.anyio
async def test_identity_resolution_persists_across_restart_and_isolates_issuers(
    migrated_pg_settings: Settings,
) -> None:
    engine_1 = create_async_engine_from_settings(migrated_pg_settings)
    factory_1 = create_session_factory(engine_1)
    try:
        alice_1 = await users_service.resolve_or_create_user(
            factory_1,
            issuer="https://idp-a.example.com",
            subject="same-sub",
        )
        other_issuer = await users_service.resolve_or_create_user(
            factory_1,
            issuer="https://idp-b.example.com",
            subject="same-sub",
        )
        assert alice_1.id != other_issuer.id
        assert alice_1.status == "active"
    finally:
        await engine_1.dispose()

    # Simulate process restart with a brand new engine and session factory
    engine_2 = create_async_engine_from_settings(migrated_pg_settings)
    factory_2 = create_session_factory(engine_2)
    try:
        alice_after_restart = await users_service.resolve_or_create_user(
            factory_2,
            issuer="https://idp-a.example.com",
            subject="same-sub",
        )
        assert alice_after_restart.id == alice_1.id
    finally:
        await engine_2.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_concurrent_first_resolution_reuses_single_user_without_orphans(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        results = await asyncio.gather(
            *[
                users_service.resolve_or_create_user(
                    session_factory,
                    issuer="https://idp.example.com",
                    subject="concurrent-first-user",
                )
                for _ in range(10)
            ]
        )

        resolved_ids = {user.id for user in results}
        assert len(resolved_ids) == 1

        async with session_factory() as session:
            user_count = (await session.execute(text("SELECT COUNT(*) FROM users"))).scalar_one()
            identity_count = (
                await session.execute(text("SELECT COUNT(*) FROM user_identities"))
            ).scalar_one()

        assert user_count == 1
        assert identity_count == 1
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_disabled_and_deleting_users_are_blocked_and_deleting_keeps_identity_binding(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    session_factory = create_session_factory(engine)
    try:
        user = await users_service.resolve_active_user(
            session_factory,
            issuer="https://idp.example.com",
            subject="status-check-user",
        )
        assert user.status == "active"

        # Disable user
        disabled = await users_service.set_user_status(
            session_factory,
            user_id=user.id,
            status="disabled",
        )
        assert disabled.status == "disabled"

        with pytest.raises(ForbiddenError) as exc:
            await users_service.resolve_active_user(
                session_factory,
                issuer="https://idp.example.com",
                subject="status-check-user",
            )
        assert exc.value.code == "user_disabled"

        with pytest.raises(ForbiddenError) as exc:
            async with session_factory.begin() as session:
                await users_service.ensure_user_active_in_session(session, user_id=user.id)
        assert exc.value.code == "user_disabled"

        # Mark user deleting: identity binding remains so re-auth hits 403 and does not recreate
        deleting = await users_service.set_user_status(
            session_factory,
            user_id=user.id,
            status="deleting",
        )
        assert deleting.status == "deleting"

        with pytest.raises(ForbiddenError) as exc:
            await users_service.resolve_active_user(
                session_factory,
                issuer="https://idp.example.com",
                subject="status-check-user",
            )
        assert exc.value.code == "user_deleting"

        with pytest.raises(ForbiddenError) as exc:
            await users_service.get_active_user(session_factory, user_id=user.id)
        assert exc.value.code == "user_deleting"

        async with session_factory() as session:
            user_count = (await session.execute(text("SELECT COUNT(*) FROM users"))).scalar_one()
        assert user_count == 1
    finally:
        await engine.dispose()
