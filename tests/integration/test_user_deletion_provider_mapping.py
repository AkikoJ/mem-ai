"""Historical provider outages retain user identity and durable deletion mappings."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import text

from mem_ai.modules.memories import service as memories_service
from mem_ai.modules.memories.schemas import CreatedMemoryOutcome
from tests.integration.test_user_deletion import (
    ALICE,
    DeletionMemoryProvider,
    UserDeletionHarness,
    accept_deletion,
)
from tests.integration.test_user_deletion import deletion_harness as deletion_harness


@pytest.mark.integration
@pytest.mark.anyio
async def test_missing_historical_provider_retains_identity_scope_and_operation_mapping(
    deletion_harness: UserDeletionHarness,
) -> None:
    harness = deletion_harness
    user_id = UUID(
        (await harness.client.get("/v1/users/me", headers=ALICE)).json()["data"]["userId"]
    )
    historical = DeletionMemoryProvider()
    historical.provider_id = "retired-memory-provider"
    created = await memories_service.create_explicit_memory(
        harness.sessions,
        memory_provider=historical,
        user_id=user_id,
        content="Historical provider data still requiring deletion",
    )
    assert isinstance(created, CreatedMemoryOutcome)
    accepted = await accept_deletion(harness)
    now = datetime.now(UTC)
    historical_operation = None
    for step in range(9):
        await harness.run(harness.worker(), now=now + timedelta(minutes=step + 1))
        operation = await harness.operation(accepted)
        assert operation["status"] != "succeeded"
        assert operation["progress"]["local_complete"] is True
        assert operation["progress"]["remote_complete"] is False
        assert "provider_operations" not in operation["progress"]
        async with harness.sessions() as session:
            parent_meta = await session.scalar(
                text(
                    "SELECT metadata_json FROM jobs "
                    "WHERE id = (SELECT job_id FROM operations WHERE id = :op)"
                ),
                {"op": UUID(accepted["operationId"])},
            )
        assert isinstance(parent_meta, dict)
        mappings = parent_meta["provider_operations"]
        assert set(mappings) == {historical.provider_id, harness.memory.provider_id}
        if historical_operation is None:
            historical_operation = mappings[historical.provider_id]
        assert mappings[historical.provider_id] == historical_operation
    assert operation["status"] == "reconciling"
    assert operation["progress"]["phase"] == "provider_unavailable"
    assert operation["error"]["code"] == "memory_provider_mismatch"
    assert (await harness.client.get("/v1/users/me", headers=ALICE)).status_code == 403
    assert created.record.memory_id in historical.records
    assert historical.clear_calls == []
    assert harness.memory.clear_calls == [user_id]
    async with harness.sessions() as session:
        assert (
            await session.scalar(
                text("SELECT count(*) FROM users WHERE id = :owner AND status = 'deleting'"),
                {"owner": user_id},
            )
            == 1
        )
        assert (
            await session.scalar(
                text("SELECT count(*) FROM user_identities WHERE user_id = :owner"),
                {"owner": user_id},
            )
            == 1
        )
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM memory_scopes "
                    "WHERE user_id = :owner AND provider_id = :provider AND barrier_state <> 'idle'"
                ),
                {"owner": user_id, "provider": historical.provider_id},
            )
            == 1
        )
        child = (
            await session.execute(
                text(
                    "SELECT j.status, j.last_error_code, j.metadata_json, "
                    "o.status AS operation_status "
                    "FROM jobs j JOIN operations o ON o.job_id = j.id "
                    "WHERE o.id = :operation AND o.owner_id = :owner"
                ),
                {"operation": UUID(historical_operation), "owner": user_id},
            )
        ).one()
        assert child.status == "reconciling"
        assert child.last_error_code == "memory_provider_mismatch"
        assert child.metadata_json["provider_id"] == historical.provider_id
        assert child.operation_status == "reconciling"
