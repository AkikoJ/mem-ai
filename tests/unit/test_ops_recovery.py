"""Unit tests for operational recovery, audit replay planning, and connection budgets."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from mem_ai.core.config import Settings
from mem_ai.modules.health import service as health_service
from mem_ai.modules.operations.audit import (
    DeletionAuditEvent,
    JsonlDeletionAuditArchiver,
    load_deletion_audit_events,
    plan_deletion_audit_replay,
)
from mem_ai.ops import compute_connection_budget


@pytest.mark.anyio
async def test_load_deletion_audit_events_filters_time_window_and_orders_stably(
    tmp_path: Path,
) -> None:
    archive_file = tmp_path / "deletion-audit.jsonl"
    archiver = JsonlDeletionAuditArchiver(archive_file)

    t0 = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)
    before = DeletionAuditEvent(
        event_type="memory_epoch_advanced",
        occurred_at=t0 - timedelta(minutes=5),
        user_id=uuid4(),
        provider_id="memos",
        memory_epoch=1,
    )
    at_t0 = DeletionAuditEvent(
        event_type="memory_tombstone_created",
        occurred_at=t0,
        user_id=uuid4(),
        provider_id="memos",
        target_type="memory",
        target_id="mem_1",
        memory_epoch=1,
    )
    after_late = DeletionAuditEvent(
        event_type="operation_accepted",
        occurred_at=t0 + timedelta(minutes=10),
        user_id=uuid4(),
        operation_id=uuid4(),
        operation_type="user_delete",
        target_type="user",
        status="pending",
    )
    after_early = DeletionAuditEvent(
        event_type="operation_accepted",
        occurred_at=t0 + timedelta(minutes=2),
        user_id=uuid4(),
        operation_id=uuid4(),
        operation_type="conversation_delete",
        target_type="conversation",
        target_id=str(uuid4()),
        status="running",
    )

    # Export intentionally out of chronological order to verify stable replay sorting.
    await archiver.export_event(before)
    await archiver.export_event(after_late)
    await archiver.export_event(at_t0)
    await archiver.export_event(after_early)

    loaded = load_deletion_audit_events(archive_file, since=t0)
    assert [e.event_id for e in loaded] == [
        at_t0.event_id,
        after_early.event_id,
        after_late.event_id,
    ]

    bounded = load_deletion_audit_events(
        archive_file,
        since=t0,
        until=t0 + timedelta(minutes=5),
    )
    assert [e.event_id for e in bounded] == [at_t0.event_id, after_early.event_id]


def test_load_deletion_audit_events_rejects_corrupt_jsonl_line(tmp_path: Path) -> None:
    archive_file = tmp_path / "corrupt.jsonl"
    archive_file.write_text('{"event_type": "not_valid"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="line 1"):
        load_deletion_audit_events(archive_file)


def test_discover_alembic_head_falls_back_to_cwd_for_non_editable_container_install(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    fake_module_file = (
        tmp_path
        / "venv"
        / "lib"
        / "python3.13"
        / "site-packages"
        / "mem_ai"
        / "modules"
        / "health"
        / "service.py"
    )
    fake_module_file.parent.mkdir(parents=True, exist_ok=True)
    fake_module_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(health_service, "__file__", str(fake_module_file))
    monkeypatch.chdir(repo_root)
    health_service._discover_alembic_head.cache_clear()
    try:
        head = health_service._discover_alembic_head()
        assert head == "20261124_0012"
    finally:
        health_service._discover_alembic_head.cache_clear()


def test_compute_connection_budget_enforces_total_and_pool_bounds() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        db_pool_size=5,
        db_max_overflow=5,
        worker_concurrency=4,
    )
    ok = compute_connection_budget(
        settings,
        replicas=6,
        reserved_connections=15,
        postgres_max_connections=100,
    )
    assert ok.replicas == 6
    assert ok.per_process_max_connections == 10
    assert ok.total_replica_connections == 60
    assert ok.total_required_connections == 75
    assert ok.worker_loop_enabled is True
    assert ok.within_budget is True
    assert ok.pool_adequate is True

    over = compute_connection_budget(
        settings,
        replicas=11,
        reserved_connections=10,
        postgres_max_connections=100,
    )
    assert over.total_required_connections == 120
    assert over.within_budget is False
    assert over.pool_adequate is True

    online_only_tight = Settings(
        _env_file=None,
        app_env="test",
        worker_loop_enabled=False,
        db_pool_size=2,
        db_max_overflow=1,
        worker_concurrency=4,
    )
    online_only_report = compute_connection_budget(
        online_only_tight,
        replicas=2,
        reserved_connections=5,
        postgres_max_connections=50,
    )
    assert online_only_report.within_budget is True
    assert online_only_report.worker_loop_enabled is False
    assert online_only_report.pool_adequate is True

    under_provisioned_settings = online_only_tight.model_copy(update={"worker_loop_enabled": True})
    under_provisioned = compute_connection_budget(
        under_provisioned_settings,
        replicas=2,
        reserved_connections=5,
        postgres_max_connections=50,
    )
    assert under_provisioned.within_budget is True
    assert under_provisioned.pool_adequate is False

    zero_replicas = compute_connection_budget(
        under_provisioned_settings,
        replicas=0,
        reserved_connections=5,
        postgres_max_connections=50,
    )
    assert zero_replicas.total_replica_connections == 0
    assert zero_replicas.within_budget is True
    assert zero_replicas.pool_adequate is True


def test_plan_deletion_audit_replay_applies_precedence_and_deduplication() -> None:
    user_deleted = uuid4()
    user_cleared = uuid4()
    user_single = uuid4()
    conv_a = uuid4()
    conv_b = uuid4()
    t0 = datetime(2026, 11, 24, 13, 0, 0, tzinfo=UTC)

    events = [
        # Events for a user who later deleted their account -> all subsumed by user_delete.
        DeletionAuditEvent(
            event_type="memory_tombstone_created",
            occurred_at=t0,
            user_id=user_deleted,
            provider_id="memos",
            target_type="memory",
            target_id="mem_subsumed",
            memory_epoch=0,
        ),
        DeletionAuditEvent(
            event_type="operation_accepted",
            occurred_at=t0 + timedelta(seconds=5),
            user_id=user_deleted,
            operation_id=uuid4(),
            operation_type="conversation_delete",
            target_type="conversation",
            target_id=str(conv_a),
            status="running",
        ),
        DeletionAuditEvent(
            event_type="operation_accepted",
            occurred_at=t0 + timedelta(seconds=10),
            user_id=user_deleted,
            operation_id=uuid4(),
            operation_type="user_delete",
            target_type="user",
            status="pending",
        ),
        # User with single delete at epoch 1, clears to epoch 2 & 3, plus a delete at epoch 3.
        DeletionAuditEvent(
            event_type="memory_tombstone_created",
            occurred_at=t0 + timedelta(seconds=1),
            user_id=user_cleared,
            provider_id="memos",
            target_type="memory",
            target_id="mem_old_epoch",
            memory_epoch=1,
        ),
        DeletionAuditEvent(
            event_type="memory_epoch_advanced",
            occurred_at=t0 + timedelta(seconds=2),
            user_id=user_cleared,
            provider_id="memos",
            memory_epoch=2,
            barrier_state="clearing",
        ),
        DeletionAuditEvent(
            event_type="memory_epoch_advanced",
            occurred_at=t0 + timedelta(seconds=4),
            user_id=user_cleared,
            provider_id="memos",
            memory_epoch=3,
            barrier_state="clearing",
        ),
        DeletionAuditEvent(
            event_type="memory_tombstone_created",
            occurred_at=t0 + timedelta(seconds=6),
            user_id=user_cleared,
            provider_id="memos",
            target_type="memory",
            target_id="mem_after_clear",
            memory_epoch=3,
        ),
        # Independent user with conversation delete + single memory delete.
        DeletionAuditEvent(
            event_type="operation_accepted",
            occurred_at=t0 + timedelta(seconds=7),
            user_id=user_single,
            operation_id=uuid4(),
            operation_type="conversation_delete",
            target_type="conversation",
            target_id=str(conv_b),
            status="running",
        ),
        DeletionAuditEvent(
            event_type="memory_tombstone_created",
            occurred_at=t0 + timedelta(seconds=8),
            user_id=user_single,
            provider_id="memos",
            target_type="memory",
            target_id="mem_keep_delete",
            memory_epoch=0,
        ),
    ]

    plan = plan_deletion_audit_replay(events)
    assert plan.user_deletes == (user_deleted,)
    assert plan.conversation_deletes == ((user_single, conv_b),)
    assert plan.memory_clears == ((user_cleared, "memos", 3),)
    assert set(plan.memory_deletes) == {
        (user_cleared, "memos", "mem_after_clear"),
        (user_single, "memos", "mem_keep_delete"),
    }


def test_dockerfile_and_dockerignore_define_locked_non_root_runtime() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dockerfile = (repo_root / "Dockerfile").read_text(encoding="utf-8")
    dockerignore = (repo_root / ".dockerignore").read_text(encoding="utf-8")

    assert "python:3.13-slim" in dockerfile
    assert "ghcr.io/astral-sh/uv:0.11.6" in dockerfile
    assert "uv sync --locked" in dockerfile
    assert "USER memai" in dockerfile
    assert "mem_ai.main:app" in dockerfile
    assert ".venv" in dockerignore
    assert ".env" in dockerignore
    assert ".scratch" in dockerignore
