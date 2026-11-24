"""Independent deletion audit archive seam for backup-restore replay (§12.4, ADR 0005)."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mem_ai.core.logging import sanitize_log_value
from mem_ai.db.base import normalize_utc

logger = logging.getLogger("mem_ai.operations.audit")

DeletionAuditEventType = Literal[
    "operation_accepted",
    "operation_transitioned",
    "memory_epoch_advanced",
    "memory_barrier_released",
    "memory_tombstone_created",
    "memory_tombstone_verified",
]

RecoveryReplayStatus = Literal["enqueued", "already_applied", "skipped_missing"]


class AuditReplayPlan(BaseModel):
    """Deduplicated, precedence-ordered replay actions derived from a deletion audit archive."""

    model_config = ConfigDict(frozen=True)

    source_event_count: int
    user_deletes: tuple[UUID, ...] = ()
    conversation_deletes: tuple[tuple[UUID, UUID], ...] = ()
    memory_clears: tuple[tuple[UUID, str, int], ...] = ()
    memory_deletes: tuple[tuple[UUID, str, str], ...] = ()


def _sanitize_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    sanitized = sanitize_log_value(None, raw)
    return sanitized if isinstance(sanitized, dict) else {}


class DeletionAuditEvent(BaseModel):
    """Append-only structured audit record exported outside the primary DB backup cycle."""

    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: DeletionAuditEventType
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    user_id: UUID | None = None
    operation_id: UUID | None = None
    operation_type: str | None = None
    provider_id: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    memory_epoch: int | None = None
    status: str | None = None
    barrier_state: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def sanitized_copy(self) -> DeletionAuditEvent:
        """Return a copy whose metadata is scrubbed of sensitive keys/values."""
        return self.model_copy(update={"metadata": _sanitize_metadata(self.metadata)})


@runtime_checkable
class DeletionAuditArchiver(Protocol):
    """Replaceable seam exporting deletion & epoch events to an independent audit sink."""

    async def export_event(self, event: DeletionAuditEvent) -> None:
        """Persist one deletion audit event durably outside primary DB backup snapshots."""
        ...


class NoopDeletionAuditArchiver:
    """Default archiver used when no external deletion audit archive target is configured."""

    async def export_event(self, event: DeletionAuditEvent) -> None:
        _ = event


class JsonlDeletionAuditArchiver:
    """Append-only JSONL file archiver suitable for an independent audit volume."""

    def __init__(self, archive_path: str | Path) -> None:
        self._path = Path(archive_path)
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def _append_line(self, serialized: str) -> None:
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

    async def export_event(self, event: DeletionAuditEvent) -> None:
        sanitized = event.sanitized_copy()
        line = sanitized.model_dump_json()
        await asyncio.to_thread(self._append_line, line)


def build_deletion_audit_archiver(archive_path: str | None) -> DeletionAuditArchiver:
    """Instantiate the configured deletion audit archiver (JSONL file or no-op)."""
    if archive_path is not None and archive_path.strip():
        return JsonlDeletionAuditArchiver(archive_path.strip())
    return NoopDeletionAuditArchiver()


def load_deletion_audit_events(
    archive_path: str | Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[DeletionAuditEvent]:
    """Read, validate, time-filter, and stably sort events from an append-only JSONL archive."""
    path = Path(archive_path)
    if not path.exists():
        raise FileNotFoundError(f"deletion audit archive not found: {path}")

    since_utc = normalize_utc(since) if since is not None else None
    until_utc = normalize_utc(until) if until is not None else None
    events: list[DeletionAuditEvent] = []

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                event = DeletionAuditEvent.model_validate_json(stripped)
            except (ValidationError, ValueError) as exc:
                raise ValueError(
                    f"invalid deletion audit record at {path} line {line_number}: {exc}"
                ) from exc
            occurred_at = normalize_utc(event.occurred_at)
            if since_utc is not None and occurred_at < since_utc:
                continue
            if until_utc is not None and occurred_at > until_utc:
                continue
            events.append(event)

    events.sort(key=lambda item: (normalize_utc(item.occurred_at), str(item.event_id)))
    return events


def plan_deletion_audit_replay(events: Sequence[DeletionAuditEvent]) -> AuditReplayPlan:
    """Collapse raw audit events into deterministic, precedence-ordered replay targets.

    Precedence:
      1. `user_delete` subsumes all conversation and memory deletions for that user.
      2. `memory_clear` / `memory_epoch_advanced` collapses to the maximum archived epoch per
         `(user_id, provider_id)` and subsumes older-epoch (`memory_epoch < max_epoch`) single
         memory deletions for that scope.
      3. `conversation_delete` and remaining `memory_delete` targets are deduplicated.
    """
    user_deletes: list[UUID] = []
    user_delete_set: set[UUID] = set()
    raw_conv_deletes: list[tuple[UUID, UUID]] = []
    max_clear_epochs: dict[tuple[UUID, str], int] = {}
    raw_mem_deletes: dict[tuple[UUID, str, str], int] = {}

    for event in events:
        if event.user_id is None:
            continue
        if event.operation_type == "user_delete":
            if event.user_id not in user_delete_set:
                user_delete_set.add(event.user_id)
                user_deletes.append(event.user_id)
            continue

        if event.operation_type == "conversation_delete" and event.target_id:
            try:
                conv_id = UUID(event.target_id)
            except ValueError:
                continue
            pair = (event.user_id, conv_id)
            if pair not in raw_conv_deletes:
                raw_conv_deletes.append(pair)
            continue

        if (
            event.event_type in ("memory_epoch_advanced", "memory_barrier_released")
            or event.operation_type == "memory_clear"
        ) and event.provider_id:
            key = (event.user_id, event.provider_id)
            target_epoch = max(1, event.memory_epoch or 1)
            previous = max_clear_epochs.get(key, 0)
            if target_epoch > previous:
                max_clear_epochs[key] = target_epoch
            continue

        if (
            (
                event.event_type in ("memory_tombstone_created", "memory_tombstone_verified")
                or event.operation_type == "memory_delete"
            )
            and event.provider_id
            and event.target_type == "memory"
            and event.target_id
        ):
            mem_key = (event.user_id, event.provider_id, event.target_id)
            epoch = event.memory_epoch if event.memory_epoch is not None else 0
            if mem_key not in raw_mem_deletes or epoch > raw_mem_deletes[mem_key]:
                raw_mem_deletes[mem_key] = epoch

    conv_deletes = tuple(pair for pair in raw_conv_deletes if pair[0] not in user_delete_set)
    memory_clears = tuple(
        (user_id, provider_id, epoch)
        for (user_id, provider_id), epoch in sorted(
            max_clear_epochs.items(), key=lambda item: (str(item[0][0]), item[0][1])
        )
        if user_id not in user_delete_set
    )
    clear_lookup = {(u, p): ep for u, p, ep in memory_clears}
    memory_deletes = tuple(
        (user_id, provider_id, memory_id)
        for (user_id, provider_id, memory_id), mem_epoch in sorted(
            raw_mem_deletes.items(),
            key=lambda item: (str(item[0][0]), item[0][1], item[0][2]),
        )
        if user_id not in user_delete_set
        and clear_lookup.get((user_id, provider_id), 0) <= mem_epoch
    )
    return AuditReplayPlan(
        source_event_count=len(events),
        user_deletes=tuple(user_deletes),
        conversation_deletes=conv_deletes,
        memory_clears=memory_clears,
        memory_deletes=memory_deletes,
    )


async def export_deletion_audit_events_safely(
    archiver: DeletionAuditArchiver | None,
    events: Sequence[DeletionAuditEvent],
) -> None:
    """Export one or more deletion audit events, alerting on I/O failures without hiding commits."""
    if archiver is None or not events:
        return
    for event in events:
        try:
            await archiver.export_event(event.sanitized_copy())
        except Exception:
            logger.error(
                "Failed to export deletion audit event to external archive",
                extra={
                    "extra_fields": {
                        "alert": "deletion_audit_export_failed",
                        "event_type": event.event_type,
                        "operation_id": str(event.operation_id) if event.operation_id else None,
                        "user_id": str(event.user_id) if event.user_id else None,
                        "provider": event.provider_id,
                    }
                },
                exc_info=True,
            )
