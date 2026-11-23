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

from pydantic import BaseModel, ConfigDict, Field

from mem_ai.core.logging import sanitize_log_value

logger = logging.getLogger("mem_ai.operations.audit")

DeletionAuditEventType = Literal[
    "operation_accepted",
    "operation_transitioned",
    "memory_epoch_advanced",
    "memory_barrier_released",
    "memory_tombstone_created",
    "memory_tombstone_verified",
]


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
