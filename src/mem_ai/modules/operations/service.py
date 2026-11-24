"""Public service entry points for creating, transitioning, auditing, and querying operations."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import NotFoundError, UnauthorizedError
from mem_ai.db.base import normalize_utc
from mem_ai.modules.operations import repository
from mem_ai.modules.operations.schemas import (
    DeletionReceiptResponse,
    OperationRecord,
    OperationStatus,
)
from mem_ai.modules.users import service as users_service

# Explicit Engineering Design §11.3.1 state machine for `operations.status`.
ALLOWED_OPERATION_TRANSITIONS: dict[OperationStatus, frozenset[OperationStatus]] = {
    "pending": frozenset({"running", "failed"}),
    "running": frozenset({"running", "reconciling", "succeeded", "failed"}),
    "reconciling": frozenset({"reconciling", "succeeded", "failed"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
}
_RECEIPT_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}\Z")


def _receipt_digest(*, settings: Settings, operation_id: UUID, receipt: str) -> str:
    return hmac.new(
        settings.deletion_receipt_signing_key(),
        f"user_delete:{operation_id}:{receipt}".encode(),
        hashlib.sha256,
    ).hexdigest()


async def create_user_deletion_operation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    settings: Settings,
    now: datetime | None = None,
) -> tuple[OperationRecord, DeletionReceiptResponse]:
    """Accept one deletion with a one-time receipt in the caller's user-lock transaction."""
    current_time = normalize_utc(now) if now is not None else datetime.now(UTC)
    expires_at = current_time + timedelta(seconds=settings.deletion_receipt_ttl_seconds)
    receipt = secrets.token_urlsafe(32)
    record = await create_operation_in_session(
        session,
        owner_id=owner_id,
        operation_type="user_delete",
        status="pending",
        target_type="user",
        target_id=str(owner_id),
        progress={"local_complete": False, "remote_complete": False},
        expires_at=expires_at,
    )
    await repository.set_deletion_receipt(
        session,
        operation_id=record.id,
        digest=_receipt_digest(settings=settings, operation_id=record.id, receipt=receipt),
    )
    return record, DeletionReceiptResponse(
        operation_id=record.id,
        status=record.status,
        status_url=record.status_url,
        deletion_receipt=receipt,
        expires_at=expires_at,
    )


async def get_operation_for_deletion_receipt(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    operation_id: UUID,
    receipt: str,
    settings: Settings,
    now: datetime | None = None,
) -> OperationRecord:
    """Authenticate a single operation-scoped receipt after the business user is disabled."""
    if _RECEIPT_PATTERN.fullmatch(receipt) is None:
        raise UnauthorizedError(code="invalid_deletion_receipt", message="invalid deletion receipt")
    current_time = normalize_utc(now) if now is not None else datetime.now(UTC)
    async with session_factory() as session:
        row = await repository.get_operation_by_id(session, operation_id=operation_id)
        expected_digest = row.deletion_receipt_digest if row is not None else None
        matches = hmac.compare_digest(
            expected_digest or "0" * 64,
            _receipt_digest(settings=settings, operation_id=operation_id, receipt=receipt),
        )
        if (
            row is None
            or row.operation_type != "user_delete"
            or expected_digest is None
            or not matches
            or row.expires_at is None
            or normalize_utc(row.expires_at) <= current_time
            or row.deletion_receipt_revoked_at is not None
        ):
            raise UnauthorizedError(
                code="invalid_deletion_receipt", message="invalid deletion receipt"
            )
        return OperationRecord.model_validate(row)


async def revoke_user_deletion_receipt_in_session(
    session: AsyncSession, *, operation_id: UUID, now: datetime | None = None
) -> bool:
    """Revoke receipt access without changing the ongoing deletion lifecycle."""
    return await repository.revoke_deletion_receipt(
        session,
        operation_id=operation_id,
        revoked_at=normalize_utc(now) if now is not None else datetime.now(UTC),
    )


async def can_finalize_user_operations_in_batch_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    deletion_operation_id: UUID,
    batch_size: int = 50,
) -> bool:
    """Check whether remaining child operations can be cleared in one final batch."""
    return await repository.can_finalize_user_operations_in_batch(
        session,
        owner_id=owner_id,
        deletion_operation_id=deletion_operation_id,
        batch_size=batch_size,
    )


async def finalize_user_operations_in_session(
    session: AsyncSession, *, owner_id: UUID, deletion_operation_id: UUID, batch_size: int = 50
) -> bool:
    """Join outstanding child operations and remove their settled business payloads."""
    return await repository.finalize_user_operations(
        session,
        owner_id=owner_id,
        deletion_operation_id=deletion_operation_id,
        batch_size=batch_size,
    )


def is_valid_operation_transition(current: OperationStatus, target: OperationStatus) -> bool:
    """Return True iff `current -> target` is allowed by Engineering Design §11.3.1."""
    return target in ALLOWED_OPERATION_TRANSITIONS[current]


def allowed_operation_source_statuses(target: OperationStatus) -> tuple[OperationStatus, ...]:
    """Return all source statuses permitted to transition to `target`."""
    return tuple(
        source
        for source, allowed_targets in ALLOWED_OPERATION_TRANSITIONS.items()
        if target in allowed_targets
    )


async def create_operation_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID | None,
    operation_type: str,
    status: OperationStatus,
    target_type: str | None = None,
    target_id: str | None = None,
    job_id: UUID | None = None,
    progress: dict[str, Any] | None = None,
    result_summary: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    expires_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> OperationRecord:
    """Create an operation row inside the caller's transaction."""
    row = await repository.create_operation(
        session,
        owner_id=owner_id,
        operation_type=operation_type,
        status=status,
        target_type=target_type,
        target_id=target_id,
        job_id=job_id,
        progress=progress or {},
        result_summary=result_summary or {},
        error_code=error_code,
        error_message=error_message,
        expires_at=expires_at,
        completed_at=completed_at,
    )
    return OperationRecord.model_validate(row)


async def get_operation_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    operation_id: UUID,
) -> OperationRecord:
    """Return an owned operation after re-verifying active user status; 404 for cross-user."""
    async with session_factory() as session:
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        row = await repository.get_operation_for_owner(
            session,
            owner_id=owner_id,
            operation_id=operation_id,
        )
        if row is None:
            raise NotFoundError(
                code="operation_not_found",
                message="operation not found",
            )
        return OperationRecord.model_validate(row)


async def get_operation_in_session(
    session: AsyncSession,
    *,
    operation_id: UUID,
    for_update: bool = False,
) -> OperationRecord | None:
    """Read an operation by ID inside the caller's transaction."""
    row = await repository.get_operation_by_id(
        session,
        operation_id=operation_id,
        for_update=for_update,
    )
    return OperationRecord.model_validate(row) if row is not None else None


async def find_operation_for_target_in_session(
    session: AsyncSession, *, owner_id: UUID, operation_type: str, target_id: str
) -> OperationRecord | None:
    """Recover an owner's effective operation after its business target was removed."""
    row = await repository.find_operation_for_target(
        session, owner_id=owner_id, operation_type=operation_type, target_id=target_id
    )
    return OperationRecord.model_validate(row) if row is not None else None


async def list_non_terminal_operations_in_session(
    session: AsyncSession,
    *,
    owner_id: UUID,
    operation_type: str,
    batch_size: int = 50,
) -> list[OperationRecord]:
    """Read one bounded batch of non-terminal operations of a given type for an owner."""
    rows = await repository.list_non_terminal_operations_for_owner(
        session,
        owner_id=owner_id,
        operation_type=operation_type,
        batch_size=batch_size,
    )
    return [OperationRecord.model_validate(row) for row in rows]


async def attach_job_in_session(
    session: AsyncSession,
    *,
    operation_id: UUID,
    job_id: UUID,
    now: datetime | None = None,
) -> OperationRecord | None:
    """Attach the durable job ID to an operation inside the caller's transaction."""
    current_time = now if now is not None else datetime.now(UTC)
    updated = await repository.attach_job_id(
        session,
        operation_id=operation_id,
        job_id=job_id,
        updated_at=current_time,
    )
    return OperationRecord.model_validate(updated) if updated is not None else None


async def transition_operation_in_session(
    session: AsyncSession,
    *,
    operation_id: UUID,
    status: OperationStatus,
    progress: dict[str, Any] | None = None,
    result_summary: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    now: datetime | None = None,
) -> OperationRecord | None:
    """Conditionally advance an operation following Engineering Design §11.3.1."""
    current_time = now if now is not None else datetime.now(UTC)
    completed_at = current_time if status in ("succeeded", "failed") else None
    allowed_sources = allowed_operation_source_statuses(status)
    updated = await repository.conditional_transition_operation(
        session,
        operation_id=operation_id,
        new_status=status,
        allowed_from_statuses=allowed_sources,
        progress=progress,
        result_summary=result_summary,
        error_code=error_code,
        error_message=error_message,
        completed_at=completed_at,
        updated_at=current_time,
    )
    return OperationRecord.model_validate(updated) if updated is not None else None


@dataclass(frozen=True, slots=True)
class OperationPurgeBatch:
    """Result of one bounded expired-operation retention scan batch."""

    deleted_count: int
    protected_skipped: int
    scanned_count: int
    next_cursor: tuple[datetime, UUID] | None
    has_more: bool


OperationProtectionFilter = Callable[[AsyncSession, Sequence[UUID]], Awaitable[frozenset[UUID]]]


async def find_non_terminal_operation_links_in_session(
    session: AsyncSession,
    *,
    job_ids: Sequence[UUID] = (),
    operation_ids: Sequence[UUID] = (),
) -> tuple[frozenset[UUID], frozenset[UUID]]:
    """Return `(protected_job_ids, protected_operation_ids)` still tied to non-terminal ops."""
    return await repository.find_non_terminal_operation_links(
        session,
        job_ids=job_ids,
        operation_ids=operation_ids,
    )


async def purge_expired_user_delete_operations_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> OperationPurgeBatch:
    """Physically delete finalized `user_delete` operations whose receipt TTL has elapsed."""
    (
        deleted_count,
        scanned_count,
        next_cursor,
        has_more,
    ) = await repository.purge_expired_user_delete_operations_batch(
        session,
        current_time=current_time,
        batch_size=batch_size,
        cursor=cursor,
    )
    return OperationPurgeBatch(
        deleted_count=deleted_count,
        protected_skipped=0,
        scanned_count=scanned_count,
        next_cursor=next_cursor,
        has_more=has_more,
    )


async def purge_expired_operation_summaries_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    retention_days: int = 30,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
    protected_operation_filter: OperationProtectionFilter | None = None,
    protected_job_filter: OperationProtectionFilter | None = None,
) -> OperationPurgeBatch:
    """Delete one bounded batch of settled non-user-delete operations past their retention window.

    Protects non-terminal operations, operations still backing active memory barriers or
    unverified tombstones, and operations linked to active/reconciling jobs.
    """
    candidates, next_cursor, has_more = await repository.scan_expired_operation_summaries_batch(
        session,
        current_time=current_time,
        retention_days=retention_days,
        batch_size=batch_size,
        cursor=cursor,
    )
    if not candidates:
        return OperationPurgeBatch(
            deleted_count=0,
            protected_skipped=0,
            scanned_count=0,
            next_cursor=None,
            has_more=False,
        )

    protected_ops: set[UUID] = set()
    if protected_operation_filter is not None:
        protected_ops.update(
            await protected_operation_filter(session, [op_id for op_id, _, _ in candidates])
        )

    protected_jobs: set[UUID] = set()
    if protected_job_filter is not None:
        job_ids = [job_id for _, job_id, _ in candidates if job_id is not None]
        if job_ids:
            protected_jobs.update(await protected_job_filter(session, job_ids))

    deletable_ids: list[UUID] = []
    protected_skipped = 0
    for op_id, job_id, _ in candidates:
        if op_id in protected_ops or (job_id is not None and job_id in protected_jobs):
            protected_skipped += 1
            continue
        deletable_ids.append(op_id)

    deleted_count = await repository.delete_operations_by_ids(
        session,
        operation_ids=deletable_ids,
    )
    return OperationPurgeBatch(
        deleted_count=deleted_count,
        protected_skipped=protected_skipped,
        scanned_count=len(candidates),
        next_cursor=next_cursor,
        has_more=has_more,
    )


async def get_oldest_operation_created_at_in_session(
    session: AsyncSession,
    *,
    operation_ids: Sequence[UUID],
) -> datetime | None:
    """Return earliest `operations.created_at` for active barrier/operation age metrics."""
    return await repository.get_oldest_operation_created_at(session, operation_ids=operation_ids)
