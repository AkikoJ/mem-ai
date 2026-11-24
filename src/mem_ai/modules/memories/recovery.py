"""Post-backup recovery replay entry points for memory deletions and epoch barriers."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ForbiddenError
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.memories import repository
from mem_ai.modules.memories.barrier import (
    NoopRecalledMemoryReferenceMarker,
    RecalledMemoryReferenceMarker,
    resolve_current_time,
)
from mem_ai.modules.memories.clearing import prepare_clear_scope_in_session
from mem_ai.modules.memories.deletions import prepare_single_delete_tx1
from mem_ai.modules.operations.audit import RecoveryReplayStatus
from mem_ai.modules.users import service as users_service


async def replay_single_memory_delete_for_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    memory_id: str,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    max_attempts: int = 5,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> RecoveryReplayStatus:
    """Idempotently install a tombstone and durable delete job via the shared Tx 1 path.

    Executes strictly locally without outbound HTTP calls while API and Worker are paused.
    Permits disabled users so deleted memories cannot resurrect if a user is re-enabled.
    """
    normalized_id = memory_id.strip()
    if not normalized_id:
        return "skipped_missing"

    marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
    try:
        prepared = await prepare_single_delete_tx1(
            session_factory,
            user_id=user_id,
            provider_id=provider_id,
            memory_id=normalized_id,
            marker=marker,
            max_attempts=max_attempts,
            attempt_retention_days=attempt_retention_days,
            now=now,
            force_async=True,
            allow_disabled_user=True,
        )
    except ForbiddenError:
        return "skipped_missing"
    return "already_applied" if prepared.shortcut is not None else "enqueued"


async def replay_clear_scope_for_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    provider_id: str,
    target_epoch: int,
    recalled_memory_marker: RecalledMemoryReferenceMarker | None = None,
    max_attempts: int = 5,
    attempt_retention_days: int = jobs_service.DEFAULT_JOB_ATTEMPT_RETENTION_DAYS,
    now: datetime | None = None,
) -> tuple[RecoveryReplayStatus, int]:
    """Idempotently raise epoch, cancel pre-backup writes, and enqueue durable clear job."""
    desired_epoch = max(1, target_epoch)
    marker = recalled_memory_marker or NoopRecalledMemoryReferenceMarker()
    async with session_factory.begin() as session:
        try:
            await users_service.ensure_user_not_deleting_in_session(
                session, user_id=user_id, for_share=True
            )
        except ForbiddenError:
            return "skipped_missing", 0

        current_time = await resolve_current_time(session, now)
        scope_row = await repository.get_or_create_scope_for_update(
            session, user_id=user_id, provider_id=provider_id
        )
        if scope_row.epoch >= desired_epoch and scope_row.barrier_state == "idle":
            return "already_applied", scope_row.epoch

        prepared = await prepare_clear_scope_in_session(
            session,
            user_id=user_id,
            provider_id=provider_id,
            marker=marker,
            max_attempts=max_attempts,
            current_time=current_time,
            force_async=True,
            attempt_retention_days=attempt_retention_days,
            min_new_epoch=desired_epoch,
        )
        if prepared.shortcut is not None:
            return "already_applied", scope_row.epoch
        return "enqueued", prepared.new_epoch
