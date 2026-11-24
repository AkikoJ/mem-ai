"""Chat idempotency key validation, semantic digest, persistence, and replay resolution.

Owns `idempotency_records` semantics per Engineering Design §2.2 & §5.3:
- Scope uniqueness: `(user_id, method, canonical_route, idempotency_key)`.
- Semantic digest covers normalized business parameters (`agent_id`, `conversation_id`,
  `conversation_omitted`, `message`, `model_id`) and explicitly excludes `stream`.
- Active (`running`) records never expire (`terminal_at = NULL`, `expires_at = NULL`).
- Terminal records retain `expires_at = terminal_at + retention_seconds` (default 24h).
- Session-participating helpers execute inside Tx 1 (`prepare_generation_phase`),
  Tx 2 (`commit_generation_success_phase`), and failure/recovery convergence.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import delete, exists, func, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import AppError, ConflictError, UnprocessableEntityError
from mem_ai.db.base import get_database_now, normalize_utc
from mem_ai.modules.conversations.models import Generation, IdempotencyRecord
from mem_ai.modules.conversations.schemas import (
    DEFAULT_IDEMPOTENCY_RETENTION_SECONDS as DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
)
from mem_ai.modules.conversations.schemas import (
    ChatAcceptedResult,
    ChatTurnOutcome,
    GenerationDetail,
    GenerationStatus,
    IdempotencyKeySpec,
    IdempotencyRecordDTO,
    generation_status_url,
)
from mem_ai.modules.users import service as users_service

CHAT_HTTP_METHOD = "POST"
CHAT_CANONICAL_ROUTE = "/v1/chat"
MAX_IDEMPOTENCY_KEY_LENGTH = 255

GenerationDetailLoader = Callable[[AsyncSession, UUID, UUID, UUID], Awaitable[GenerationDetail]]

_ERROR_STATUS_FALLBACKS: dict[str, int] = {
    "generation_cancelled": 409,
    "generation_lease_lost": 409,
    "conversation_deleting": 409,
    "conversation_generation_conflict": 409,
    "user_disabled": 403,
    "user_deleting": 403,
    "generation_timeout": 504,
    "generation_commit_failed": 503,
    "generation_context_failed": 503,
    "model_upstream_error": 502,
}


def normalize_idempotency_key(raw_key: str) -> str:
    """Validate and normalize an `Idempotency-Key` header value."""
    stripped = raw_key.strip()
    if not stripped:
        raise UnprocessableEntityError(
            code="invalid_idempotency_key",
            message="Idempotency-Key header must not be blank",
        )
    if len(stripped) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise UnprocessableEntityError(
            code="invalid_idempotency_key",
            message=(
                f"Idempotency-Key header exceeds maximum length of {MAX_IDEMPOTENCY_KEY_LENGTH}"
            ),
        )
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in stripped):
        raise UnprocessableEntityError(
            code="invalid_idempotency_key",
            message="Idempotency-Key header contains invalid control characters",
        )
    return stripped


def compute_chat_request_digest(
    *,
    agent_id: UUID,
    conversation_id: UUID | None,
    message: str,
    requested_model_id: str | None,
) -> str:
    """Compute canonical SHA-256 hex digest over chat business parameters (excluding `stream`)."""
    normalized_message = message.strip()
    normalized_model_id = requested_model_id.strip() if requested_model_id is not None else None
    canonical_payload = {
        "v": 1,
        "agent_id": str(agent_id),
        "conversation_id": str(conversation_id) if conversation_id is not None else None,
        "conversation_omitted": conversation_id is None,
        "message": normalized_message,
        "model_id": normalized_model_id,
    }
    encoded = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_chat_idempotency_spec(
    *,
    raw_key: str,
    agent_id: UUID,
    conversation_id: UUID | None,
    message: str,
    requested_model_id: str | None,
    retention_seconds: float = DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
) -> IdempotencyKeySpec:
    """Validate `raw_key` and construct a canonical `IdempotencyKeySpec` for `POST /v1/chat`."""
    key = normalize_idempotency_key(raw_key)
    digest = compute_chat_request_digest(
        agent_id=agent_id,
        conversation_id=conversation_id,
        message=message,
        requested_model_id=requested_model_id,
    )
    return IdempotencyKeySpec(
        method=CHAT_HTTP_METHOD,
        canonical_route=CHAT_CANONICAL_ROUTE,
        key=key,
        request_digest=digest,
        retention_seconds=retention_seconds,
    )


def is_record_expired(record: IdempotencyRecordDTO, *, current_time: datetime) -> bool:
    """Return True iff a terminal idempotency record has reached or passed `expires_at`."""
    if record.status == "running" or record.expires_at is None:
        return False
    return normalize_utc(record.expires_at) <= normalize_utc(current_time)


def infer_error_http_status(
    error_code: str | None,
    *,
    generation_status: GenerationStatus,
) -> int:
    """Map a terminal generation failure/cancellation to its canonical HTTP status category."""
    if generation_status == "cancelled":
        return 409
    if error_code is not None and error_code in _ERROR_STATUS_FALLBACKS:
        return _ERROR_STATUS_FALLBACKS[error_code]
    return 502


def _resolve_error_http_status(
    response_status_code: int | None,
    *,
    error_code: str | None,
    generation_status: GenerationStatus,
) -> int:
    if generation_status == "cancelled":
        return 409
    if response_status_code is not None and 400 <= response_status_code <= 599:
        return response_status_code
    return infer_error_http_status(error_code, generation_status=generation_status)


def build_replay_error(
    detail: GenerationDetail,
    *,
    response_status_code: int | None = None,
) -> AppError:
    """Build the stable `AppError` returned when replaying a `failed` or `cancelled` generation."""
    gen = detail.generation
    details: dict[str, object] = {
        "conversation_id": str(gen.conversation_id),
        "generation_id": str(gen.id),
        "status": gen.status,
        "status_url": generation_status_url(gen.conversation_id, gen.id),
    }
    if gen.user_message_id is not None:
        details["user_message_id"] = str(gen.user_message_id)

    if gen.status == "cancelled":
        return AppError(
            status_code=409,
            code="generation_cancelled",
            message=gen.error_message or "generation was cancelled",
            details=details,
        )

    http_status = _resolve_error_http_status(
        response_status_code,
        error_code=gen.error_code,
        generation_status=gen.status,
    )
    return AppError(
        status_code=http_status,
        code=gen.error_code or "model_upstream_error",
        message=gen.error_message or "generation failed",
        details=details,
    )


def resolve_replay_from_detail(
    detail: GenerationDetail,
    *,
    response_status_code: int | None,
) -> ChatTurnOutcome:
    """Resolve a persisted `GenerationDetail` into 202 accepted, 200 result, or terminal error."""
    gen = detail.generation
    if gen.status == "running":
        return ChatAcceptedResult(
            conversation_id=gen.conversation_id,
            generation_id=gen.id,
            user_message_id=gen.user_message_id,
            status="running",
            status_url=generation_status_url(gen.conversation_id, gen.id),
        )
    if gen.status == "succeeded":
        return detail.to_committed_chat_result()
    raise build_replay_error(detail, response_status_code=response_status_code)


async def _get_record_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    method: str,
    canonical_route: str,
    key: str,
    for_update: bool = False,
) -> IdempotencyRecord | None:
    """Fetch an `idempotency_records` ORM row by its canonical scope tuple."""
    stmt = select(IdempotencyRecord).where(
        IdempotencyRecord.user_id == user_id,
        IdempotencyRecord.method == method,
        IdempotencyRecord.canonical_route == canonical_route,
        IdempotencyRecord.idempotency_key == key,
    )
    if for_update:
        stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def _get_record_by_generation_in_session(
    session: AsyncSession,
    *,
    generation_id: UUID,
) -> IdempotencyRecord | None:
    """Fetch the `idempotency_records` ORM row associated with `generation_id`, if any."""
    stmt = select(IdempotencyRecord).where(IdempotencyRecord.generation_id == generation_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def purge_expired_or_find_live_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    spec: IdempotencyKeySpec,
    current_time: datetime,
) -> IdempotencyRecordDTO | None:
    """Inside Tx 1: return an active/unexpired hit, or delete an expired terminal key."""
    row = await _get_record_in_session(
        session,
        user_id=user_id,
        method=spec.method,
        canonical_route=spec.canonical_route,
        key=spec.key,
        for_update=True,
    )
    if row is None:
        return None
    dto = IdempotencyRecordDTO.model_validate(row)
    if not is_record_expired(dto, current_time=current_time):
        return dto

    await session.execute(delete(IdempotencyRecord).where(IdempotencyRecord.id == row.id))
    await session.flush()
    return None


async def create_placeholder_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    spec: IdempotencyKeySpec,
    conversation_id: UUID,
    generation_id: UUID,
) -> IdempotencyRecordDTO:
    """Atomically insert a `running` idempotency placeholder inside Tx 1."""
    row = IdempotencyRecord(
        user_id=user_id,
        method=spec.method,
        canonical_route=spec.canonical_route,
        idempotency_key=spec.key,
        request_digest=spec.request_digest,
        conversation_id=conversation_id,
        generation_id=generation_id,
        status="running",
        response_status_code=None,
        error_code=None,
        error_message=None,
        terminal_at=None,
        expires_at=None,
    )
    session.add(row)
    await session.flush()
    return IdempotencyRecordDTO.model_validate(row)


async def finalize_succeeded_in_session(
    session: AsyncSession,
    *,
    generation_id: UUID,
    terminal_at: datetime,
    retention_seconds: float = DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
) -> IdempotencyRecordDTO | None:
    """Atomically finalize a linked `running` idempotency record as `succeeded` inside Tx 2."""
    utc_terminal = normalize_utc(terminal_at)
    expires_at = utc_terminal + timedelta(seconds=retention_seconds)
    stmt = (
        update(IdempotencyRecord)
        .where(
            IdempotencyRecord.generation_id == generation_id,
            IdempotencyRecord.status == "running",
        )
        .values(
            status="succeeded",
            response_status_code=200,
            error_code=None,
            error_message=None,
            terminal_at=utc_terminal,
            expires_at=expires_at,
            updated_at=func.now(),
        )
        .returning(IdempotencyRecord)
        .execution_options(populate_existing=True)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.flush()
    return IdempotencyRecordDTO.model_validate(row) if row is not None else None


async def finalize_terminal_failure_in_session(
    session: AsyncSession,
    *,
    generation_id: UUID,
    target_status: Literal["failed", "cancelled"],
    error_code: str,
    error_message: str,
    response_status_code: int | None,
    terminal_at: datetime,
    retention_seconds: float = DEFAULT_IDEMPOTENCY_RETENTION_SECONDS,
) -> IdempotencyRecordDTO | None:
    """Atomically finalize a linked `running` idempotency record as `failed` or `cancelled`."""
    utc_terminal = normalize_utc(terminal_at)
    expires_at = utc_terminal + timedelta(seconds=retention_seconds)
    resolved_http_status = _resolve_error_http_status(
        response_status_code,
        error_code=error_code,
        generation_status=target_status,
    )
    resolved_code = "generation_cancelled" if target_status == "cancelled" else error_code
    stmt = (
        update(IdempotencyRecord)
        .where(
            IdempotencyRecord.generation_id == generation_id,
            IdempotencyRecord.status == "running",
        )
        .values(
            status=target_status,
            response_status_code=resolved_http_status,
            error_code=resolved_code,
            error_message=error_message[:500],
            terminal_at=utc_terminal,
            expires_at=expires_at,
            updated_at=func.now(),
        )
        .returning(IdempotencyRecord)
        .execution_options(populate_existing=True)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.flush()
    return IdempotencyRecordDTO.model_validate(row) if row is not None else None


async def resolve_chat_idempotency(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: UUID,
    spec: IdempotencyKeySpec,
    load_detail_in_session: GenerationDetailLoader,
    now: datetime | None = None,
) -> ChatTurnOutcome | None:
    """Read-only idempotency lookup executed before lease contention, persona, or model calls.

    Returns `None` on cache miss or expired terminal record so the caller proceeds as a new turn.
    Raises `409 idempotency_key_conflict` on digest mismatch, `403` for disabled/deleting users,
    `404` when the linked conversation is deleting/missing, or the saved terminal error.
    """
    async with session_factory() as session:
        current_time = normalize_utc(now) if now is not None else await get_database_now(session)
        await users_service.ensure_user_active_in_session(session, user_id=owner_id)
        row = await _get_record_in_session(
            session,
            user_id=owner_id,
            method=spec.method,
            canonical_route=spec.canonical_route,
            key=spec.key,
            for_update=False,
        )
        if row is None:
            return None
        record = IdempotencyRecordDTO.model_validate(row)
        if is_record_expired(record, current_time=current_time):
            return None
        if record.request_digest != spec.request_digest:
            raise ConflictError(
                code="idempotency_key_conflict",
                message="idempotency key was already used with different request parameters",
            )
        detail = await load_detail_in_session(
            session,
            owner_id,
            record.conversation_id,
            record.generation_id,
        )
        return resolve_replay_from_detail(detail, response_status_code=record.response_status_code)


async def get_idempotency_record_for_generation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    generation_id: UUID,
) -> IdempotencyRecordDTO | None:
    """Read an idempotency record by `generation_id` (used by tests and diagnostics)."""
    async with session_factory() as session:
        row = await _get_record_by_generation_in_session(session, generation_id=generation_id)
        return IdempotencyRecordDTO.model_validate(row) if row is not None else None


@dataclass(frozen=True, slots=True)
class IdempotencyPurgeBatch:
    """Result of one bounded expired-idempotency retention scan batch."""

    deleted_count: int
    scanned_count: int
    next_cursor: tuple[datetime, UUID] | None
    has_more: bool


async def purge_expired_idempotency_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> IdempotencyPurgeBatch:
    """Delete one bounded batch of expired terminal idempotency rows using a stable cursor.

    Active (`running`) records, unexpired terminal records, linked active generations, and all
    parent conversations/generations/messages remain untouched.
    """
    utc_now = normalize_utc(current_time)
    bounded = max(1, min(500, batch_size))
    base_conditions = [
        IdempotencyRecord.status.in_(("succeeded", "failed", "cancelled")),
        IdempotencyRecord.terminal_at.is_not(None),
        IdempotencyRecord.expires_at.is_not(None),
        IdempotencyRecord.expires_at <= utc_now,
        ~exists(
            select(Generation.id).where(
                Generation.id == IdempotencyRecord.generation_id,
                Generation.status == "running",
            )
        ),
    ]
    conditions = list(base_conditions)
    if cursor is not None:
        cursor_ts, cursor_id = normalize_utc(cursor[0]), cursor[1]
        conditions.append(
            tuple_(IdempotencyRecord.expires_at, IdempotencyRecord.id) > (cursor_ts, cursor_id)
        )

    stmt = (
        select(IdempotencyRecord.id, IdempotencyRecord.expires_at)
        .where(*conditions)
        .order_by(IdempotencyRecord.expires_at.asc(), IdempotencyRecord.id.asc())
        .limit(bounded)
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).all())
    if not rows:
        return IdempotencyPurgeBatch(
            deleted_count=0,
            scanned_count=0,
            next_cursor=None,
            has_more=False,
        )

    ids_to_delete = [row.id for row in rows]
    await session.execute(delete(IdempotencyRecord).where(IdempotencyRecord.id.in_(ids_to_delete)))

    last_row = rows[-1]
    assert last_row.expires_at is not None
    next_cursor = (normalize_utc(last_row.expires_at), last_row.id)
    has_more = bool(
        await session.scalar(
            select(
                exists(
                    select(IdempotencyRecord.id).where(
                        *base_conditions,
                        tuple_(IdempotencyRecord.expires_at, IdempotencyRecord.id) > next_cursor,
                    )
                )
            )
        )
    )
    return IdempotencyPurgeBatch(
        deleted_count=len(ids_to_delete),
        scanned_count=len(rows),
        next_cursor=next_cursor if has_more else None,
        has_more=has_more,
    )
