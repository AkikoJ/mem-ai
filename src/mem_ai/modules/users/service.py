"""Public use cases and shared user status policy owned by the `users` module."""

from __future__ import annotations

from datetime import datetime
from typing import TypeVar
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import ConflictError, ForbiddenError, NotFoundError
from mem_ai.db.base import is_unique_violation
from mem_ai.modules.users import repository
from mem_ai.modules.users.models import USER_EMAIL_UNIQUE_INDEX, USER_IDENTITY_UNIQUE_CONSTRAINT
from mem_ai.modules.users.schemas import UserCredentialRecord, UserRecord, UserStatus

_UserT = TypeVar("_UserT", UserRecord, UserCredentialRecord)


def assert_user_active(user: _UserT) -> _UserT:  # noqa: UP047 - tools/check_architecture.py parses on older Python
    """Enforce the canonical user status rule shared by auth, commits, and background workers."""
    if user.status == "disabled":
        raise ForbiddenError(
            code="user_disabled",
            message="user is disabled",
        )
    if user.status == "deleting":
        raise ForbiddenError(
            code="user_deleting",
            message="user is being deleted",
        )
    return user


async def ensure_user_active_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_share: bool = False,
) -> UserRecord:
    """Verify inside an existing transaction that a user still exists and is active."""
    user = await repository.get_user_by_id(session, user_id=user_id, for_share=for_share)
    if user is None:
        raise ForbiddenError(
            code="user_not_active",
            message="user is not active",
        )
    return assert_user_active(UserRecord.model_validate(user))


async def ensure_user_not_deleting_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_share: bool = False,
) -> UserRecord:
    """Verify a user exists and is not already deleting (permits disabled during DR replay)."""
    user = await repository.get_user_by_id(session, user_id=user_id, for_share=for_share)
    if user is None:
        raise ForbiddenError(code="user_not_active", message="user is not active")
    record = UserRecord.model_validate(user)
    if record.status == "deleting":
        raise ForbiddenError(code="user_deleting", message="user is being deleted")
    return record


async def resolve_or_create_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    issuer: str,
    subject: str,
) -> UserRecord:
    """Map a verified `(issuer, subject)` to a stable local user, safe under concurrent races."""
    normalized_issuer = issuer.strip()
    normalized_subject = subject.strip()
    if not normalized_issuer or not normalized_subject:
        raise ValueError("issuer and subject must be non-empty")

    async with session_factory.begin() as session:
        existing = await repository.get_user_by_identity(
            session,
            issuer=normalized_issuer,
            subject=normalized_subject,
        )
        if existing is not None:
            return UserRecord.model_validate(existing)

        conflict_exc: IntegrityError | None = None
        try:
            async with session.begin_nested():
                created = await repository.create_user_with_identity(
                    session,
                    issuer=normalized_issuer,
                    subject=normalized_subject,
                )
            return UserRecord.model_validate(created)
        except IntegrityError as exc:
            if not is_unique_violation(exc, constraint_name=USER_IDENTITY_UNIQUE_CONSTRAINT):
                raise
            conflict_exc = exc

        # Concurrent first-request winner committed the mapping; savepoint rolled back our insert.
        winner = await repository.get_user_by_identity(
            session,
            issuer=normalized_issuer,
            subject=normalized_subject,
        )
        if winner is None:
            raise ConflictError(
                code="user_identity_conflict",
                message="concurrent user identity resolution conflict",
            ) from conflict_exc
        return UserRecord.model_validate(winner)


async def resolve_active_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    issuer: str,
    subject: str,
) -> UserRecord:
    """Resolve `(issuer, subject)` to a local user and reject disabled or deleting users."""
    user = await resolve_or_create_user(
        session_factory,
        issuer=issuer,
        subject=subject,
    )
    return assert_user_active(user)


async def get_active_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
) -> UserRecord:
    """Load a local user in a short read session and verify active status."""
    async with session_factory() as session:
        return await ensure_user_active_in_session(session, user_id=user_id)


async def set_user_status(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    status: UserStatus,
    deleted_at: datetime | None = None,
) -> UserRecord:
    """Transition a local user's lifecycle status in a short transaction."""
    async with session_factory.begin() as session:
        existing = await repository.get_user_by_id(session, user_id=user_id, for_update=True)
        if existing is not None and existing.status == "deleting" and status != "deleting":
            raise ConflictError(code="user_deleting", message="user deletion cannot be reversed")
        updated = await repository.update_user_status(
            session,
            user_id=user_id,
            status=status,
            deleted_at=deleted_at,
        )
        if updated is None:
            raise NotFoundError(code="user_not_found", message="user not found")
        return UserRecord.model_validate(updated)


# ========================================
# 基于邮箱的用户管理（内置认证）
# ========================================


async def get_user_credentials_by_email_in_session(
    session: AsyncSession,
    *,
    email: str,
    for_update: bool = False,
) -> UserCredentialRecord | None:
    """按规范化邮箱读取含密码哈希的凭据快照（仅供内置认证校验使用）"""
    user = await repository.get_user_by_email(
        session,
        email=email.strip().lower(),
        for_update=for_update,
    )
    if user is None:
        return None
    return UserCredentialRecord.model_validate(user)


async def get_user_credentials_by_id_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_update: bool = False,
) -> UserCredentialRecord | None:
    """按用户 ID 读取含密码哈希的凭据快照（仅供内置认证校验使用）"""
    user = await repository.get_user_by_id(session, user_id=user_id, for_update=for_update)
    if user is None:
        return None
    return UserCredentialRecord.model_validate(user)


async def get_user_by_id_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    for_update: bool = False,
) -> UserRecord | None:
    """根据用户 ID 查找用户（不含凭据）"""
    user = await repository.get_user_by_id(session, user_id=user_id, for_update=for_update)
    if user is None:
        return None
    return UserRecord.model_validate(user)


async def get_user_by_email(
    session: AsyncSession,
    *,
    email: str,
    for_update: bool = False,
) -> UserRecord | None:
    """根据邮箱查找用户（不含凭据）"""
    user = await repository.get_user_by_email(
        session,
        email=email.strip().lower(),
        for_update=for_update,
    )
    if user is None:
        return None
    return UserRecord.model_validate(user)


async def create_user_with_password(
    session: AsyncSession,
    *,
    email: str,
    password_hash: str,
) -> UserRecord:
    """创建带邮箱密码的新用户；并发重复邮箱统一映射为 409"""
    normalized_email = email.strip().lower()
    conflict_exc: IntegrityError | None = None
    try:
        async with session.begin_nested():
            user = await repository.create_user_with_email(
                session,
                email=normalized_email,
                password_hash=password_hash,
                status="active",
            )
        return UserRecord.model_validate(user)
    except IntegrityError as exc:
        if not is_unique_violation(exc, constraint_name=USER_EMAIL_UNIQUE_INDEX):
            raise
        conflict_exc = exc
    raise ConflictError(
        code="email_already_registered",
        message="email is already registered",
    ) from conflict_exc


async def update_user_password_hash(
    session: AsyncSession,
    *,
    user_id: UUID,
    password_hash: str,
) -> UserRecord:
    """更新用户密码哈希"""
    user = await repository.update_user_password(
        session,
        user_id=user_id,
        password_hash=password_hash,
    )
    if user is None:
        raise NotFoundError(code="user_not_found", message="user not found")
    return UserRecord.model_validate(user)
