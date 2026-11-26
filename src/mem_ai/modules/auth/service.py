"""内置认证公开用例：注册、登录、刷新轮换、注销、改密与登录会话保留清理。

本模块拥有 `auth_sessions` 的写入入口与事务阶段；SQL 在 `repository.py`，密码/JWT 纯函数在
`tokens.py`。用户行读写通过 `users.service` 公开入口协作。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import (
    ForbiddenError,
    UnauthorizedError,
    UnprocessableEntityError,
)
from mem_ai.db.base import get_database_now, normalize_utc
from mem_ai.modules.auth import repository, tokens
from mem_ai.modules.auth.schemas import (
    AuthSessionPurgeBatch,
    AuthTokenSettings,
    ChangePasswordResponse,
    LoginResponse,
    RefreshResponse,
    RegisterResponse,
    TokenPair,
)
from mem_ai.modules.users import service as users_service

_INVALID_LOGIN_MESSAGE = "invalid email or password"


async def _issue_token_pair_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    email: str,
    token_settings: AuthTokenSettings,
    now: datetime,
) -> TokenPair:
    """在调用方事务内创建登录会话并签发令牌对。"""
    refresh_token = tokens.generate_refresh_token()
    auth_session = await repository.create_session(
        session,
        user_id=user_id,
        token_hash=tokens.hash_refresh_token(refresh_token),
        expires_at=now + timedelta(seconds=token_settings.refresh_token_ttl),
        last_used_at=now,
    )
    access_token = tokens.create_access_token(
        user_id=user_id,
        email=email,
        secret=token_settings.jwt_secret,
        expires_in_seconds=token_settings.access_token_ttl,
        session_id=auth_session.id,
    )
    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=token_settings.access_token_ttl,
    )


async def register_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    email: str,
    password: str,
    token_settings: AuthTokenSettings,
) -> RegisterResponse:
    """注册 `active` 本地用户并签发首个登录会话。"""
    if not token_settings.allow_self_registration:
        raise ForbiddenError(
            code="registration_disabled",
            message="self-registration is disabled",
        )
    normalized_email = tokens.normalize_email(email)
    tokens.validate_password_strength(password)
    password_hash = tokens.hash_password(password)

    async with session_factory.begin() as session:
        now = await get_database_now(session)
        user = await users_service.create_user_with_password(
            session,
            email=normalized_email,
            password_hash=password_hash,
        )
        token_pair = await _issue_token_pair_in_session(
            session,
            user_id=user.id,
            email=normalized_email,
            token_settings=token_settings,
            now=now,
        )
    return RegisterResponse(user_id=user.id, email=normalized_email, tokens=token_pair)


async def login_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    email: str,
    password: str,
    token_settings: AuthTokenSettings,
) -> LoginResponse:
    """校验邮箱密码并创建登录会话；未知邮箱与错误密码返回一致 401。"""
    normalized_email = tokens.normalize_email(email)
    async with session_factory() as session:
        credentials = await users_service.get_user_credentials_by_email_in_session(
            session,
            email=normalized_email,
        )

    if credentials is None or credentials.password_hash is None:
        tokens.verify_dummy_password(password)
        raise UnauthorizedError(code="invalid_credentials", message=_INVALID_LOGIN_MESSAGE)
    if not tokens.verify_password(password, credentials.password_hash):
        raise UnauthorizedError(code="invalid_credentials", message=_INVALID_LOGIN_MESSAGE)

    async with session_factory.begin() as session:
        # Keep deletion behind this issuance, or observe its committed deleting status first.
        active_user = await users_service.ensure_user_active_in_session(
            session,
            user_id=credentials.id,
            for_share=True,
        )
        now = await get_database_now(session)
        resolved_email = active_user.email or normalized_email
        token_pair = await _issue_token_pair_in_session(
            session,
            user_id=active_user.id,
            email=resolved_email,
            token_settings=token_settings,
            now=now,
        )
    return LoginResponse(user_id=active_user.id, email=resolved_email, tokens=token_pair)


async def refresh_tokens(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    refresh_token: str,
    token_settings: AuthTokenSettings,
) -> RefreshResponse:
    """原子轮换 Refresh Token；重放已失效令牌时提交家族吊销后返回 401。"""
    token_hash = tokens.hash_refresh_token(refresh_token)
    replay_detected = False
    token_pair: TokenPair | None = None

    # Discover the immutable owner without retaining a stale ORM snapshot in the write session.
    async with session_factory() as session:
        observed = await repository.get_session_by_token_hash(session, token_hash=token_hash)
        if observed is None:
            raise UnauthorizedError(code="invalid_credentials", message="invalid refresh token")
        owner_id = observed.user_id

    async with session_factory.begin() as session:
        # Deletion and password changes lock user -> sessions. Use the same order, but defer
        # active-status validation until after the replay branch so revoked tokens stay 401.
        await users_service.get_user_by_id_in_session(session, user_id=owner_id, for_update=True)
        old_session = await repository.get_session_by_token_hash(
            session,
            token_hash=token_hash,
            for_update=True,
        )
        if old_session is None:
            raise UnauthorizedError(code="invalid_credentials", message="invalid refresh token")
        now = await get_database_now(session)

        if old_session.revoked_at is not None:
            # 先提交家族吊销，再在事务外返回 401，避免回滚吊销。
            await repository.revoke_session_family(
                session,
                root_session_id=old_session.id,
                revoked_at=now,
            )
            replay_detected = True
        else:
            if normalize_utc(old_session.expires_at) <= now:
                raise UnauthorizedError(
                    code="expired_credentials",
                    message="refresh token has expired",
                )
            active_user = await users_service.ensure_user_active_in_session(
                session,
                user_id=old_session.user_id,
            )
            new_refresh_token = tokens.generate_refresh_token()
            new_session = await repository.create_session(
                session,
                user_id=active_user.id,
                token_hash=tokens.hash_refresh_token(new_refresh_token),
                expires_at=now + timedelta(seconds=token_settings.refresh_token_ttl),
                last_used_at=now,
            )
            await repository.mark_session_rotated(
                session,
                old_session=old_session,
                new_session_id=new_session.id,
                revoked_at=now,
            )
            token_pair = TokenPair(
                access_token=tokens.create_access_token(
                    user_id=active_user.id,
                    email=active_user.email or "",
                    secret=token_settings.jwt_secret,
                    expires_in_seconds=token_settings.access_token_ttl,
                    session_id=new_session.id,
                ),
                refresh_token=new_refresh_token,
                expires_in=token_settings.access_token_ttl,
            )

    if replay_detected or token_pair is None:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="refresh token has been revoked",
        )
    return RefreshResponse(tokens=token_pair)


async def logout(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    refresh_token: str | None,
    user_id: UUID | None,
    logout_all: bool,
) -> None:
    """吊销当前登录会话或用户全部会话；单会话注销幂等。"""
    if logout_all:
        if user_id is None:
            raise UnauthorizedError(
                code="missing_credentials",
                message="access token required to revoke all sessions",
            )
        async with session_factory.begin() as session:
            await revoke_user_sessions_in_session(session, user_id=user_id)
        return

    cleaned = refresh_token.strip() if refresh_token is not None else ""
    if not cleaned:
        raise UnprocessableEntityError(
            code="invalid_logout_request",
            message="either refreshToken or allSessions=true is required",
        )

    async with session_factory.begin() as session:
        now = await get_database_now(session)
        auth_session = await repository.get_session_by_token_hash(
            session,
            token_hash=tokens.hash_refresh_token(cleaned),
            for_update=True,
        )
        if auth_session is None or auth_session.revoked_at is not None:
            return
        if user_id is not None and auth_session.user_id != user_id:
            raise ForbiddenError(
                code="session_owner_mismatch",
                message="refresh token does not belong to current user",
            )
        await repository.revoke_session_by_id(
            session,
            session_id=auth_session.id,
            revoked_at=now,
        )


async def change_password(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: UUID,
    current_password: str,
    new_password: str,
    current_session_id: UUID | None = None,
) -> ChangePasswordResponse:
    """校验当前密码、更新哈希并吊销其余活跃登录会话。"""
    tokens.validate_password_strength(new_password)
    async with session_factory() as session:
        credentials = await users_service.get_user_credentials_by_id_in_session(
            session,
            user_id=user_id,
        )

    if credentials is None or credentials.password_hash is None:
        tokens.verify_dummy_password(current_password)
        raise UnauthorizedError(code="invalid_credentials", message="invalid current password")
    if not tokens.verify_password(current_password, credentials.password_hash):
        raise UnauthorizedError(code="invalid_credentials", message="invalid current password")
    new_password_hash = tokens.hash_password(new_password)

    async with session_factory.begin() as session:
        await users_service.ensure_user_active_in_session(session, user_id=user_id)
        await users_service.update_user_password_hash(
            session,
            user_id=user_id,
            password_hash=new_password_hash,
        )
        revoked_count = await revoke_user_sessions_in_session(
            session,
            user_id=user_id,
            exclude_session_id=current_session_id,
        )
    return ChangePasswordResponse(message="password updated", revoked_sessions=revoked_count)


async def revoke_user_sessions_in_session(
    session: AsyncSession,
    *,
    user_id: UUID,
    revoked_at: datetime | None = None,
    exclude_session_id: UUID | None = None,
) -> int:
    """在调用方事务内吊销用户活跃登录会话（供用户删除受理等流程复用）。"""
    effective_time = revoked_at if revoked_at is not None else await get_database_now(session)
    return await repository.revoke_user_sessions(
        session,
        user_id=user_id,
        revoked_at=effective_time,
        exclude_session_id=exclude_session_id,
    )


async def purge_expired_auth_sessions_batch_in_session(
    session: AsyncSession,
    *,
    current_time: datetime,
    retention_days: int,
    batch_size: int = 100,
    cursor: tuple[datetime, UUID] | None = None,
) -> AuthSessionPurgeBatch:
    """删除一个有界批次的已过期或长期已吊销登录会话，保护活跃会话。"""
    deleted, scanned, next_cursor, has_more = await repository.purge_expired_auth_sessions_batch(
        session,
        current_time=current_time,
        revoked_cutoff=current_time - timedelta(days=retention_days),
        batch_size=batch_size,
        cursor=cursor,
    )
    return AuthSessionPurgeBatch(
        deleted_count=deleted,
        scanned_count=scanned,
        next_cursor=next_cursor,
        has_more=has_more,
    )
