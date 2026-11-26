"""内置认证 HTTP 路由：注册、登录、刷新、注销与改密（Router 仅做 HTTP 适配）。"""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from mem_ai.api.dependencies import SessionFactoryDep, SettingsDep
from mem_ai.api.responses import ApiResponse, ok_response
from mem_ai.modules.auth import service as auth_service
from mem_ai.modules.auth.dependencies import AuthContextDep, OptionalAuthContextDep
from mem_ai.modules.auth.schemas import (
    AuthTokenSettings,
    ChangePasswordRequest,
    ChangePasswordResponse,
    LoginRequest,
    LoginResponse,
    LogoutRequest,
    RefreshRequest,
    RefreshResponse,
    RegisterRequest,
    RegisterResponse,
)

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post(
    "/register",
    response_model=ApiResponse[RegisterResponse],
    status_code=status.HTTP_201_CREATED,
)
async def register(
    body: RegisterRequest,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ApiResponse[RegisterResponse]:
    """注册本地用户；关闭自助注册返回 403，邮箱已存在返回 409。"""
    result = await auth_service.register_user(
        session_factory,
        email=body.email,
        password=body.password,
        token_settings=AuthTokenSettings.from_settings(settings),
    )
    return ok_response(result)


@router.post("/login", response_model=ApiResponse[LoginResponse])
async def login(
    body: LoginRequest,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ApiResponse[LoginResponse]:
    """邮箱密码登录；未知邮箱与错误密码统一 401，非 active 用户 403。"""
    result = await auth_service.login_user(
        session_factory,
        email=body.email,
        password=body.password,
        token_settings=AuthTokenSettings.from_settings(settings),
    )
    return ok_response(result)


@router.post("/refresh", response_model=ApiResponse[RefreshResponse])
async def refresh_token(
    body: RefreshRequest,
    session_factory: SessionFactoryDep,
    settings: SettingsDep,
) -> ApiResponse[RefreshResponse]:
    """原子轮换 Refresh Token；重放旧令牌吊销整个会话家族并返回 401。"""
    result = await auth_service.refresh_tokens(
        session_factory,
        refresh_token=body.refresh_token,
        token_settings=AuthTokenSettings.from_settings(settings),
    )
    return ok_response(result)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    body: LogoutRequest,
    session_factory: SessionFactoryDep,
    auth: OptionalAuthContextDep,
) -> Response:
    """按 `refreshToken` 吊销单会话（无需 Access Token），或 `allSessions=true` 吊销全部会话。"""
    await auth_service.logout(
        session_factory,
        refresh_token=body.refresh_token,
        user_id=auth.user_id if auth is not None else None,
        logout_all=body.all_sessions,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/password", response_model=ApiResponse[ChangePasswordResponse])
async def change_password(
    body: ChangePasswordRequest,
    session_factory: SessionFactoryDep,
    auth: AuthContextDep,
) -> ApiResponse[ChangePasswordResponse]:
    """校验当前密码并改密，同时吊销其余活跃登录会话。"""
    result = await auth_service.change_password(
        session_factory,
        user_id=auth.user_id,
        current_password=body.current_password,
        new_password=body.new_password,
        current_session_id=auth.session_id,
    )
    return ok_response(result)
