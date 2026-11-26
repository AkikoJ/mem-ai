"""Pydantic schemas and value objects for built-in authentication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from mem_ai.core.config import Settings


class RegisterRequest(BaseModel):
    """POST /v1/auth/register 请求；密码强度由 `tokens.validate_password_strength` 校验。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class LoginRequest(BaseModel):
    """POST /v1/auth/login 请求。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class RefreshRequest(BaseModel):
    """POST /v1/auth/refresh 请求。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    refresh_token: str = Field(alias="refreshToken", min_length=1, max_length=512)

    @field_validator("refresh_token")
    @classmethod
    def _strip_token(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("refreshToken must not be blank")
        return stripped


class LogoutRequest(BaseModel):
    """POST /v1/auth/logout 请求：`refreshToken` 吊销单会话，`allSessions=true` 吊销全部会话。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    refresh_token: str | None = Field(default=None, alias="refreshToken", max_length=512)
    all_sessions: bool = Field(default=False, alias="allSessions")


class ChangePasswordRequest(BaseModel):
    """POST /v1/auth/password 请求。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    current_password: str = Field(alias="currentPassword", min_length=1, max_length=256)
    new_password: str = Field(alias="newPassword", min_length=1, max_length=256)


class TokenPair(BaseModel):
    """短期 Access Token 与可撤销 Refresh Token。"""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    access_token: str = Field(alias="accessToken", serialization_alias="accessToken")
    refresh_token: str = Field(alias="refreshToken", serialization_alias="refreshToken")
    expires_in: int = Field(alias="expiresIn", serialization_alias="expiresIn")


class RegisterResponse(BaseModel):
    """POST /v1/auth/register 成功响应。"""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    user_id: UUID = Field(alias="userId", serialization_alias="userId")
    email: str
    tokens: TokenPair


class LoginResponse(BaseModel):
    """POST /v1/auth/login 成功响应。"""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    user_id: UUID = Field(alias="userId", serialization_alias="userId")
    email: str
    tokens: TokenPair


class RefreshResponse(BaseModel):
    """POST /v1/auth/refresh 成功响应。"""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    tokens: TokenPair


class ChangePasswordResponse(BaseModel):
    """POST /v1/auth/password 成功响应。"""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    message: str
    revoked_sessions: int = Field(alias="revokedSessions", serialization_alias="revokedSessions")


@dataclass(frozen=True, slots=True)
class AuthTokenSettings:
    """令牌签发相关配置值对象，避免在路由与服务间成组传递。"""

    jwt_secret: str
    access_token_ttl: int
    refresh_token_ttl: int
    allow_self_registration: bool
    jwt_leeway_seconds: int = 0

    @classmethod
    def from_settings(cls, settings: Settings) -> AuthTokenSettings:
        return cls(
            jwt_secret=settings.auth_jwt_secret.get_secret_value().strip(),
            access_token_ttl=settings.auth_access_token_expires_in,
            refresh_token_ttl=settings.auth_refresh_token_expires_in,
            allow_self_registration=settings.auth_allow_self_registration,
            jwt_leeway_seconds=settings.auth_jwt_leeway_seconds,
        )


@dataclass(frozen=True, slots=True)
class AuthSessionPurgeBatch:
    """一次有界登录会话保留清理批次的结果。"""

    deleted_count: int
    scanned_count: int
    next_cursor: tuple[datetime, UUID] | None
    has_more: bool
