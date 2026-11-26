"""内置认证的纯函数工具：邮箱规范化、密码策略与哈希、Refresh Token 摘要及 Access Token（JWT）。

本文件不依赖数据库与 SQLAlchemy，所有持久化由 `auth/repository.py` 与 `auth/service.py` 负责。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import secrets
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from mem_ai.core.exceptions import UnauthorizedError, UnprocessableEntityError

MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
PBKDF2_ITERATIONS = 200_000
PBKDF2_SALT_BYTES = 16
REFRESH_TOKEN_BYTES = 32

BUILTIN_JWT_ALGORITHM = "HS256"
BUILTIN_JWT_ISSUER = "mem-ai-builtin"
BUILTIN_JWT_TOKEN_TYPE = "access"

_PASSWORD_HASH_SCHEME = "pbkdf2_sha256"


def normalize_email(email: str) -> str:
    """去除首尾空白并转换为小写，作为 `users.email` 的唯一规范化形式。"""
    return email.strip().lower()


def validate_password_strength(password: str) -> None:
    """校验密码长度与字符类别，不符合时抛出 422 `weak_password`。"""
    if len(password) < MIN_PASSWORD_LENGTH:
        raise UnprocessableEntityError(
            code="weak_password",
            message=f"password must be at least {MIN_PASSWORD_LENGTH} characters",
        )
    if len(password) > MAX_PASSWORD_LENGTH:
        raise UnprocessableEntityError(
            code="weak_password",
            message=f"password must be at most {MAX_PASSWORD_LENGTH} characters",
        )
    if not password.strip():
        raise UnprocessableEntityError(
            code="weak_password",
            message="password must not be blank",
        )
    categories = sum(
        (
            any(ch.isalpha() for ch in password),
            any(ch.isdigit() for ch in password),
            any(not ch.isalnum() and not ch.isspace() for ch in password),
        )
    )
    if categories < 2:
        raise UnprocessableEntityError(
            code="weak_password",
            message="password must combine at least two of letters, digits and symbols",
        )


def b64url_encode(data: bytes) -> str:
    """URL 安全 base64 编码（无填充）。"""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64url_decode(segment: str) -> bytes:
    """严格解码 URL 安全 base64，格式错误统一映射为 401。"""
    try:
        padded = segment + "=" * (-len(segment) % 4)
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="malformed credential encoding",
        ) from exc


def hash_password(password: str) -> str:
    """使用 PBKDF2-HMAC-SHA256 与随机盐哈希密码。

    格式：`pbkdf2_sha256$<iterations>$<salt_b64>$<key_b64>`。
    """
    salt = secrets.token_bytes(PBKDF2_SALT_BYTES)
    key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"{_PASSWORD_HASH_SCHEME}${PBKDF2_ITERATIONS}${b64url_encode(salt)}${b64url_encode(key)}"


def verify_password(password: str, password_hash: str) -> bool:
    """固定时间校验密码；哈希格式非法时返回 False。"""
    parts = password_hash.split("$")
    if len(parts) != 4 or parts[0] != _PASSWORD_HASH_SCHEME:
        return False
    try:
        iterations = int(parts[1])
        if iterations <= 0:
            return False
        salt = b64url_decode(parts[2])
        expected = b64url_decode(parts[3])
    except (ValueError, UnauthorizedError):
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


_DUMMY_PASSWORD_HASH = hash_password("dummy-timing-equalizer-0000")


def verify_dummy_password(password: str) -> None:
    """对未知用户执行等量 PBKDF2 计算，降低邮箱枚举的时间侧信道。"""
    verify_password(password, _DUMMY_PASSWORD_HASH)


def generate_refresh_token() -> str:
    """生成 256 位高熵 Refresh Token。"""
    return b64url_encode(secrets.token_bytes(REFRESH_TOKEN_BYTES))


def hash_refresh_token(token: str) -> str:
    """计算 Refresh Token 的 SHA-256 十六进制摘要（仅摘要入库）。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BuiltInTokenClaims:
    """已验签的内置 Access Token 声明。"""

    user_id: UUID
    email: str
    session_id: UUID | None
    expires_at: int
    issuer: str


def _json_segment(value: dict[str, Any]) -> str:
    return b64url_encode(json.dumps(value, separators=(",", ":")).encode("utf-8"))


def _sign(secret: str, signing_input: str) -> bytes:
    return hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()


def create_access_token(
    *,
    user_id: UUID,
    email: str,
    secret: str,
    expires_in_seconds: int,
    session_id: UUID | None = None,
) -> str:
    """签发 HS256 内置 Access Token，可携带当前登录会话 ID（`sid`）。"""
    now = int(time.time())
    payload: dict[str, Any] = {
        "iss": BUILTIN_JWT_ISSUER,
        "sub": str(user_id),
        "email": email,
        "typ": BUILTIN_JWT_TOKEN_TYPE,
        "iat": now,
        "exp": now + expires_in_seconds,
    }
    if session_id is not None:
        payload["sid"] = str(session_id)
    header = {"alg": BUILTIN_JWT_ALGORITHM, "typ": "JWT"}
    signing_input = f"{_json_segment(header)}.{_json_segment(payload)}"
    return f"{signing_input}.{b64url_encode(_sign(secret, signing_input))}"


def _decode_json_object(segment: str) -> dict[str, Any]:
    try:
        decoded = json.loads(b64url_decode(segment).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="malformed JWT JSON payload",
        ) from exc
    if not isinstance(decoded, dict):
        raise UnauthorizedError(code="invalid_credentials", message="invalid JWT structure")
    return decoded


def try_peek_jwt_issuer(token: str) -> str | None:
    """不验签读取 `iss`，仅用于将内置令牌分派给 `verify_access_token` 完整验签。"""
    segments = token.split(".")
    if len(segments) != 3:
        return None
    try:
        payload = _decode_json_object(segments[1])
    except UnauthorizedError:
        return None
    issuer = payload.get("iss")
    return issuer if isinstance(issuer, str) else None


def _parse_uuid_claim(value: object, *, name: str) -> UUID:
    try:
        return UUID(str(value))
    except ValueError as exc:
        raise UnauthorizedError(
            code="invalid_credentials",
            message=f"JWT {name} claim is invalid",
        ) from exc


def verify_access_token(
    token: str,
    *,
    secret: str,
    leeway_seconds: int = 0,
) -> BuiltInTokenClaims:
    """验签并解析内置 Access Token，校验算法、签发者、类型与过期时间。"""
    segments = token.split(".")
    if len(segments) != 3 or any(not seg for seg in segments):
        raise UnauthorizedError(code="invalid_credentials", message="malformed JWT token")
    header_seg, payload_seg, signature_seg = segments

    header = _decode_json_object(header_seg)
    if header.get("alg") != BUILTIN_JWT_ALGORITHM:
        raise UnauthorizedError(
            code="invalid_credentials",
            message="unsupported JWT signing algorithm",
        )

    expected = _sign(secret, f"{header_seg}.{payload_seg}")
    if not hmac.compare_digest(expected, b64url_decode(signature_seg)):
        raise UnauthorizedError(code="invalid_credentials", message="invalid JWT signature")

    payload = _decode_json_object(payload_seg)
    if payload.get("iss") != BUILTIN_JWT_ISSUER or payload.get("typ") != BUILTIN_JWT_TOKEN_TYPE:
        raise UnauthorizedError(code="invalid_credentials", message="unexpected JWT issuer or type")

    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or isinstance(exp, bool) or not math.isfinite(float(exp)):
        raise UnauthorizedError(
            code="invalid_credentials",
            message="JWT missing finite numeric exp claim",
        )
    if time.time() - leeway_seconds >= float(exp):
        raise UnauthorizedError(code="expired_credentials", message="access token has expired")

    email = payload.get("email", "")
    if not isinstance(email, str):
        raise UnauthorizedError(code="invalid_credentials", message="JWT email claim is invalid")
    raw_sid = payload.get("sid")
    return BuiltInTokenClaims(
        user_id=_parse_uuid_claim(payload.get("sub"), name="sub"),
        email=email,
        session_id=_parse_uuid_claim(raw_sid, name="sid") if raw_sid is not None else None,
        expires_at=int(exp),
        issuer=BUILTIN_JWT_ISSUER,
    )
