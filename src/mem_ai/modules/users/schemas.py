"""Pydantic schemas and project DTOs for the users module."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

UserStatus = Literal["active", "disabled", "deleting"]


class UserRecord(BaseModel):
    """Immutable application-layer user snapshot returned by `users.service`."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    status: UserStatus
    email: str | None = None
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class UserCredentialRecord(BaseModel):
    """Credential-bearing user snapshot restricted to built-in auth verification."""

    model_config = ConfigDict(from_attributes=True, frozen=True)

    id: UUID
    status: UserStatus
    email: str | None = None
    password_hash: str | None = None
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class UserMeResponse(BaseModel):
    """Public HTTP payload returned by `GET /v1/users/me`."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    user_id: UUID = Field(alias="userId", serialization_alias="userId")
    actor_id: str = Field(alias="actorId", serialization_alias="actorId")
    auth_method: str = Field(alias="authMethod", serialization_alias="authMethod")
    permissions: list[str]
    status: UserStatus
    created_at: datetime = Field(alias="createdAt", serialization_alias="createdAt")
