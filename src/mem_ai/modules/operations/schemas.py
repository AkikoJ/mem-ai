"""Application DTOs and HTTP schemas for asynchronous operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

OperationStatus = Literal["pending", "running", "reconciling", "succeeded", "failed"]


def operation_status_url(operation_id: UUID) -> str:
    """Return the canonical status polling path for an operation."""
    return f"/v1/operations/{operation_id}"


class OperationRecord(BaseModel):
    """Immutable snapshot of a persisted operation."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    owner_id: UUID | None
    operation_type: str
    status: OperationStatus
    target_type: str | None
    target_id: str | None
    job_id: UUID | None
    progress: dict[str, Any]
    result_summary: dict[str, Any]
    error_code: str | None
    error_message: str | None
    expires_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def status_url(self) -> str:
        return operation_status_url(self.id)


class OperationErrorResponse(BaseModel):
    """Sanitized error object exposed inside operation status responses."""

    model_config = ConfigDict(populate_by_name=True)

    code: str
    message: str


class OperationAcceptedResponse(BaseModel):
    """202 Accepted payload returned for async or reconciling operations."""

    model_config = ConfigDict(populate_by_name=True)

    operation_id: UUID = Field(serialization_alias="operationId")
    status: OperationStatus
    status_url: str = Field(serialization_alias="statusUrl")

    @classmethod
    def from_record(cls, record: OperationRecord) -> OperationAcceptedResponse:
        return cls(
            operation_id=record.id,
            status=record.status,
            status_url=record.status_url,
        )


class OperationResponse(BaseModel):
    """Detailed operation status payload returned by `GET /v1/operations/{operationId}`."""

    model_config = ConfigDict(populate_by_name=True)

    operation_id: UUID = Field(serialization_alias="operationId")
    operation_type: str = Field(serialization_alias="operationType")
    status: OperationStatus
    status_url: str = Field(serialization_alias="statusUrl")
    progress: dict[str, Any]
    result: dict[str, Any]
    error: OperationErrorResponse | None = None
    created_at: datetime = Field(serialization_alias="createdAt")
    updated_at: datetime = Field(serialization_alias="updatedAt")
    completed_at: datetime | None = Field(default=None, serialization_alias="completedAt")
    expires_at: datetime | None = Field(default=None, serialization_alias="expiresAt")

    @classmethod
    def from_record(cls, record: OperationRecord) -> OperationResponse:
        error_obj = (
            OperationErrorResponse(
                code=record.error_code,
                message=record.error_message or record.error_code,
            )
            if record.error_code is not None
            else None
        )
        return cls(
            operation_id=record.id,
            operation_type=record.operation_type,
            status=record.status,
            status_url=record.status_url,
            progress=dict(record.progress),
            result=dict(record.result_summary),
            error=error_obj,
            created_at=record.created_at,
            updated_at=record.updated_at,
            completed_at=record.completed_at,
            expires_at=record.expires_at,
        )
