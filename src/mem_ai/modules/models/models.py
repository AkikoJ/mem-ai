"""SQLAlchemy ORM model for server-registered `model_configs`."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Boolean, CheckConstraint, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

MODEL_CONFIG_ID_UNIQUE_CONSTRAINT = "uq_model_configs_model_id"


class ModelConfig(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Server-controlled model registration row synced from trusted deployment configuration."""

    __tablename__ = "model_configs"
    __table_args__ = (
        CheckConstraint("model_id <> ''", name="model_id_non_empty"),
        CheckConstraint("provider_id <> ''", name="provider_id_non_empty"),
        CheckConstraint("upstream_model <> ''", name="upstream_model_non_empty"),
        Index("ix_model_configs_enabled_model_id", "enabled", "model_id"),
    )

    model_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    provider_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="openai_compatible",
        server_default="openai_compatible",
    )
    upstream_model: Mapped[str] = mapped_column(String(128), nullable=False)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    supports_streaming: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    max_context_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    max_output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    required_permission: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        default=None,
    )
    credential_ref: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        default=None,
    )
    capabilities_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
