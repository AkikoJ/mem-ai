"""SQLAlchemy ORM models for `conversations`, `messages`, and `generations`."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from mem_ai.db.base import Base, CreatedAtMixin, TimestampMixin, UUIDPrimaryKeyMixin

CONVERSATION_STATUSES = ("active", "deleting")
GENERATION_STATUSES = ("running", "succeeded", "failed", "cancelled")
MESSAGE_ROLES = ("user", "assistant")

MESSAGE_SEQUENCE_UNIQUE_CONSTRAINT = "uq_messages_conversation_id_sequence"
MESSAGE_GENERATION_ROLE_UNIQUE_CONSTRAINT = "uq_messages_generation_id_role"
GENERATION_USER_MESSAGE_FK = "fk_generations_user_message_id_messages"
GENERATION_ASSISTANT_MESSAGE_FK = "fk_generations_assistant_message_id_messages"


class Conversation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Owner-scoped conversation pinning an immutable persona version snapshot reference."""

    __tablename__ = "conversations"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'deleting')", name="status_valid"),
        CheckConstraint("persona_version_number >= 1", name="persona_version_number_positive"),
        CheckConstraint("next_message_sequence >= 1", name="next_message_sequence_positive"),
        Index("ix_conversations_owner_id_created_at_id", "owner_id", "created_at", "id"),
        Index("ix_conversations_owner_id_status", "owner_id", "status"),
    )

    owner_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    # Logical references so PersonaProvider can be swapped to a remote service without DB coupling.
    agent_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    persona_version_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    persona_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default="active",
    )
    next_message_sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )


class Generation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Single generation turn tracking pinned configuration, lease, outcome, and usage."""

    __tablename__ = "generations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'cancelled')",
            name="status_valid",
        ),
        CheckConstraint("memory_epoch >= 0", name="memory_epoch_non_negative"),
        ForeignKeyConstraint(
            ["user_message_id"],
            ["messages.id"],
            ondelete="RESTRICT",
            use_alter=True,
            name=GENERATION_USER_MESSAGE_FK,
        ),
        ForeignKeyConstraint(
            ["assistant_message_id"],
            ["messages.id"],
            ondelete="RESTRICT",
            use_alter=True,
            name=GENERATION_ASSISTANT_MESSAGE_FK,
        ),
        Index(
            "ix_generations_conversation_id_created_at_id",
            "conversation_id",
            "created_at",
            "id",
        ),
        Index("ix_generations_conversation_id_status", "conversation_id", "status"),
        Index("ix_generations_owner_id_created_at_id", "owner_id", "created_at", "id"),
    )

    conversation_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    owner_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    agent_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    persona_version_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    user_message_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=True,
        unique=True,
        default=None,
    )
    assistant_message_id: Mapped[UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=True,
        unique=True,
        default=None,
    )
    model_id: Mapped[str] = mapped_column(String(128), nullable=False)
    memory_epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="running",
        server_default="running",
    )
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True, default=None)
    error_message: Mapped[str | None] = mapped_column(String(500), nullable=True, default=None)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    finish_reason: Mapped[str | None] = mapped_column(String(64), nullable=True, default=None)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True, default=None)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )


class Message(UUIDPrimaryKeyMixin, CreatedAtMixin, Base):
    """Authoritative ordered user and complete assistant messages within a conversation."""

    __tablename__ = "messages"
    __table_args__ = (
        CheckConstraint("role IN ('user', 'assistant')", name="role_valid"),
        CheckConstraint("sequence >= 1", name="sequence_positive"),
        UniqueConstraint("conversation_id", "sequence", name=MESSAGE_SEQUENCE_UNIQUE_CONSTRAINT),
        UniqueConstraint("generation_id", "role", name=MESSAGE_GENERATION_ROLE_UNIQUE_CONSTRAINT),
        Index("ix_messages_conversation_id_sequence", "conversation_id", "sequence"),
        Index("ix_messages_generation_id", "generation_id"),
    )

    conversation_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    generation_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("generations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
