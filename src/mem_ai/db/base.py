"""Declarative ORM base, deterministic constraint naming, and common column mixins."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, MetaData, func, select
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_PG_UNIQUE_VIOLATION_SQLSTATE = "23505"


def normalize_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def get_database_now(session: AsyncSession) -> datetime:
    """Return PostgreSQL's authoritative UTC transaction timestamp (`SELECT now()`)."""
    db_now = await session.scalar(select(func.now()))
    if not isinstance(db_now, datetime):
        return datetime.now(UTC)
    return normalize_utc(db_now)


def is_unique_violation(exc: IntegrityError, *, constraint_name: str) -> bool:
    """Check whether `exc` is a PostgreSQL unique violation on `constraint_name`."""
    orig = exc.orig
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    if sqlstate == _PG_UNIQUE_VIOLATION_SQLSTATE:
        diag = getattr(orig, "diag", None)
        constraint = getattr(diag, "constraint_name", None) if diag is not None else None
        if constraint is None or constraint == constraint_name:
            return True
    return constraint_name in str(exc)


NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Project ORM base using deterministic PostgreSQL constraint naming."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class UUIDPrimaryKeyMixin:
    """Application-generated UUID4 primary key mixin."""

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )


class CreatedAtMixin:
    """UTC creation timestamp populated by PostgreSQL default."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class TimestampMixin(CreatedAtMixin):
    """UTC creation and update timestamps for mutable tables."""

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
