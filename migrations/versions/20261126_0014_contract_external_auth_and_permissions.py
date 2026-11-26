"""contract external auth identities, model required_permission, and tighten users credentials

Revision ID: 20261126_0014
Revises: 20261126_0013
Create Date: 2026-11-26 22:50:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261126_0014"
down_revision: str | Sequence[str] | None = "20261126_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Contract legacy external-identity & permission schema and enforce built-in user columns."""
    # 1. Deterministically backfill any historical V1 user rows created before built-in credentials.
    #    - RFC 2606 `.invalid` domain guarantees zero collision with real user emails.
    #    - `!unusable-legacy-migrated` never matches `pbkdf2_sha256$...` in `verify_password`.
    op.execute(
        sa.text(
            "UPDATE users "
            "SET email = 'legacy-' || id::text || '@migrated.invalid' "
            "WHERE email IS NULL OR email = ''"
        )
    )
    op.execute(
        sa.text(
            "UPDATE users "
            "SET password_hash = '!unusable-legacy-migrated' "
            "WHERE password_hash IS NULL OR password_hash = ''"
        )
    )

    # 2. Enforce non-null credentials and replace partial unique index with full unique index.
    op.drop_constraint(op.f("ck_users_email_lowercase"), "users", type_="check")
    op.drop_index("ix_users_email_unique", table_name="users")
    op.alter_column("users", "email", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("users", "password_hash", existing_type=sa.String(length=255), nullable=False)
    op.create_index("ix_users_email_unique", "users", ["email"], unique=True)
    op.create_check_constraint(
        op.f("ck_users_email_lowercase"),
        "users",
        "email <> '' AND email = lower(email)",
    )

    # 3. Drop retired external identity mapping table.
    op.drop_index("ix_user_identities_user_id", table_name="user_identities")
    op.drop_table("user_identities")

    # 4. Drop retired model permission column.
    op.drop_column("model_configs", "required_permission")


def downgrade() -> None:
    """Restore legacy external-identity table, model required_permission, and nullable columns."""
    op.add_column(
        "model_configs",
        sa.Column("required_permission", sa.String(length=128), nullable=True),
    )

    op.create_table(
        "user_identities",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("issuer", sa.String(length=255), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_user_identities_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_user_identities")),
        sa.UniqueConstraint(
            "issuer",
            "subject",
            name=op.f("uq_user_identities_issuer_subject"),
        ),
    )
    op.create_index("ix_user_identities_user_id", "user_identities", ["user_id"], unique=False)

    op.drop_constraint(op.f("ck_users_email_lowercase"), "users", type_="check")
    op.drop_index("ix_users_email_unique", table_name="users")
    op.alter_column("users", "password_hash", existing_type=sa.String(length=255), nullable=True)
    op.alter_column("users", "email", existing_type=sa.String(length=255), nullable=True)
    op.create_index(
        "ix_users_email_unique",
        "users",
        ["email"],
        unique=True,
        postgresql_where=sa.text("email IS NOT NULL"),
    )
    op.create_check_constraint(
        op.f("ck_users_email_lowercase"),
        "users",
        "email IS NULL OR email = lower(email)",
    )
