"""add jobs, operations, memory_scopes, and memory_list_snapshots tables

Revision ID: 20261121_0004
Revises: 20261121_0003
Create Date: 2026-11-21 20:30:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261121_0004"
down_revision: str | Sequence[str] | None = "20261121_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `jobs`, `operations`, `memory_scopes`, and `memory_list_snapshots` tables."""
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("business_id", sa.String(length=128), nullable=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("dedupe_key", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            server_default=sa.text("5"),
            nullable=False,
        ),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "next_run_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_ref", sa.String(length=255), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_summary", sa.Text(), nullable=True),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'reconciling', "
            "'succeeded', 'failed', 'cancelled')",
            name=op.f("ck_jobs_status_valid"),
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=op.f("ck_jobs_attempt_count_non_negative"),
        ),
        sa.CheckConstraint(
            "max_attempts >= 1",
            name=op.f("ck_jobs_max_attempts_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_jobs_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
        sa.UniqueConstraint("dedupe_key", name=op.f("uq_jobs_dedupe_key")),
    )
    op.create_index("ix_jobs_business_id", "jobs", ["business_id"], unique=False)
    op.create_index("ix_jobs_external_ref", "jobs", ["external_ref"], unique=False)
    op.create_index("ix_jobs_status_next_run_at", "jobs", ["status", "next_run_at"], unique=False)
    op.create_index(
        "ix_jobs_user_id_job_type_status",
        "jobs",
        ["user_id", "job_type", "status"],
        unique=False,
    )

    op.create_table(
        "operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("operation_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("target_type", sa.String(length=64), nullable=True),
        sa.Column("target_id", sa.String(length=255), nullable=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "progress",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "result_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=500), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'reconciling', 'succeeded', 'failed')",
            name=op.f("ck_operations_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name=op.f("fk_operations_job_id_jobs"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_operations_owner_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_operations")),
    )
    op.create_index(
        "ix_operations_owner_id_created_at",
        "operations",
        ["owner_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_operations_status_updated_at",
        "operations",
        ["status", "updated_at"],
        unique=False,
    )

    op.create_table(
        "memory_scopes",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", sa.String(length=64), nullable=False),
        sa.Column("epoch", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "barrier_state",
            sa.String(length=32),
            server_default="idle",
            nullable=False,
        ),
        sa.Column("barrier_operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("epoch >= 0", name=op.f("ck_memory_scopes_epoch_non_negative")),
        sa.CheckConstraint(
            "barrier_state IN ('idle', 'clearing', 'reconciling')",
            name=op.f("ck_memory_scopes_barrier_state_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["barrier_operation_id"],
            ["operations.id"],
            name=op.f("fk_memory_scopes_barrier_operation_id_operations"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_memory_scopes_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_memory_scopes")),
        sa.UniqueConstraint(
            "user_id",
            "provider_id",
            name=op.f("uq_memory_scopes_user_id_provider_id"),
        ),
    )

    op.create_table(
        "memory_list_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_id", sa.String(length=64), nullable=False),
        sa.Column("filters_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "items_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("total_items", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "total_items >= 0",
            name=op.f("ck_memory_list_snapshots_total_items_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_memory_list_snapshots_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_memory_list_snapshots")),
    )
    op.create_index(
        "ix_memory_list_snapshots_expires_at",
        "memory_list_snapshots",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_memory_list_snapshots_user_provider_filters",
        "memory_list_snapshots",
        ["user_id", "provider_id", "filters_hash"],
        unique=False,
    )


def downgrade() -> None:
    """Drop memory, operations, and jobs tables cleanly."""
    op.drop_index(
        "ix_memory_list_snapshots_user_provider_filters",
        table_name="memory_list_snapshots",
    )
    op.drop_index("ix_memory_list_snapshots_expires_at", table_name="memory_list_snapshots")
    op.drop_table("memory_list_snapshots")

    op.drop_table("memory_scopes")

    op.drop_index("ix_operations_status_updated_at", table_name="operations")
    op.drop_index("ix_operations_owner_id_created_at", table_name="operations")
    op.drop_table("operations")

    op.drop_index("ix_jobs_user_id_job_type_status", table_name="jobs")
    op.drop_index("ix_jobs_status_next_run_at", table_name="jobs")
    op.drop_index("ix_jobs_external_ref", table_name="jobs")
    op.drop_index("ix_jobs_business_id", table_name="jobs")
    op.drop_table("jobs")
