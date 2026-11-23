"""add job_attempts table, lease-expiry index on jobs, and read-only memory_jobs view

Revision ID: 20261123_0008
Revises: 20261122_0007
Create Date: 2026-11-23 10:15:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20261123_0008"
down_revision: str | Sequence[str] | None = "20261122_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create `job_attempts`, expired-lease index on `jobs`, and read-only `memory_jobs` view."""
    op.create_index(
        "ix_jobs_status_lease_expires_at",
        "jobs",
        ["status", "lease_expires_at"],
        unique=False,
    )

    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="running", nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_ref", sa.String(length=255), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column(
            "result_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_number >= 1",
            name=op.f("ck_job_attempts_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'retry_wait', 'reconciling', "
            "'failed', 'cancelled', 'lease_lost')",
            name=op.f("ck_job_attempts_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name=op.f("fk_job_attempts_job_id_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_attempts")),
    )
    op.create_index(
        "ix_job_attempts_job_id_attempt_number",
        "job_attempts",
        ["job_id", "attempt_number"],
        unique=False,
    )
    op.create_index(
        "ix_job_attempts_expires_at",
        "job_attempts",
        ["expires_at"],
        unique=False,
    )

    op.execute(
        """
        CREATE VIEW memory_jobs AS
        SELECT
            id,
            job_type,
            business_id,
            user_id,
            dedupe_key,
            status,
            attempt_count,
            max_attempts,
            lease_token,
            lease_expires_at,
            next_run_at,
            started_at,
            finished_at,
            external_ref,
            last_error_code,
            last_error_summary,
            metadata_json ->> 'write_id' AS write_id,
            metadata_json ->> 'provider_id' AS provider_id,
            metadata_json ->> 'scope' AS scope,
            metadata_json ->> 'conversation_id' AS conversation_id,
            metadata_json ->> 'generation_id' AS generation_id,
            CASE
                WHEN (metadata_json ->> 'memory_epoch') ~ '^[0-9]+$'
                THEN (metadata_json ->> 'memory_epoch')::integer
                ELSE NULL
            END AS memory_epoch,
            metadata_json,
            created_at,
            updated_at
        FROM jobs
        WHERE job_type IN ('memory_turn_write', 'memory_explicit_create')
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_memory_jobs_write()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'memory_jobs is a read-only view; write through jobs instead';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_memory_jobs_read_only
        INSTEAD OF INSERT OR UPDATE OR DELETE ON memory_jobs
        FOR EACH ROW
        EXECUTE FUNCTION prevent_memory_jobs_write()
        """
    )


def downgrade() -> None:
    """Drop read-only `memory_jobs` view, `job_attempts`, and `jobs` lease-expiry index."""
    op.execute("DROP TRIGGER IF EXISTS trg_memory_jobs_read_only ON memory_jobs")
    op.execute("DROP VIEW IF EXISTS memory_jobs")
    op.execute("DROP FUNCTION IF EXISTS prevent_memory_jobs_write()")

    op.drop_index("ix_job_attempts_expires_at", table_name="job_attempts")
    op.drop_index("ix_job_attempts_job_id_attempt_number", table_name="job_attempts")
    op.drop_table("job_attempts")

    op.drop_index("ix_jobs_status_lease_expires_at", table_name="jobs")
