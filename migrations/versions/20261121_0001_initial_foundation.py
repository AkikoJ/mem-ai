"""initial foundation baseline

Revision ID: 20261121_0001
Revises:
Create Date: 2026-11-21 15:52:00+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "20261121_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Establish the baseline Alembic revision for migration readiness checks."""
    return None


def downgrade() -> None:
    """Revert the baseline revision."""
    return None
