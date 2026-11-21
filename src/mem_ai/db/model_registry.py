"""Explicit ORM model registry loaded by Alembic before metadata inspection."""

from __future__ import annotations


def load_all_models() -> None:
    """Import all ORM model modules so `Base.metadata` is complete for migrations.

    Subsequent feature slices add explicit imports of their `modules.<name>.models`
    modules here so `alembic check` and autogenerate always see the full schema.
    """
    return None
