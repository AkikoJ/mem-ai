"""Alembic environment using synchronous psycopg 3 engine and full ORM metadata."""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData

from mem_ai.core.config import Settings
from mem_ai.db.base import Base
from mem_ai.db.engine import create_sync_engine_from_settings
from mem_ai.db.model_registry import load_all_models

config = context.config

if config.config_file_name is not None and not config.attributes.get("skip_logging_config"):
    fileConfig(config.config_file_name, disable_existing_loggers=False)

load_all_models()


def _resolve_settings() -> Settings:
    provided = config.attributes.get("settings")
    if isinstance(provided, Settings):
        return provided
    return Settings()


def _resolve_metadata() -> MetaData:
    provided = config.attributes.get("target_metadata")
    if isinstance(provided, MetaData):
        return provided
    return Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in offline SQL generation mode."""
    settings = _resolve_settings()
    context.configure(
        url=settings.database_url().render_as_string(hide_password=False),
        target_metadata=_resolve_metadata(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live PostgreSQL connection."""
    settings = _resolve_settings()
    connectable = create_sync_engine_from_settings(settings)
    try:
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=_resolve_metadata(),
                compare_type=True,
                compare_server_default=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
