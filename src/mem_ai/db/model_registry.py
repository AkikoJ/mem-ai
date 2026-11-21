"""Explicit ORM model registry loaded by Alembic before metadata inspection."""

from __future__ import annotations


def load_all_models() -> None:
    """Import all ORM model modules so `Base.metadata` is complete for migrations.

    Subsequent feature slices add explicit imports of their `modules.<name>.models`
    modules here so `alembic check` and autogenerate always see the full schema.
    """
    import mem_ai.modules.agents.models as _agents_models
    import mem_ai.modules.jobs.models as _jobs_models
    import mem_ai.modules.memories.models as _memories_models
    import mem_ai.modules.operations.models as _operations_models
    import mem_ai.modules.users.models as _users_models

    _ = (_agents_models, _jobs_models, _memories_models, _operations_models, _users_models)
