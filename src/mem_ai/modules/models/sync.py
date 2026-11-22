"""Explicit server-side CLI and programmatic command to sync `model_configs` idempotently."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from mem_ai.core.config import Settings
from mem_ai.core.exceptions import AppError, ServiceUnavailableError
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.models import service as models_service
from mem_ai.modules.models.schemas import ModelConfigRecord, ModelConfigSeed


def _parse_raw_seed_entries(raw_entries: Sequence[Any]) -> list[ModelConfigSeed]:
    if not raw_entries:
        raise ServiceUnavailableError(
            code="model_configuration_missing",
            message="no model registry entries configured for sync",
        )
    seeds: list[ModelConfigSeed] = []
    seen_ids: set[str] = set()
    for entry in raw_entries:
        seed = ModelConfigSeed.model_validate(entry)
        if seed.model_id in seen_ids:
            raise ValueError(f"duplicate model_id in model registry config: {seed.model_id!r}")
        seen_ids.add(seed.model_id)
        seeds.append(seed)
    return seeds


def load_model_seeds_from_settings(settings: Settings) -> list[ModelConfigSeed]:
    """Validate and return server-controlled model seeds from application `Settings`."""
    return _parse_raw_seed_entries(settings.model_registry_entries)


def load_model_seeds_from_file(config_file: Path) -> list[ModelConfigSeed]:
    """Load and validate server-controlled model seeds from a JSON file."""
    try:
        raw_text = config_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ServiceUnavailableError(
            code="model_configuration_missing",
            message=f"unable to read model registry config file: {config_file}",
        ) from exc

    try:
        parsed = json.loads(raw_text)
    except ValueError as exc:
        raise ValueError(f"invalid JSON in model registry config file: {config_file}") from exc

    if isinstance(parsed, dict):
        raw_entries = parsed.get("models")
    elif isinstance(parsed, list):
        raw_entries = parsed
    else:
        raw_entries = None

    if not isinstance(raw_entries, list):
        raise ValueError("model registry config must be a JSON list or an object with 'models'")

    return _parse_raw_seed_entries(raw_entries)


async def run_sync_models(
    settings: Settings | None = None,
    *,
    config_file: Path | None = None,
) -> list[ModelConfigRecord]:
    """Execute an explicit idempotent sync of `model_configs` against PostgreSQL."""
    resolved_settings = settings if settings is not None else Settings()
    seeds = (
        load_model_seeds_from_file(config_file)
        if config_file is not None
        else load_model_seeds_from_settings(resolved_settings)
    )

    engine = create_async_engine_from_settings(resolved_settings)
    session_factory = create_session_factory(engine)
    try:
        return await models_service.sync_model_configs(session_factory, seeds=seeds)
    finally:
        await engine.dispose()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m mem_ai.modules.models.sync",
        description="Idempotently sync server-controlled model registry entries into PostgreSQL.",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Optional path to a JSON file containing model registry entries.",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    """CLI entry point returning 0 on success or 1 on missing/invalid configuration."""
    args = _build_arg_parser().parse_args(list(argv) if argv is not None else None)
    try:
        synced = asyncio.run(run_sync_models(settings, config_file=args.config_file))
    except AppError as exc:
        print(f"[models.sync] error ({exc.code}): {exc.message}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"[models.sync] error (model_configuration_invalid): {exc}", file=sys.stderr)
        return 1

    model_ids = ", ".join(record.model_id for record in synced)
    print(f"[models.sync] synced {len(synced)} model(s): {model_ids}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
