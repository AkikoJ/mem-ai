"""Architecture tests enforcing layered imports, route uniqueness, docs, and ticket DAG."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from fastapi import FastAPI
from fastapi.routing import APIRoute

from mem_ai.application import create_app
from mem_ai.core.config import Settings
from tools.check_architecture import (
    ArchitectureErrors,
    check_imports_for_source,
    run_all_checks,
)


def _iter_assembled_api_routes(app: FastAPI) -> Iterator[tuple[str, set[str], str]]:
    for route in app.routes:
        if isinstance(route, APIRoute):
            yield route.path, set(route.methods or ()), route.name
            continue
        effective_contexts = getattr(route, "effective_route_contexts", None)
        if callable(effective_contexts):
            for ctx in effective_contexts():
                yield ctx.path, set(ctx.methods or ()), ctx.name


def test_repository_architecture_checks_pass() -> None:
    errors = run_all_checks()
    assert errors == [], "\n".join(errors)


def test_assembled_fastapi_app_has_unique_method_and_canonical_routes() -> None:
    app = create_app(Settings(_env_file=None, app_env="test"))
    seen: defaultdict[tuple[str, str], list[str]] = defaultdict(list)

    for path, methods, name in _iter_assembled_api_routes(app):
        for method in sorted(methods):
            if method in {"HEAD", "OPTIONS"}:
                continue
            seen[(method, path)].append(name)

    duplicates = {key: names for key, names in seen.items() if len(names) > 1}
    assert duplicates == {}
    assert ("GET", "/health/live") in seen
    assert ("GET", "/health/ready") in seen
    assert ("GET", "/v1/users/me") in seen
    assert ("GET", "/v1/models") in seen
    assert ("POST", "/v1/chat") in seen


def test_layered_import_rule_rejects_forbidden_contract_and_router_dependencies() -> None:
    contract_errors = ArchitectureErrors()
    check_imports_for_source(
        rel_path="src/mem_ai/contracts/bad_provider.py",
        source="import sqlalchemy\nfrom mem_ai.modules.health import router\n",
        errors=contract_errors,
    )
    assert len(contract_errors.items) == 2

    router_errors = ArchitectureErrors()
    check_imports_for_source(
        rel_path="src/mem_ai/modules/health/router.py",
        source="from sqlalchemy import text\nfrom mem_ai.modules.agents import repository\n",
        errors=router_errors,
    )
    assert any("Router 禁止越层依赖" in msg and "sqlalchemy" in msg for msg in router_errors.items)
    assert any("禁止跨模块直接访问持久化层" in msg for msg in router_errors.items)
