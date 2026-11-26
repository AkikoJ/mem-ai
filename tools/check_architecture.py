#!/usr/bin/env python3
"""Validate the repository's navigation contract and architecture rules."""

from __future__ import annotations

import ast
import re
import sys
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESIGN = ROOT / "docs/backend-scaffold-design.md"
SPEC = ROOT / ".scratch/ai-chat-memory/spec.md"
ISSUES_DIR = ROOT / ".scratch/ai-chat-memory/issues"
MAX_SOURCE_LINES = 800
LOCAL_DOCS = (
    ROOT / "AGENTS.md",
    ROOT / "README.md",
    ROOT / "DESIGN.md",
    ROOT / "GLOSSARY.md",
    ROOT / "docs/backend-scaffold-design.md",
    ROOT / "docs/operations/deployment-and-recovery.md",
    ROOT / "docs/agents/issue-tracker.md",
    ROOT / ".scratch/ai-chat-memory/spec.md",
)
LINK_PATTERN = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
CODE_PATTERN = re.compile(r"`([^`]+)`")
MERMAID_EDGE_PATTERN = re.compile(r"\bT(\d{2})\s*-->\s*T(\d{2})\b")
BLOCKED_BY_TICKET_PATTERN = re.compile(r"\((\d{2})-[^)]+\.md\)")

# Explicit infrastructure/assembly entry points exempted from cross-module import checks.
ASSEMBLY_EXCEPTIONS = frozenset(
    {
        "src/mem_ai/application.py",
        "src/mem_ai/main.py",
        "src/mem_ai/ops.py",
        "src/mem_ai/db/model_registry.py",
        "src/mem_ai/api/router.py",
        "src/mem_ai/workers/assembly.py",
    }
)

_CONTRACT_FORBIDDEN_ROOTS = frozenset(
    {
        "alembic",
        "fastapi",
        "httpx",
        "memos",
        "openai",
        "psycopg",
        "sqlalchemy",
        "starlette",
    }
)
_CONTRACT_FORBIDDEN_INTERNAL_PREFIXES = (
    "mem_ai.api",
    "mem_ai.db",
    "mem_ai.integrations",
    "mem_ai.modules",
    "mem_ai.workers",
)
_CORE_FORBIDDEN_INTERNAL_PREFIXES = (
    "mem_ai.api",
    "mem_ai.integrations",
    "mem_ai.modules",
    "mem_ai.workers",
)
_ROUTER_FORBIDDEN_ROOTS = frozenset({"alembic", "psycopg", "sqlalchemy"})


class ArchitectureErrors:
    def __init__(self) -> None:
        self.items: list[str] = []

    def add(self, message: str) -> None:
        self.items.append(message)


def section(text: str, heading: str, next_heading: str | None = None) -> str:
    start = text.find(heading)
    if start < 0:
        return ""
    content = text[start + len(heading) :]
    if next_heading:
        end = content.find(next_heading)
        if end >= 0:
            content = content[:end]
    return content


def table_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) > 1 and not all(set(cell) <= {"-", " "} for cell in cells):
            rows.append(cells)
    return rows[1:] if rows and rows[0][0] in {"路径", "业务能力"} else rows


def check_navigation(errors: ArchitectureErrors) -> None:
    if not DESIGN.exists():
        errors.add("缺少 docs/backend-scaffold-design.md")
        return

    text = DESIGN.read_text(encoding="utf-8")
    required_headings = (
        "## 3. 工程目录",
        "### 3.0.1 当前目录状态",
        "### 3.0.2 功能定位索引",
    )
    for heading in required_headings:
        if heading not in text:
            errors.add(f"工程目录缺少必要标题 {heading}")

    status_text = section(text, "### 3.0.1 当前目录状态", "### 3.0.2 功能定位索引")
    for row in table_rows(status_text):
        if len(row) < 3:
            errors.add(f"工程目录状态表列数不足 {row}")
            continue
        paths = CODE_PATTERN.findall(row[0])
        if "已落地" not in row[1]:
            continue
        for path_text in paths:
            path = ROOT / path_text.rstrip("/")
            if not path.exists():
                errors.add(f"目录状态标为已落地但路径不存在 {path_text}")

    index_text = section(text, "### 3.0.2 功能定位索引", "目录树如下。")
    seen_paths: dict[str, str] = {}
    for row in table_rows(index_text):
        if len(row) < 6:
            errors.add(f"功能定位索引列数不足 {row}")
            continue
        capability, owner, _, lookup, verification, tickets = row[:6]
        if not owner or not CODE_PATTERN.search(owner):
            errors.add(f"功能定位索引缺少主负责人 {capability}")
        if not CODE_PATTERN.search(lookup):
            errors.add(f"功能定位索引缺少首要查找路径 {capability}")
        if not verification or not tickets:
            errors.add(f"功能定位索引缺少验证位置或 ticket {capability}")
        for path_text in CODE_PATTERN.findall(lookup):
            if not path_text.startswith("src/"):
                continue
            previous = seen_paths.get(path_text)
            if previous and previous != capability:
                errors.add(
                    f"功能定位索引将同一首要路径分配给多个能力 "
                    f"{path_text}（{previous}、{capability}）"
                )
            seen_paths[path_text] = capability


def check_links(errors: ArchitectureErrors) -> None:
    for document in LOCAL_DOCS:
        if not document.exists():
            continue
        text = document.read_text(encoding="utf-8")
        for target in LINK_PATTERN.findall(text):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = target.split("#", 1)[0].strip()
            if not target:
                continue
            resolved = (document.parent / target).resolve()
            if not resolved.exists():
                errors.add(f"失效本地链接 {document.relative_to(ROOT)} -> {target}")


def source_files() -> list[Path]:
    source_root = ROOT / "src"
    if not source_root.exists():
        return []
    return sorted(source_root.rglob("*.py"))


def check_source_file_sizes(errors: ArchitectureErrors) -> None:
    for path in source_files():
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        if line_count > MAX_SOURCE_LINES:
            errors.add(
                f"源码文件超过 {MAX_SOURCE_LINES} 行，需要重新核对职责边界和拆分位置 "
                f"{path.relative_to(ROOT)}（{line_count} 行）"
            )


def check_source_module_ownership(errors: ArchitectureErrors) -> None:
    source_module_root = ROOT / "src" / "mem_ai" / "modules"
    if not source_module_root.exists():
        return

    design_text = DESIGN.read_text(encoding="utf-8")
    index_text = section(design_text, "### 3.0.2 功能定位索引", "目录树如下。")
    indexed_modules = set(re.findall(r"src/mem_ai/modules/([a-z0-9_]+)/", index_text))
    for module_path in sorted(
        path
        for path in source_module_root.iterdir()
        if path.is_dir() and not path.name.startswith("_")
    ):
        if module_path.name not in indexed_modules:
            errors.add(f"源码模块未登记到功能定位索引 {module_path.relative_to(ROOT)}")


def _extract_imports(tree: ast.AST) -> list[tuple[str, int]]:
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append((node.module, node.lineno))
            for alias in node.names:
                if alias.name != "*":
                    imports.append((f"{node.module}.{alias.name}", node.lineno))
    return imports


def _matches_prefix(module_name: str, prefixes: tuple[str, ...]) -> bool:
    return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in prefixes)


def check_imports_for_source(
    *,
    rel_path: str,
    source: str,
    errors: ArchitectureErrors,
) -> None:
    """Validate layered import rules for a single Python source file."""
    normalized = rel_path.replace("\\", "/")
    if normalized in ASSEMBLY_EXCEPTIONS:
        return

    try:
        tree = ast.parse(source, filename=normalized)
    except SyntaxError as exc:
        errors.add(f"源码无法解析 {normalized}: {exc.msg}（第 {exc.lineno} 行）")
        return

    reported: set[tuple[int, str]] = set()
    for imported, lineno in _extract_imports(tree):
        root_pkg = imported.split(".", 1)[0]

        if normalized.startswith("src/mem_ai/contracts/") and (
            root_pkg in _CONTRACT_FORBIDDEN_ROOTS
            or _matches_prefix(imported, _CONTRACT_FORBIDDEN_INTERNAL_PREFIXES)
        ):
            key = (lineno, root_pkg)
            if key not in reported:
                reported.add(key)
                errors.add(
                    f"公共协议禁止依赖基础设施或业务实现 {normalized}:{lineno} -> {imported}"
                )

        if normalized.startswith("src/mem_ai/core/") and _matches_prefix(
            imported, _CORE_FORBIDDEN_INTERNAL_PREFIXES
        ):
            key = (lineno, imported.split(".")[1])
            if key not in reported:
                reported.add(key)
                errors.add(f"core 层禁止反向依赖上层模块 {normalized}:{lineno} -> {imported}")

        if normalized.startswith("src/mem_ai/modules/"):
            parts = normalized.split("/")
            module_name = parts[3] if len(parts) > 3 else ""
            is_router = normalized.endswith("/router.py")

            if is_router and (
                root_pkg in _ROUTER_FORBIDDEN_ROOTS
                or _matches_prefix(imported, ("mem_ai.db",))
                or imported.endswith((".repository", ".models"))
                or ".adapters" in imported
            ):
                key = (lineno, "router_layer")
                if key not in reported:
                    reported.add(key)
                    errors.add(
                        f"Router 禁止越层依赖持久化或具体适配器 {normalized}:{lineno} -> {imported}"
                    )

            mod_match = re.match(r"^mem_ai\.modules\.([a-z0-9_]+)\.(repository|models)\b", imported)
            if mod_match and mod_match.group(1) != module_name:
                key = (lineno, f"cross_module_{mod_match.group(1)}")
                if key not in reported:
                    reported.add(key)
                    errors.add(f"禁止跨模块直接访问持久化层 {normalized}:{lineno} -> {imported}")


def check_layered_imports(errors: ArchitectureErrors) -> None:
    for path in source_files():
        rel_path = path.relative_to(ROOT).as_posix()
        check_imports_for_source(
            rel_path=rel_path,
            source=path.read_text(encoding="utf-8"),
            errors=errors,
        )


def check_ticket_dependency_graph(errors: ArchitectureErrors) -> None:
    if not SPEC.exists() or not ISSUES_DIR.exists():
        return

    spec_text = SPEC.read_text(encoding="utf-8")
    mermaid_edges: set[tuple[str, str]] = {
        (src, dst) for src, dst in MERMAID_EDGE_PATTERN.findall(spec_text)
    }

    ticket_ids: set[str] = set()
    ticket_edges: set[tuple[str, str]] = set()
    for issue_file in sorted(ISSUES_DIR.glob("*.md")):
        match = re.match(r"^(\d{2})-", issue_file.name)
        if not match:
            continue
        ticket_id = match.group(1)
        ticket_ids.add(ticket_id)
        for line in issue_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("**Blocked by:**"):
                for dep_id in BLOCKED_BY_TICKET_PATTERN.findall(line):
                    ticket_edges.add((dep_id, ticket_id))
                break

    if not ticket_ids:
        return

    missing_in_spec = sorted(ticket_edges - mermaid_edges)
    extra_in_spec = sorted(mermaid_edges - ticket_edges)
    for src, dst in missing_in_spec:
        errors.add(f"spec 依赖图缺少 ticket 阻塞边 T{src} -> T{dst}")
    for src, dst in extra_in_spec:
        errors.add(f"spec 依赖图存在未在 ticket 中声明的边 T{src} -> T{dst}")

    # Verify DAG has no cycles via Kahn topological sort.
    adjacency: defaultdict[str, list[str]] = defaultdict(list)
    indegree: dict[str, int] = {tid: 0 for tid in ticket_ids}
    for src, dst in ticket_edges:
        if src not in ticket_ids or dst not in ticket_ids:
            errors.add(f"ticket 依赖边引用了不存在的编号 T{src} -> T{dst}")
            continue
        adjacency[src].append(dst)
        indegree[dst] += 1

    queue = deque(sorted(tid for tid, deg in indegree.items() if deg == 0))
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for nxt in adjacency[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if visited != len(ticket_ids):
        errors.add("ticket 依赖图存在循环依赖")


def _extract_router_prefixes(tree: ast.AST) -> dict[str, str]:
    prefixes: dict[str, str] = {}
    for node in ast.walk(tree):
        target_name: str | None = None
        value: ast.expr | None = None
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            target_name = node.targets[0].id
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name = node.target.id
            value = node.value

        if not target_name or not isinstance(value, ast.Call):
            continue
        func = value.func
        func_name = (
            func.id
            if isinstance(func, ast.Name)
            else (func.attr if isinstance(func, ast.Attribute) else "")
        )
        if func_name != "APIRouter":
            continue

        prefix = ""
        for kw in value.keywords:
            if (
                kw.arg == "prefix"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ):
                prefix = kw.value.value
                break
        prefixes[target_name] = prefix
    return prefixes


def _join_route(prefix: str, route: str) -> str:
    if not prefix:
        return route
    if not route:
        return prefix
    return prefix.rstrip("/") + "/" + route.lstrip("/")


def route_literals(path: Path) -> list[tuple[str, str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    router_prefixes = _extract_router_prefixes(tree)
    routes: list[tuple[str, str, int]] = []
    methods = {"delete", "get", "patch", "post", "put"}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            method = decorator.func.attr.lower()
            if method not in methods or not decorator.args:
                continue
            route = decorator.args[0]
            if isinstance(route, ast.Constant) and isinstance(route.value, str):
                owner = (
                    decorator.func.value.id if isinstance(decorator.func.value, ast.Name) else ""
                )
                prefix = router_prefixes.get(owner, "")
                routes.append((method.upper(), _join_route(prefix, route.value), decorator.lineno))
    return routes


def check_duplicate_literal_routes(errors: ArchitectureErrors) -> None:
    occurrences: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
    for path in source_files():
        try:
            routes = route_literals(path)
        except SyntaxError as exc:
            errors.add(f"源码无法解析 {path.relative_to(ROOT)}: {exc.msg}（第 {exc.lineno} 行）")
            continue
        for method, route, line in routes:
            key = (method, route)
            occurrences[key].append(f"{path.relative_to(ROOT)}:{line}")

    for (method, route), locations in occurrences.items():
        if len(locations) > 1:
            errors.add(
                f"发现重复的字面量路由 {method} {route}，请确认是否应由同一入口负责 "
                + "、".join(locations)
            )


def run_all_checks() -> list[str]:
    """Run all architecture and documentation checks and return error messages."""
    errors = ArchitectureErrors()
    check_navigation(errors)
    check_links(errors)
    check_source_file_sizes(errors)
    check_source_module_ownership(errors)
    check_layered_imports(errors)
    check_duplicate_literal_routes(errors)
    check_ticket_dependency_graph(errors)
    return errors.items


def main() -> int:
    errors = run_all_checks()
    if errors:
        for item in errors:
            print(f"[architecture] {item}", file=sys.stderr)
        print(f"[architecture] 失败，共 {len(errors)} 项", file=sys.stderr)
        return 1

    print("[architecture] 工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
