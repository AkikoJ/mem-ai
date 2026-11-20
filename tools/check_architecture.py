#!/usr/bin/env python3
"""Validate the repository's navigation contract and lightweight architecture rules."""

from __future__ import annotations

import ast
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESIGN = ROOT / "docs/backend-scaffold-design.md"
MAX_SOURCE_LINES = 800
LOCAL_DOCS = (
    ROOT / "AGENTS.md",
    ROOT / "README.md",
    ROOT / "DESIGN.md",
    ROOT / "GLOSSARY.md",
    ROOT / "docs/backend-scaffold-design.md",
    ROOT / "docs/agents/issue-tracker.md",
    ROOT / ".scratch/ai-chat-memory/spec.md",
)
LINK_PATTERN = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
CODE_PATTERN = re.compile(r"`([^`]+)`")


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
    for module_path in sorted(path for path in source_module_root.iterdir() if path.is_dir()):
        if module_path.name not in indexed_modules:
            errors.add(
                f"源码模块未登记到功能定位索引 "
                f"{module_path.relative_to(ROOT)}"
            )


def route_literals(path: Path) -> list[tuple[str, str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
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
                routes.append((method.upper(), route.value, decorator.lineno))
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


def main() -> int:
    errors = ArchitectureErrors()
    check_navigation(errors)
    check_links(errors)
    check_source_file_sizes(errors)
    check_source_module_ownership(errors)
    check_duplicate_literal_routes(errors)

    if errors.items:
        for item in errors.items:
            print(f"[architecture] {item}", file=sys.stderr)
        print(f"[architecture] 失败，共 {len(errors.items)} 项", file=sys.stderr)
        return 1

    print("[architecture] 工程目录、功能索引、文档链接和已落地源码检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
