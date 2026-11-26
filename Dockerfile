# syntax=docker/dockerfile:1.7

FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable


FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000

RUN groupadd --system memai && useradd --system --gid memai --home-dir /app --shell /usr/sbin/nologin memai

WORKDIR /app

COPY --from=builder --chown=memai:memai /app/.venv /app/.venv
COPY --chown=memai:memai alembic.ini ./
COPY --chown=memai:memai migrations ./migrations

USER memai

EXPOSE 8000

# Default entrypoint runs the unified single-process backend server.
# Override CMD for single-executor operations:
#   Migrations: ["alembic", "upgrade", "head"]
#   Model sync: ["python", "-m", "mem_ai.modules.models.sync"]
#   Ops CLI:    ["python", "-m", "mem_ai.ops", "inspect-health"]
CMD ["sh", "-c", "exec uvicorn mem_ai.main:app --host ${APP_HOST:-0.0.0.0} --port ${APP_PORT:-8000}"]
