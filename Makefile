.PHONY: help sync db-up db-down db-ps migrate migrate-check sync-models api api-prod worker ops-health ops-budget test check

# Shell and command-line variables take precedence over `.env`.
APP_HOST ?= $(or $(shell sed -n 's/^APP_HOST=//p' .env 2>/dev/null | head -n 1),127.0.0.1)
APP_PORT ?= $(or $(shell sed -n 's/^APP_PORT=//p' .env 2>/dev/null | head -n 1),8000)
API_REPLICAS ?= 2
WORKER_REPLICAS ?= 1
POSTGRES_MAX_CONNECTIONS ?= 100

help:
	@echo "Available targets:"
	@echo "  make sync          Sync locked Python dependencies"
	@echo "  make db-up         Start PostgreSQL container and wait until healthy"
	@echo "  make db-down       Stop PostgreSQL container (keeps volume data)"
	@echo "  make db-ps         Show Docker Compose service status"
	@echo "  make migrate       Apply Alembic migrations to head"
	@echo "  make migrate-check Check for Alembic metadata drift"
	@echo "  make sync-models   Idempotently sync server model registry config into PostgreSQL"
	@echo "  make api           Start FastAPI dev server with --reload"
	@echo "  make api-prod      Start FastAPI server without --reload"
	@echo "  make worker        Start standalone background Worker process"
	@echo "  make ops-health    Inspect DB readiness, job queue, leases, and memory barriers"
	@echo "  make ops-budget    Validate PostgreSQL connection budget across API & Worker replicas"
	@echo "  make test          Run pytest suite"
	@echo "  make check         Run Ruff lint/format, mypy, and architecture checks"

sync:
	uv sync --locked --dev

db-up:
	docker compose up -d --wait

db-down:
	docker compose down

db-ps:
	docker compose ps

migrate:
	uv run alembic upgrade head

migrate-check:
	uv run alembic check

sync-models:
	uv run python -m mem_ai.modules.models.sync

api:
	uv run uvicorn mem_ai.main:app --reload --host "$(APP_HOST)" --port "$(APP_PORT)"

api-prod:
	uv run uvicorn mem_ai.main:app --host "$(APP_HOST)" --port "$(APP_PORT)"

worker:
	uv run python -m mem_ai.workers.main

ops-health:
	uv run python -m mem_ai.ops inspect-health

ops-budget:
	uv run python -m mem_ai.ops check-budget --api-replicas $(API_REPLICAS) --worker-replicas $(WORKER_REPLICAS) --postgres-max-connections $(POSTGRES_MAX_CONNECTIONS)

test:
	uv run pytest

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy src
	python3 tools/check_architecture.py
