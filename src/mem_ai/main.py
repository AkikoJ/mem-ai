"""ASGI application entry point for Uvicorn and production process managers."""

from __future__ import annotations

from mem_ai.application import create_app

app = create_app()
