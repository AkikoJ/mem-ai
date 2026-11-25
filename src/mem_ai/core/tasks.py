"""Bounded graceful draining helpers for in-flight asyncio tasks."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any


async def drain_tasks_bounded(
    tasks: Iterable[asyncio.Task[Any]],
    *,
    timeout_seconds: float,
    max_cancel_grace_seconds: float = 1.0,
) -> set[asyncio.Task[Any]]:
    """Wait for tasks to complete, then cancel and bound cleanup within `timeout_seconds` total.

    Returns the subset of tasks still active after the bounded cancellation cleanup window.
    """
    pending = {task for task in tasks if not task.done()}
    if not pending:
        return set()

    total_budget = max(0.0, float(timeout_seconds))
    deadline = time.monotonic() + total_budget
    cancel_grace = min(max_cancel_grace_seconds, total_budget * 0.2)
    grace_wait = max(0.0, total_budget - cancel_grace)

    if grace_wait > 0.0:
        _, still_pending = await asyncio.wait(pending, timeout=grace_wait)
    else:
        still_pending = pending

    if not still_pending:
        return set()

    for task in still_pending:
        if not task.done():
            task.cancel()

    remaining = max(0.0, deadline - time.monotonic())
    if remaining <= 0.0:
        await asyncio.sleep(0)
        return {task for task in still_pending if not task.done()}

    _, cancellation_pending = await asyncio.wait(still_pending, timeout=remaining)
    return cancellation_pending
