"""Job lease heartbeat renewal for long-running durable task handlers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from mem_ai.core.exceptions import AppError, ConflictError
from mem_ai.modules.jobs import service as jobs_service
from mem_ai.modules.jobs.schemas import ClaimedJob

logger = logging.getLogger("mem_ai.workers.lease")


class JobLeaseHeartbeat:
    """Periodic lease renewal context manager protecting long-running job handlers."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        claimed: ClaimedJob,
        lease_ttl_seconds: float,
        renew_interval_seconds: float | None,
        on_lease_lost: Callable[[AppError], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._claimed = claimed
        self._lease_ttl_seconds = lease_ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._on_lease_lost = on_lease_lost
        self._task: asyncio.Task[None] | None = None
        self.loss_error: AppError | None = None

    async def __aenter__(self) -> JobLeaseHeartbeat:
        if self._renew_interval_seconds is not None and self._renew_interval_seconds > 0:
            self._task = asyncio.create_task(self._run_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)
        await self.stop()

    async def stop(self) -> None:
        """Stop background lease renewal before conditional outcome commit."""
        task = self._task
        self._task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run_loop(self) -> None:
        assert self._renew_interval_seconds is not None
        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                await jobs_service.renew_job_lease(
                    self._session_factory,
                    job_id=self._claimed.job.id,
                    lease_token=self._claimed.lease_token,
                    lease_ttl_seconds=self._lease_ttl_seconds,
                )
            except ConflictError as exc:
                self.loss_error = exc
                if self._on_lease_lost is not None:
                    self._on_lease_lost(exc)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Transient error renewing job lease; retrying before expiry",
                    extra={
                        "extra_fields": {
                            "job_id": str(self._claimed.job.id),
                            "job_type": self._claimed.job.job_type,
                        }
                    },
                    exc_info=True,
                )
