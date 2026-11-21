"""Project-owned read-only contract for immutable published persona snapshots."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

MemoryWriteScope = Literal["user", "conversation"]


class MemoryPolicy(BaseModel):
    """Memory recall and write rules pinned to a published persona version."""

    model_config = ConfigDict(frozen=True)

    enabled: bool = True
    write_scope: MemoryWriteScope = "user"
    recall_limit: int = Field(default=5, ge=0, le=50)


class PersonaSnapshot(BaseModel):
    """Immutable published persona version resolved outside DB write transactions."""

    model_config = ConfigDict(frozen=True)

    agent_id: UUID
    version_id: UUID
    version: int = Field(ge=1)
    system_prompt: str
    behavior_rules: tuple[str, ...] = ()
    tone: str | None = None
    language: str | None = None
    default_model_id: str | None = None
    memory_policy: MemoryPolicy = Field(default_factory=MemoryPolicy)
    published_at: datetime


@runtime_checkable
class PersonaProvider(Protocol):
    """Read-only persona snapshot lookup seam consumed by Conversation Core."""

    async def get_latest_published(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
    ) -> PersonaSnapshot | None:
        """Return the latest published persona snapshot for a private Agent."""
        ...

    async def get_version(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
        version_id: UUID,
    ) -> PersonaSnapshot | None:
        """Return a specific immutable persona version snapshot pinned to a conversation."""
        ...
