"""Contract tests verifying `LocalPersonaProvider` and replaceable `PersonaProvider` adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest

from mem_ai.api.dependencies import PersonaProviderDep, ProviderRegistry, get_persona_provider
from mem_ai.application import create_app
from mem_ai.contracts.persona_provider import MemoryPolicy, PersonaProvider, PersonaSnapshot
from mem_ai.core.config import Settings
from mem_ai.db.engine import create_async_engine_from_settings, create_session_factory
from mem_ai.modules.agents import service as agents_service
from mem_ai.modules.agents.schemas import (
    AgentCreateRequest,
    AgentPublishRequest,
    AgentUpdateDraftRequest,
    MemoryPolicyPatch,
    MemoryPolicyPayload,
)
from mem_ai.modules.personas.provider import LocalPersonaProvider
from mem_ai.modules.users import service as users_service


@dataclass
class _InMemoryPersonaAdapter:
    """Alternate replaceable `PersonaProvider` implementation enforcing the exact same contract."""

    versions: dict[tuple[UUID, UUID, UUID], PersonaSnapshot] = field(default_factory=dict)
    latest_by_owner_agent: dict[tuple[UUID, UUID], UUID] = field(default_factory=dict)
    retired_agents: set[tuple[UUID, UUID]] = field(default_factory=set)

    def publish(self, *, owner_id: UUID, snapshot: PersonaSnapshot) -> None:
        self.versions[(owner_id, snapshot.agent_id, snapshot.version_id)] = snapshot
        self.latest_by_owner_agent[(owner_id, snapshot.agent_id)] = snapshot.version_id

    def retire(self, *, owner_id: UUID, agent_id: UUID) -> None:
        self.retired_agents.add((owner_id, agent_id))

    async def get_latest_published(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
    ) -> PersonaSnapshot | None:
        if (owner_id, agent_id) in self.retired_agents:
            return None
        version_id = self.latest_by_owner_agent.get((owner_id, agent_id))
        if version_id is None:
            return None
        return self.versions.get((owner_id, agent_id, version_id))

    async def get_version(
        self,
        *,
        owner_id: UUID,
        agent_id: UUID,
        version_id: UUID,
    ) -> PersonaSnapshot | None:
        return self.versions.get((owner_id, agent_id, version_id))


async def _assert_persona_provider_contract(
    provider: PersonaProvider,
    *,
    alice_id: UUID,
    bob_id: UUID,
    agent_id: UUID,
    v1_id: UUID,
    v2_id: UUID,
    retire_agent_fn: object,
) -> None:
    assert isinstance(provider, PersonaProvider)

    # Latest published is v2 for Alice.
    latest = await provider.get_latest_published(owner_id=alice_id, agent_id=agent_id)
    assert latest is not None
    assert isinstance(latest, PersonaSnapshot)
    assert latest.agent_id == agent_id
    assert latest.version_id == v2_id
    assert latest.version == 2
    assert latest.system_prompt == "Prompt v2"
    assert latest.default_model_id == "model-v2"
    assert latest.memory_policy.write_scope == "conversation"

    # Pinned v1 remains readable and immutable.
    pinned_v1 = await provider.get_version(
        owner_id=alice_id,
        agent_id=agent_id,
        version_id=v1_id,
    )
    assert pinned_v1 is not None
    assert pinned_v1.version == 1
    assert pinned_v1.system_prompt == "Prompt v1"
    assert pinned_v1.default_model_id == "model-v1"
    assert pinned_v1.memory_policy.write_scope == "user"

    # Owner isolation: Bob cannot read latest or pinned version.
    assert await provider.get_latest_published(owner_id=bob_id, agent_id=agent_id) is None
    assert await provider.get_version(owner_id=bob_id, agent_id=agent_id, version_id=v1_id) is None
    # Wrong agent_id cannot resolve v1_id.
    assert await provider.get_version(owner_id=alice_id, agent_id=uuid4(), version_id=v1_id) is None

    # Retired agent blocks get_latest_published (new chats) while preserving get_version (existing).
    assert callable(retire_agent_fn)
    await retire_agent_fn()
    assert await provider.get_latest_published(owner_id=alice_id, agent_id=agent_id) is None
    still_pinned = await provider.get_version(
        owner_id=alice_id,
        agent_id=agent_id,
        version_id=v1_id,
    )
    assert still_pinned is not None
    assert still_pinned.version_id == v1_id
    assert still_pinned.system_prompt == "Prompt v1"


@pytest.mark.integration
@pytest.mark.anyio
async def test_local_and_replacement_persona_providers_share_identical_contract(
    migrated_pg_settings: Settings,
) -> None:
    engine = create_async_engine_from_settings(migrated_pg_settings)
    sf = create_session_factory(engine)
    try:
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-contract",
        )
        bob = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="bob-contract",
        )

        created = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(
                name="Contract Agent",
                systemPrompt="Prompt v1",
                behaviorRules=["Rule A"],
                tone="calm",
                language="en",
                defaultModelId="model-v1",
                memoryPolicy=MemoryPolicyPayload(writeScope="user", recallLimit=4),
            ),
        )
        local_provider = LocalPersonaProvider(session_factory=sf)

        # Unpublished agent must return None from get_latest_published.
        assert (
            await local_provider.get_latest_published(
                owner_id=alice.id,
                agent_id=created.agent_id,
            )
            is None
        )

        v1 = await agents_service.publish_agent(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert v1.published_version is not None
        v1_id = v1.published_version.version_id

        # Edit draft and confirm LocalPersonaProvider still returns immutable v1.
        await agents_service.update_agent_draft(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            data=AgentUpdateDraftRequest(
                systemPrompt="Prompt v2",
                defaultModelId="model-v2",
                memoryPolicy=MemoryPolicyPatch(writeScope="conversation", recallLimit=6),
            ),
        )
        latest_before_v2 = await local_provider.get_latest_published(
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert latest_before_v2 is not None
        assert latest_before_v2.version == 1
        assert latest_before_v2.system_prompt == "Prompt v1"

        # Unpublished /versions snapshot must not resolve via get_version either.
        unpublished_v2 = await agents_service.create_persona_version(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
        )
        assert (
            await local_provider.get_version(
                owner_id=alice.id,
                agent_id=created.agent_id,
                version_id=unpublished_v2.version_id,
            )
            is None
        )

        v2 = await agents_service.publish_agent(
            sf,
            owner_id=alice.id,
            agent_id=created.agent_id,
            data=AgentPublishRequest(versionId=unpublished_v2.version_id),
        )
        assert v2.published_version is not None
        v2_id = v2.published_version.version_id

        async def _retire_in_db() -> None:
            await agents_service.retire_agent(
                sf,
                owner_id=alice.id,
                agent_id=created.agent_id,
            )

        await _assert_persona_provider_contract(
            local_provider,
            alice_id=alice.id,
            bob_id=bob.id,
            agent_id=created.agent_id,
            v1_id=v1_id,
            v2_id=v2_id,
            retire_agent_fn=_retire_in_db,
        )

        # Verify a replaceable in-memory adapter satisfies the exact same contract.
        mem_provider = _InMemoryPersonaAdapter()
        now = datetime.now(UTC)
        mem_agent_id = uuid4()
        mem_v1_id = uuid4()
        mem_v2_id = uuid4()
        mem_provider.publish(
            owner_id=alice.id,
            snapshot=PersonaSnapshot(
                agent_id=mem_agent_id,
                version_id=mem_v1_id,
                version=1,
                system_prompt="Prompt v1",
                behavior_rules=("Rule A",),
                tone="calm",
                language="en",
                default_model_id="model-v1",
                memory_policy=MemoryPolicy(write_scope="user", recall_limit=4),
                published_at=now,
            ),
        )
        mem_provider.publish(
            owner_id=alice.id,
            snapshot=PersonaSnapshot(
                agent_id=mem_agent_id,
                version_id=mem_v2_id,
                version=2,
                system_prompt="Prompt v2",
                behavior_rules=("Rule A",),
                tone="calm",
                language="en",
                default_model_id="model-v2",
                memory_policy=MemoryPolicy(write_scope="conversation", recall_limit=6),
                published_at=now,
            ),
        )

        async def _retire_in_mem() -> None:
            mem_provider.retire(owner_id=alice.id, agent_id=mem_agent_id)

        await _assert_persona_provider_contract(
            mem_provider,
            alice_id=alice.id,
            bob_id=bob.id,
            agent_id=mem_agent_id,
            v1_id=mem_v1_id,
            v2_id=mem_v2_id,
            retire_agent_fn=_retire_in_mem,
        )
    finally:
        await engine.dispose()


@pytest.mark.integration
@pytest.mark.anyio
async def test_application_wires_local_persona_provider_by_default_and_allows_replacement(
    migrated_pg_settings: Settings,
) -> None:
    app = create_app(migrated_pg_settings)

    @app.get("/_test/persona-seam/{owner_id}/{agent_id}")
    async def read_persona(
        owner_id: UUID,
        agent_id: UUID,
        persona_provider: PersonaProviderDep,
    ) -> dict[str, object]:
        snap = await persona_provider.get_latest_published(owner_id=owner_id, agent_id=agent_id)
        return {
            "provider_type": type(persona_provider).__name__,
            "system_prompt": snap.system_prompt if snap is not None else None,
        }

    async with app.router.lifespan_context(app):
        sf = app.state.session_factory
        alice = await users_service.resolve_or_create_user(
            sf,
            issuer="https://issuer.test",
            subject="alice-wiring",
        )
        created = await agents_service.create_agent(
            sf,
            owner_id=alice.id,
            data=AgentCreateRequest(name="Default Wired", systemPrompt="From Postgres"),
        )
        await agents_service.publish_agent(sf, owner_id=alice.id, agent_id=created.agent_id)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/_test/persona-seam/{alice.id}/{created.agent_id}")
            assert resp.status_code == 200
            assert resp.json() == {
                "provider_type": "LocalPersonaProvider",
                "system_prompt": "From Postgres",
            }

            # Replace adapter via dependency override without changing consumer code.
            replacement = _InMemoryPersonaAdapter()
            replacement.publish(
                owner_id=alice.id,
                snapshot=PersonaSnapshot(
                    agent_id=created.agent_id,
                    version_id=uuid4(),
                    version=1,
                    system_prompt="From Replacement Adapter",
                    published_at=datetime.now(UTC),
                ),
            )
            app.dependency_overrides[get_persona_provider] = lambda: replacement
            try:
                replaced_resp = await client.get(
                    f"/_test/persona-seam/{alice.id}/{created.agent_id}"
                )
                assert replaced_resp.status_code == 200
                assert replaced_resp.json() == {
                    "provider_type": "_InMemoryPersonaAdapter",
                    "system_prompt": "From Replacement Adapter",
                }
            finally:
                app.dependency_overrides.clear()

    # Also verify explicit ProviderRegistry injection skips default LocalPersonaProvider.
    injected = _InMemoryPersonaAdapter()
    app_with_injected = create_app(
        migrated_pg_settings,
        providers=ProviderRegistry(persona_provider=injected),
    )
    async with app_with_injected.router.lifespan_context(app_with_injected):
        assert app_with_injected.state.providers.persona_provider is injected
