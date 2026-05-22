from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.stores.memory import MemoryOutboxStore, StoredEvent


class FakeOutboxStore(MemoryOutboxStore):
    capabilities = OutboxCapabilities(
        store_name="FakeOutboxStore",
        rpo_zero_for_accepted_events=True,
        supports_ordering=True,
        supports_failover_replay=True,
        supports_ttl_freeze=True,
    )


__all__ = ["FakeOutboxStore", "StoredEvent"]
