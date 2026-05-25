from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OutboxCapabilities:
    """Static capabilities and limits advertised by a store adapter."""

    store_name: str
    rpo_zero_for_accepted_events: bool
    supports_ordering: bool
    supports_failover_replay: bool
    supports_ttl_freeze: bool
    max_payload_bytes: int | None = None
    notes: tuple[str, ...] = ()

    def require_rpo_zero(self) -> None:
        from durable_outbox.core.errors import ConfigurationError

        if not self.rpo_zero_for_accepted_events:
            msg = f"{self.store_name} is not certified RPO=0 for accepted events"
            raise ConfigurationError(msg)
