from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class OutboxSettings:
    environment: str = "local"
    dispatcher_limit: int = 100
    claim_timeout: timedelta = timedelta(minutes=5)
    cleanup_safety_margin: timedelta = timedelta(minutes=5)
