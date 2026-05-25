from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class CleanupPolicy:
    """Retention windows used by host cleanup runners."""

    sent_safety_margin: timedelta = timedelta(minutes=5)
    failed_retention: timedelta = timedelta(days=30)
