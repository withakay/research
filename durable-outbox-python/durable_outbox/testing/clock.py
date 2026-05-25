from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class FixedClock:
    """Test clock that always returns the configured UTC timestamp."""

    now: datetime

    def utcnow(self) -> datetime:
        return self.now
