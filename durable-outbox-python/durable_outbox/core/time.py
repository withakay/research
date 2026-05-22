from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def utcnow(self) -> datetime: ...


class SystemClock:
    def utcnow(self) -> datetime:
        return datetime.now(UTC)
