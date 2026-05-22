from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_delay: timedelta = timedelta(seconds=1)
    max_delay: timedelta = timedelta(minutes=5)
    multiplier: float = 2.0

    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime:
        exponent = max(attempt_count - 1, 0)
        delay_seconds = self.initial_delay.total_seconds() * (self.multiplier**exponent)
        capped = min(delay_seconds, self.max_delay.total_seconds())
        return now + timedelta(seconds=capped)
