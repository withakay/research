from dataclasses import dataclass
from datetime import datetime, timedelta
from random import Random, random


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_delay: timedelta = timedelta(seconds=1)
    max_delay: timedelta = timedelta(minutes=5)
    multiplier: float = 2.0
    jitter: float = 0.1
    random: Random | None = None

    def __post_init__(self) -> None:
        if self.multiplier <= 0:
            raise ValueError("retry multiplier must be positive")
        if not 0 <= self.jitter <= 1:
            raise ValueError("retry jitter must be between 0 and 1")

    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime:
        exponent = max(attempt_count - 1, 0)
        delay_seconds = self.initial_delay.total_seconds() * (self.multiplier**exponent)
        capped = min(delay_seconds, self.max_delay.total_seconds())
        if self.jitter > 0:
            capped = min(
                capped * self._jitter_factor(),
                self.max_delay.total_seconds(),
            )
        return now + timedelta(seconds=capped)

    def _jitter_factor(self) -> float:
        value = self.random.random() if self.random is not None else random()
        return 1 + ((value * 2) - 1) * self.jitter
