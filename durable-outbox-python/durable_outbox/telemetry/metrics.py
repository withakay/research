from collections import Counter
from typing import Protocol


class MetricsAdapter(Protocol):
    def increment(self, name: str, **labels: str) -> None: ...

    def gauge(self, name: str, value: float, **labels: str) -> None: ...


class NoopMetrics:
    def increment(self, name: str, **labels: str) -> None:
        return None

    def gauge(self, name: str, value: float, **labels: str) -> None:
        return None


class InMemoryMetrics:
    def __init__(self) -> None:
        self.counts: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    def increment(self, name: str, **labels: str) -> None:
        self.counts[(name, tuple(sorted(labels.items())))] += 1

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.gauges[(name, tuple(sorted(labels.items())))] = value
