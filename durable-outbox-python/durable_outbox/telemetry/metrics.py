from collections import Counter
from threading import Lock
from typing import Protocol

type MetricKey = tuple[str, tuple[tuple[str, str], ...]]


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
        self.counts: Counter[MetricKey] = Counter()
        self.gauges: dict[MetricKey, float] = {}
        self._lock = Lock()

    def increment(self, name: str, **labels: str) -> None:
        with self._lock:
            self.counts[(name, tuple(sorted(labels.items())))] += 1

    def gauge(self, name: str, value: float, **labels: str) -> None:
        with self._lock:
            self.gauges[(name, tuple(sorted(labels.items())))] = value
