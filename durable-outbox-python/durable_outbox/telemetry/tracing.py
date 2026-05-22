from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    span_id: str


class Tracer(Protocol):
    def current_context(self) -> TraceContext | None: ...


class NoopTracer:
    def current_context(self) -> TraceContext | None:
        return None
