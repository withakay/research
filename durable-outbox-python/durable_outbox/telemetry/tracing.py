from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class TraceContext:
    """W3C trace context values that can be injected into sink headers."""

    trace_id: str
    span_id: str
    trace_flags: str = "01"

    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"


class Tracer(Protocol):
    """Supplies the current trace context for outbound sink propagation."""

    def current_context(self) -> TraceContext | None: ...


class NoopTracer:
    """Tracer implementation used when host applications do not provide one."""

    def current_context(self) -> TraceContext | None:
        return None
