import asyncio
import base64
import json
import os
from pathlib import Path

from durable_outbox.core.model import OutboxEvent, PublishResult
from durable_outbox.core.time import Clock, SystemClock


class FileSink:
    """Append published events to a local JSONL file for integration testing."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        fsync: bool = True,
    ) -> None:
        self.path = Path(path)
        self.clock = clock or SystemClock()
        self.fsync = fsync
        self._lock = asyncio.Lock()
        self._offset = 0

    async def publish(self, event: OutboxEvent) -> PublishResult:
        async with self._lock:
            offset = self._offset
            self._offset += 1
            published_at = self.clock.utcnow()
            line = _encode_event(event, offset=offset, published_at=published_at)
            await asyncio.to_thread(self._append_line, line)
            return PublishResult(
                partition=0,
                offset=offset,
                published_at=published_at,
                metadata={"path": str(self.path)},
            )

    def _append_line(self, line: bytes) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab") as handle:
            handle.write(line)
            handle.write(b"\n")
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())


def _encode_event(
    event: OutboxEvent,
    *,
    offset: int,
    published_at: object,
) -> bytes:
    return json.dumps(
        {
            "event_id": event.event_id,
            "topic": event.topic,
            "partition": 0,
            "offset": offset,
            "key_base64": _optional_base64(event.key),
            "payload_base64": _base64(event.payload),
            "headers": {
                name: _base64(value) for name, value in sorted(event.headers.items())
            },
            "published_at": str(published_at),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _base64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _optional_base64(value: bytes | None) -> str | None:
    if value is None:
        return None
    return _base64(value)
