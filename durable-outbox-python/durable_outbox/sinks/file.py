import asyncio
import base64
import json
import os
from pathlib import Path
from time import monotonic
from typing import BinaryIO

from durable_outbox.core.model import OutboxEvent, PublishResult
from durable_outbox.core.time import Clock, SystemClock


class FileSink:
    """Append published events to a local JSONL file for integration testing."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        fsync: bool = False,
        fsync_interval_events: int = 1,
        fsync_interval_ms: int | None = None,
    ) -> None:
        if fsync_interval_events < 1:
            msg = "fsync_interval_events must be at least 1"
            raise ValueError(msg)
        if fsync_interval_ms is not None and fsync_interval_ms < 0:
            msg = "fsync_interval_ms must be non-negative"
            raise ValueError(msg)
        self.path = Path(path)
        self.clock = clock or SystemClock()
        self.fsync = fsync
        self.fsync_interval_events = fsync_interval_events
        self.fsync_interval_ms = fsync_interval_ms
        self._lock = asyncio.Lock()
        self._offset = 0
        self._handle: BinaryIO | None = None
        self._events_since_fsync = 0
        self._last_fsync_at = monotonic()

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

    async def aclose(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._close_handle)

    async def __aenter__(self) -> FileSink:
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.aclose()

    def _append_line(self, line: bytes) -> None:
        handle = self._open_handle()
        handle.write(line)
        handle.write(b"\n")
        handle.flush()
        if self.fsync and self._should_fsync():
            self._fsync(handle)

    def _open_handle(self) -> BinaryIO:
        if self._handle is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = self.path.open("ab")
            self._last_fsync_at = monotonic()
        return self._handle

    def _should_fsync(self) -> bool:
        self._events_since_fsync += 1
        if self._events_since_fsync >= self.fsync_interval_events:
            return True
        if self.fsync_interval_ms is None:
            return False
        elapsed_ms = (monotonic() - self._last_fsync_at) * 1000
        return elapsed_ms >= self.fsync_interval_ms

    def _fsync(self, handle: BinaryIO) -> None:
        os.fsync(handle.fileno())
        self._events_since_fsync = 0
        self._last_fsync_at = monotonic()

    def _close_handle(self) -> None:
        if self._handle is None:
            return
        try:
            self._handle.flush()
            if self.fsync and self._events_since_fsync:
                self._fsync(self._handle)
        finally:
            self._handle.close()
            self._handle = None


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
