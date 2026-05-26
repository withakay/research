from __future__ import annotations

import asyncio
import os
import re
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, Protocol, cast
from uuid import uuid4

import pytest

from durable_outbox.core import OutboxEvent
from durable_outbox.core.model import OutboxStatus, PublishResult
from durable_outbox.stores.sql import (
    SQL_ORDERED_INDEX_NAME,
    SQL_PENDING_INDEX_NAME,
    SQL_REPLAY_INDEX_NAME,
    SQL_SCHEMA,
    AzureSqlSyncOutboxStore,
    SqlStoredEvent,
)
from durable_outbox.stores.sql_pyodbc import PyodbcSqlOutboxClient

pytestmark = pytest.mark.integration

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class LiveSqlCursor(Protocol):
    def execute(self, sql: str, *params: object) -> Any: ...


class LiveSqlConnection(Protocol):
    def cursor(self) -> LiveSqlCursor: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


def _connection_string() -> str:
    if os.environ.get("DURABLE_OUTBOX_SQL_LIVE") != "1":
        pytest.skip("set DURABLE_OUTBOX_SQL_LIVE=1 to run live SQL tests")
    return os.environ.get("DURABLE_OUTBOX_SQL_CONNECTION_STRING", "")


def _require_connection_string() -> str:
    connection_string = _connection_string()
    if not connection_string:
        pytest.fail(
            "DURABLE_OUTBOX_SQL_CONNECTION_STRING is required when "
            "DURABLE_OUTBOX_SQL_LIVE=1"
        )
    return connection_string


def _table_names() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    events = os.environ.get(
        "DURABLE_OUTBOX_SQL_TABLE_NAME",
        f"durable_outbox_events_it_{suffix}",
    )
    cleanup = (
        os.environ.get("DURABLE_OUTBOX_SQL_CLEANUP_STATE_TABLE_NAME")
        or os.environ.get("DURABLE_OUTBOX_SQL_CLEANUP_TABLE_NAME")
        or f"durable_outbox_cleanup_it_{suffix}"
    )
    _validate_identifier(events)
    _validate_identifier(cleanup)
    return events, cleanup


def _validate_identifier(value: str) -> None:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"invalid SQL test identifier: {value!r}")


def _quote_identifier(value: str) -> str:
    _validate_identifier(value)
    return f"[{value}]"


def _event(event_id: str, *, expired: bool = False) -> OutboxEvent:
    now = datetime.now(UTC)
    created_at = now - timedelta(hours=2) if expired else now
    expires_at = now - timedelta(hours=1) if expired else now + timedelta(minutes=15)
    return OutboxEvent(
        event_id=event_id,
        topic="durable.outbox.live.sql",
        payload=f'{{"event_id":"{event_id}"}}'.encode(),
        key=event_id.encode(),
        headers={"content-type": b"application/json"},
        created_at=created_at,
        expires_at=expires_at,
    )


def _connect(connection_string: str) -> LiveSqlConnection:
    pyodbc = import_module("pyodbc")
    return cast("LiveSqlConnection", pyodbc.connect(connection_string, timeout=30))


def _reset_schema(
    connection_string: str,
    *,
    events_table: str,
    cleanup_table: str,
) -> None:
    connection = _connect(connection_string)
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(events_table)}")
        cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(cleanup_table)}")
        for statement in _schema_sql(events_table=events_table):
            cursor.execute(statement)
        cursor.execute(
            "CREATE TABLE "
            f"{_quote_identifier(cleanup_table)} "
            "(control_key NVARCHAR(128) NOT NULL PRIMARY KEY, "
            "reason NVARCHAR(2048) NOT NULL)"
        )
        connection.commit()
    finally:
        connection.close()


def _drop_schema(
    connection_string: str,
    *,
    events_table: str,
    cleanup_table: str,
) -> None:
    connection = _connect(connection_string)
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(events_table)}")
        cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(cleanup_table)}")
        connection.commit()
    finally:
        connection.close()


def _schema_sql(*, events_table: str) -> tuple[str, ...]:
    schema = (
        SQL_SCHEMA.replace("durable_outbox_events", events_table)
        .replace(SQL_PENDING_INDEX_NAME, f"{SQL_PENDING_INDEX_NAME}_{events_table}")
        .replace(SQL_REPLAY_INDEX_NAME, f"{SQL_REPLAY_INDEX_NAME}_{events_table}")
        .replace(SQL_ORDERED_INDEX_NAME, f"{SQL_ORDERED_INDEX_NAME}_{events_table}")
    )
    return tuple(
        statement.strip() for statement in schema.split(";") if statement.strip()
    )


@pytest.mark.asyncio
async def test_live_pyodbc_claim_freeze_and_cleanup_paths() -> None:
    connection_string = _require_connection_string()
    events_table, cleanup_table = _table_names()
    await asyncio.to_thread(
        _reset_schema,
        connection_string,
        events_table=events_table,
        cleanup_table=cleanup_table,
    )
    client = PyodbcSqlOutboxClient.from_connection_string(
        connection_string,
        table_name=events_table,
        cleanup_state_table_name=cleanup_table,
    )
    try:
        event = _event(f"sql-live-{uuid4().hex}")
        expired_event = _event(f"sql-live-expired-{uuid4().hex}", expired=True)
        await client.upsert_new(
            SqlStoredEvent(event=event, accepted_at=event.created_at)
        )
        await client.upsert_new(
            SqlStoredEvent(event=expired_event, accepted_at=expired_event.created_at)
        )

        claimed = await client.claim_batch_pending_atomic(
            limit=1,
            now=datetime.now(UTC),
            claim_timeout=timedelta(minutes=5),
        )
        assert len(claimed) == 1
        assert claimed[0].event.event_id == expired_event.event_id
        assert claimed[0].status is OutboxStatus.IN_FLIGHT
        assert claimed[0].claim_token is not None
        assert claimed[0].attempt_count == 1

        replay_claims = await client.claim_failover_replay_batch_atomic(
            failover_started_at=event.created_at - timedelta(seconds=1),
            limit=1,
            now=datetime.now(UTC),
            claim_timeout=timedelta(minutes=5),
            exclude_event_ids=set(),
        )
        assert len(replay_claims) == 1
        assert replay_claims[0].record.event.event_id == event.event_id
        assert replay_claims[0].record.status is OutboxStatus.IN_FLIGHT
        assert replay_claims[0].record.claim_token is not None
        assert replay_claims[0].source_status is OutboxStatus.PENDING

        claimed[0].status = OutboxStatus.SENT
        claimed[0].sent_at = datetime.now(UTC)
        claimed[0].publish_result = PublishResult(
            partition=0,
            offset=1,
            published_at=claimed[0].sent_at,
            metadata={"provider": "pyodbc"},
        )
        await client.replace(claimed[0], expected_version=claimed[0].version)

        await client.set_cleanup_freeze("live sql failover")
        assert await client.get_cleanup_freeze_reason() == "live sql failover"
        await client.clear_cleanup_freeze()
        assert await client.get_cleanup_freeze_reason() is None

        cleanup = await client.list_cleanup_candidates(
            now=datetime.now(UTC),
            safety_margin=timedelta(),
            limit=10,
        )
        assert [record.event.event_id for record in cleanup] == [expired_event.event_id]
    finally:
        await asyncio.to_thread(
            _drop_schema,
            connection_string,
            events_table=events_table,
            cleanup_table=cleanup_table,
        )


@pytest.mark.asyncio
async def test_live_azure_sql_store_waits_for_database_copy_sync() -> None:
    connection_string = _require_connection_string()
    partner_server = os.environ.get("DURABLE_OUTBOX_SQL_PARTNER_SERVER")
    partner_database = os.environ.get("DURABLE_OUTBOX_SQL_PARTNER_DATABASE")
    if not partner_server or not partner_database:
        pytest.skip(
            "set DURABLE_OUTBOX_SQL_PARTNER_SERVER and "
            "DURABLE_OUTBOX_SQL_PARTNER_DATABASE to certify Azure SQL sync"
        )
    events_table, cleanup_table = _table_names()
    await asyncio.to_thread(
        _reset_schema,
        connection_string,
        events_table=events_table,
        cleanup_table=cleanup_table,
    )
    client = PyodbcSqlOutboxClient.from_connection_string(
        connection_string,
        table_name=events_table,
        cleanup_state_table_name=cleanup_table,
        partner_server=partner_server,
        partner_database=partner_database,
    )
    store = AzureSqlSyncOutboxStore(client=client)
    try:
        event = _event(f"sql-live-sync-{uuid4().hex}")
        receipt = await store.put(event)
        assert receipt.event_id == event.event_id
        assert receipt.rpo_zero is True
        assert receipt.durability_witness == (
            "azure-sql:primary",
            "azure-sql:sync-secondary",
        )
    finally:
        await asyncio.to_thread(
            _drop_schema,
            connection_string,
            events_table=events_table,
            cleanup_table=cleanup_table,
        )
