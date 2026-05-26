from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.errors import ClaimConflictError, RetryableStoreError
from durable_outbox.core.model import (
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.stores.sql import SQL_TABLE_NAME, SqlStoredEvent
from durable_outbox.stores.sql_pyodbc import (
    PyodbcSqlOutboxClient,
    decode_sql_record,
    encode_sql_record,
)
from durable_outbox.testing.provider_contract import make_event


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.rowcount = 1

    def execute(self, sql: str, *params: object) -> FakeCursor:
        self.connection.statements.append((sql, params))
        if self.connection.rowcounts:
            self.rowcount = self.connection.rowcounts.pop(0)
        return self

    def fetchone(self) -> object | None:
        if not self.connection.rows:
            return None
        return self.connection.rows.pop(0)


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.rows: list[object | None] = []
        self.rowcounts: list[int] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_sql_pyodbc_module_does_not_import_pyodbc_at_import_time() -> None:
    module = import_module("durable_outbox.stores.sql_pyodbc")

    assert module.PyodbcSqlOutboxClient is PyodbcSqlOutboxClient


def test_pyodbc_client_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = import_module

    def fail_pyodbc_import(name: str) -> Any:
        if name == "pyodbc":
            raise ModuleNotFoundError("No module named 'pyodbc'")
        return real_import_module(name)

    monkeypatch.setattr(
        "durable_outbox.stores.sql_pyodbc.import_module", fail_pyodbc_import
    )

    with pytest.raises(ConfigurationError, match="durable-outbox\\[sql\\]"):
        PyodbcSqlOutboxClient.from_connection_string("Driver={ODBC Driver 18};")


def test_sql_record_round_trips_through_encoder() -> None:
    occurred_at = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    event = OutboxEvent(
        event_id="sql-round-trip",
        topic="durable.outbox.outputs",
        payload=b'{"hello":"world"}',
        key=b"model-run-1",
        headers={"content-type": b"application/json"},
        created_at=occurred_at,
        expires_at=occurred_at + timedelta(hours=1),
        ordering_key="customer-1",
        ordering_sequence=7,
        publishing_mode=PublishingMode.ORDERED,
        schema_id="schema-1",
        schema_version="1",
    )
    record = SqlStoredEvent(
        event=event,
        version=42,
        status=OutboxStatus.SENT,
        accepted_at=occurred_at,
        attempt_count=3,
        claim_token="claim-token",  # noqa: S106
        claimed_at=occurred_at,
        next_attempt_at=occurred_at + timedelta(seconds=10),
        sent_at=occurred_at + timedelta(seconds=20),
        publish_result=PublishResult(
            partition=2,
            offset=11,
            published_at=occurred_at + timedelta(seconds=20),
            metadata={"broker": "kafka"},
        ),
        failed_at=occurred_at + timedelta(seconds=30),
        last_error_type="RetryablePublishError",
        last_error="temporary",
    )

    decoded = decode_sql_record(encode_sql_record(record))

    assert decoded == record


@pytest.mark.asyncio
async def test_pyodbc_upsert_new_inserts_encoded_record() -> None:
    connection = FakeConnection()
    event = make_event("sql-insert")
    row = encode_sql_record(SqlStoredEvent(event=event, version=1))
    connection.rows.append(row)
    client = PyodbcSqlOutboxClient(
        lambda: connection,
        table_name=SQL_TABLE_NAME,
    )

    inserted = await client.upsert_new(SqlStoredEvent(event=event))

    sql, params = connection.statements[0]
    assert "INSERT INTO [durable_outbox_events]" in sql
    assert "OUTPUT INSERTED.*" in sql
    assert event.event_id not in sql
    assert params[0] == event.event_id
    assert inserted.version == 1
    assert connection.commits == 1
    assert connection.closed is True


@pytest.mark.asyncio
async def test_pyodbc_get_decodes_row_to_sql_stored_event() -> None:
    connection = FakeConnection()
    event = make_event("sql-get")
    connection.rows.append(encode_sql_record(SqlStoredEvent(event=event, version=5)))
    client = PyodbcSqlOutboxClient(lambda: connection)

    record = await client.get(event.event_id)

    assert record is not None
    assert record.event == event
    assert record.version == 5
    sql, params = connection.statements[0]
    assert "WHERE event_id = ?" in sql
    assert params == (event.event_id,)


@pytest.mark.asyncio
async def test_pyodbc_replace_maps_zero_rows_to_claim_conflict() -> None:
    connection = FakeConnection()
    connection.rowcounts.append(0)
    client = PyodbcSqlOutboxClient(lambda: connection)

    with pytest.raises(ClaimConflictError):
        await client.replace(
            SqlStoredEvent(event=make_event("sql-replace-conflict"), version=3),
            expected_version=2,
        )


@pytest.mark.asyncio
async def test_pyodbc_replace_returns_record_from_output_row() -> None:
    connection = FakeConnection()
    event = make_event("sql-replace")
    connection.rows.append(encode_sql_record(SqlStoredEvent(event=event, version=9)))
    client = PyodbcSqlOutboxClient(lambda: connection)

    replaced = await client.replace(SqlStoredEvent(event=event), expected_version=8)

    sql, params = connection.statements[0]
    assert "UPDATE [durable_outbox_events]" in sql
    assert "OUTPUT INSERTED.*" in sql
    assert params[-2:] == (event.event_id, 8)
    assert replaced.version == 9
    assert connection.commits == 1


@pytest.mark.asyncio
async def test_pyodbc_wait_for_database_copy_sync_executes_procedure() -> None:
    connection = FakeConnection()
    client = PyodbcSqlOutboxClient(
        lambda: connection,
        partner_server="secondary.database.windows.net",
        partner_database="outbox",
    )

    await client.wait_for_database_copy_sync()

    sql, params = connection.statements[0]
    assert "sp_wait_for_database_copy_sync" in sql
    assert params == ("secondary.database.windows.net", "outbox")


@pytest.mark.asyncio
async def test_pyodbc_wait_for_database_copy_sync_maps_errors_to_retryable() -> None:
    class FailingConnection(FakeConnection):
        def cursor(self) -> FakeCursor:
            raise RuntimeError("timeout")

    client = PyodbcSqlOutboxClient(
        FailingConnection,
        partner_server="secondary.database.windows.net",
        partner_database="outbox",
    )

    with pytest.raises(RetryableStoreError, match="database copy sync"):
        await client.wait_for_database_copy_sync()


@pytest.mark.asyncio
async def test_pyodbc_synchronized_secondary_count_queries_dmv() -> None:
    connection = FakeConnection()
    connection.rows.append({"synchronized_secondaries": 2})
    client = PyodbcSqlOutboxClient(lambda: connection)

    count = await client.synchronized_secondary_count()

    sql, _params = connection.statements[0]
    assert "sys.dm_hadr_database_replica_states" in sql
    assert count == 2


@pytest.mark.asyncio
async def test_pyodbc_cleanup_freeze_state_round_trips() -> None:
    connection = FakeConnection()
    connection.rows.append({"reason": "failover"})
    client = PyodbcSqlOutboxClient(lambda: connection)

    await client.set_cleanup_freeze("failover")
    reason = await client.get_cleanup_freeze_reason()
    await client.clear_cleanup_freeze()

    assert reason == "failover"
    assert "MERGE [durable_outbox_cleanup_state]" in connection.statements[0][0]
    assert (
        "SELECT reason FROM [durable_outbox_cleanup_state]"
        in connection.statements[1][0]
    )
    assert "DELETE FROM [durable_outbox_cleanup_state]" in connection.statements[2][0]
