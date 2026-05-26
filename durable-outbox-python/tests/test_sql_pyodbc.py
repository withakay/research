from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import TYPE_CHECKING, Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.errors import ClaimConflictError, RetryableStoreError
from durable_outbox.core.model import (
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.stores.sql import (
    SQL_TABLE_NAME,
    AzureSqlSyncOutboxStore,
    SqlStoredEvent,
)
from durable_outbox.stores.sql_pyodbc import (
    PyodbcSqlOutboxClient,
    decode_sql_record,
    encode_sql_record,
)
from durable_outbox.testing import FixedClock
from durable_outbox.testing.provider_contract import make_event

if TYPE_CHECKING:
    from collections.abc import Iterator


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


class FakePyodbcRow:
    def __init__(self, values: dict[str, object]) -> None:
        self.cursor_description = tuple(
            (name, None, None, None, None, None, None) for name in values
        )
        self.values = tuple(values.values())

    def __iter__(self) -> Iterator[object]:
        return iter(self.values)


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
    assert sql.count("?") == len(params)
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
async def test_pyodbc_get_decodes_real_row_shape() -> None:
    connection = FakeConnection()
    event = make_event("sql-get-row-shape")
    connection.rows.append(
        FakePyodbcRow(encode_sql_record(SqlStoredEvent(event=event, version=5)))
    )
    client = PyodbcSqlOutboxClient(lambda: connection)

    record = await client.get(event.event_id)

    assert record is not None
    assert record.event == event
    assert record.version == 5


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
    assert sql.count("?") == len(params)
    assert params[-2:] == (event.event_id, (8).to_bytes(8, byteorder="big"))
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


@pytest.mark.asyncio
async def test_pyodbc_atomic_claim_updates_and_outputs_claimed_rows() -> None:
    connection = FakeConnection()
    now = datetime.now(UTC)
    event = make_event("sql-atomic-claim")
    expected_claim_id = "00000000-0000-4000-8000-000000000001"
    row = encode_sql_record(
        SqlStoredEvent(
            event=event,
            version=3,
            status=OutboxStatus.IN_FLIGHT,
            claim_token=expected_claim_id,
            claimed_at=now,
            attempt_count=1,
        )
    )
    connection.rows.append(row)
    client = PyodbcSqlOutboxClient(lambda: connection)

    records = await client.claim_batch_pending_atomic(
        limit=25,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    sql, params = connection.statements[0]
    assert "UPDATE target" in sql
    assert "OUTPUT INSERTED.*" in sql
    assert "NEWID()" in sql
    assert "WITH (READPAST, UPDLOCK, ROWLOCK)" in sql
    assert "fresh_ordering_blockers" in sql
    assert "fresh_ordering_blockers.publishing_mode = 'ORDERED'" in sql
    assert "candidate.publishing_mode = 'ORDERED'" in sql
    assert "publishing_mode != 'ORDERED'" in sql
    assert "COALESCE(candidate.ordering_sequence, 0)" in sql
    assert "created_at_utc, candidate.event_id" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert params == (now - timedelta(minutes=5), 25, now, now)
    assert [record.event.event_id for record in records] == [event.event_id]
    assert records[0].claim_token == expected_claim_id


@pytest.mark.asyncio
async def test_sql_store_uses_pyodbc_atomic_claim_without_replace() -> None:
    connection = FakeConnection()
    now = datetime.now(UTC)
    event = make_event("sql-store-atomic-claim")
    expected_claim_id = "00000000-0000-4000-8000-000000000002"
    connection.rows.append(
        encode_sql_record(
            SqlStoredEvent(
                event=event,
                version=2,
                status=OutboxStatus.IN_FLIGHT,
                claim_token=expected_claim_id,
                claimed_at=now,
                attempt_count=1,
            )
        )
    )
    client = PyodbcSqlOutboxClient(lambda: connection)
    store = AzureSqlSyncOutboxStore(
        client=client,
        clock=FixedClock(now),
    )

    claims = await store.claim_batch(limit=1)

    assert [(claim.event.event_id, claim.claim_token) for claim in claims] == [
        (event.event_id, expected_claim_id)
    ]
    assert len(connection.statements) == 1
    assert "UPDATE target" in connection.statements[0][0]


@pytest.mark.asyncio
async def test_pyodbc_claim_batch_pending_uses_bounded_locked_query() -> None:
    connection = FakeConnection()
    event = make_event("sql-claim-query")
    row = encode_sql_record(SqlStoredEvent(event=event, version=3))
    connection.rows.append(row)
    now = datetime.now(UTC)
    client = PyodbcSqlOutboxClient(lambda: connection)

    records = await client.claim_batch_pending(
        limit=25,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    sql, params = connection.statements[0]
    assert "TOP (?)" in sql
    assert "WITH (READPAST, UPDLOCK, ROWLOCK)" in sql
    assert "fresh_ordering_blockers" in sql
    assert "status = 'PENDING'" in sql
    assert "claimed_at_utc <= ?" in sql
    assert params == (25, now, now - timedelta(minutes=5), now - timedelta(minutes=5))
    assert [record.event.event_id for record in records] == [event.event_id]


@pytest.mark.asyncio
async def test_pyodbc_claim_batch_pending_returns_fresh_ordering_blockers() -> None:
    connection = FakeConnection()
    now = datetime.now(UTC)
    blocker = SqlStoredEvent(
        event=make_event("sql-claim-blocker", ordering_key="customer-1"),
        version=2,
        status=OutboxStatus.IN_FLIGHT,
        claimed_at=now,
    )
    pending = SqlStoredEvent(
        event=make_event("sql-claim-after-blocker", ordering_key="customer-1"),
        version=3,
    )
    connection.rows.extend([encode_sql_record(blocker), encode_sql_record(pending)])
    client = PyodbcSqlOutboxClient(lambda: connection)

    records = await client.claim_batch_pending(
        limit=1,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    assert [record.event.event_id for record in records] == [
        "sql-claim-blocker",
        "sql-claim-after-blocker",
    ]
    assert records[0].status is OutboxStatus.IN_FLIGHT


@pytest.mark.asyncio
async def test_pyodbc_failover_replay_candidates_uses_replay_index_predicates() -> None:
    connection = FakeConnection()
    first = make_event("sql-replay-excluded")
    second = make_event("sql-replay-query")
    connection.rows.append(encode_sql_record(SqlStoredEvent(event=second, version=4)))
    failover_started_at = datetime.now(UTC)
    client = PyodbcSqlOutboxClient(lambda: connection)

    records = await client.list_failover_replay_candidates(
        failover_started_at=failover_started_at,
        limit=10,
        exclude_event_ids={first.event_id},
    )

    sql, params = connection.statements[0]
    assert "TOP (?)" in sql
    assert "IX_outbox_replay" in sql
    assert "status IN ('PENDING', 'IN_FLIGHT', 'SENT')" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "event_id NOT IN (?)" in sql
    assert first.event_id not in sql
    assert params == (failover_started_at, first.event_id, 10)
    assert [record.event.event_id for record in records] == [second.event_id]


@pytest.mark.asyncio
async def test_pyodbc_cleanup_candidates_uses_bounded_cleanup_query() -> None:
    connection = FakeConnection()
    event = make_event("sql-cleanup-query")
    connection.rows.append(
        encode_sql_record(
            SqlStoredEvent(event=event, version=6, status=OutboxStatus.SENT)
        )
    )
    now = event.expires_at + timedelta(minutes=10)
    client = PyodbcSqlOutboxClient(lambda: connection)

    records = await client.list_cleanup_candidates(
        now=now,
        safety_margin=timedelta(minutes=1),
        limit=5,
    )

    sql, params = connection.statements[0]
    assert "TOP (?)" in sql
    assert "status = 'SENT'" in sql
    assert "expires_at_utc < ?" in sql
    assert params == (5, now - timedelta(minutes=1))
    assert [record.event.event_id for record in records] == [event.event_id]
