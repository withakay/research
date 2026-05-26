from __future__ import annotations

# ruff: noqa: S608
import asyncio
import base64
import json
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    RetryableStoreError,
)
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishingMode
from durable_outbox.core.validation import require_positive_limit
from durable_outbox.stores.sql import (
    SQL_REPLAY_INDEX_NAME,
    SQL_TABLE_NAME,
    SqlReplayClaimedRecord,
    SqlStoredEvent,
)

if TYPE_CHECKING:
    from durable_outbox.core.model import PublishResult

_SQL_EXTRA_MESSAGE = "SQL support requires the sql extra: install durable-outbox[sql]"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CLEANUP_FREEZE_KEY = "cleanup-freeze"


class PyodbcCursorLike(Protocol):
    rowcount: int

    def execute(self, sql: str, *params: object) -> Self: ...

    def fetchone(self) -> object | None: ...


class PyodbcConnectionLike(Protocol):
    def cursor(self) -> PyodbcCursorLike: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PyodbcSqlConnectionSettings:
    connection_string: str
    table_name: str = SQL_TABLE_NAME
    cleanup_state_table_name: str = "durable_outbox_cleanup_state"
    connect_timeout_seconds: int = 30


class PyodbcSqlOutboxClient:
    """pyodbc-backed SQL client for persistence and RPO checks.

    Normal dispatch claims and failover replay claims use SQL Server
    ``UPDATE ... OUTPUT`` statements when this client is wired into the built-in
    SQL stores. Bounded candidate query methods remain available for custom
    store flows and tests.
    """

    def __init__(
        self,
        connection_factory: Callable[[], PyodbcConnectionLike],
        *,
        table_name: str = SQL_TABLE_NAME,
        cleanup_state_table_name: str = "durable_outbox_cleanup_state",
        partner_server: str | None = None,
        partner_database: str | None = None,
    ) -> None:
        self.connection_factory = connection_factory
        self.table_name = _quote_identifier(table_name)
        self.cleanup_state_table_name = _quote_identifier(cleanup_state_table_name)
        self.partner_server = partner_server
        self.partner_database = partner_database

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        *,
        table_name: str = SQL_TABLE_NAME,
        cleanup_state_table_name: str = "durable_outbox_cleanup_state",
        connect_timeout_seconds: int = 30,
        partner_server: str | None = None,
        partner_database: str | None = None,
    ) -> PyodbcSqlOutboxClient:
        module = _import_pyodbc()

        def connect() -> PyodbcConnectionLike:
            connection = module.connect(
                connection_string,
                timeout=connect_timeout_seconds,
            )
            return cast("PyodbcConnectionLike", connection)

        return cls(
            connect,
            table_name=table_name,
            cleanup_state_table_name=cleanup_state_table_name,
            partner_server=partner_server,
            partner_database=partner_database,
        )

    async def get(self, event_id: str) -> SqlStoredEvent | None:
        return await asyncio.to_thread(self._get_sync, event_id)

    async def upsert_new(self, record: SqlStoredEvent) -> SqlStoredEvent:
        return await asyncio.to_thread(self._upsert_new_sync, record)

    async def replace(
        self,
        record: SqlStoredEvent,
        *,
        expected_version: int,
    ) -> SqlStoredEvent:
        return await asyncio.to_thread(self._replace_sync, record, expected_version)

    async def list_records(self) -> Sequence[SqlStoredEvent]:
        raise ConfigurationError(
            "PyodbcSqlOutboxClient does not support full-table list_records(); "
            "use provider-specific query methods"
        )

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]:
        require_positive_limit(limit)
        return await asyncio.to_thread(
            self._claim_batch_pending_sync,
            limit,
            now,
            claim_timeout,
        )

    async def claim_batch_pending_atomic(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]:
        require_positive_limit(limit)
        return await asyncio.to_thread(
            self._claim_batch_pending_atomic_sync,
            limit,
            now,
            claim_timeout,
        )

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Sequence[SqlStoredEvent]:
        require_positive_limit(limit)
        return await asyncio.to_thread(
            self._list_failover_replay_candidates_sync,
            failover_started_at,
            limit,
            tuple(sorted(exclude_event_ids)),
        )

    async def claim_failover_replay_batch_atomic(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
        exclude_event_ids: set[str],
    ) -> Sequence[SqlReplayClaimedRecord]:
        require_positive_limit(limit)
        return await asyncio.to_thread(
            self._claim_failover_replay_batch_atomic_sync,
            failover_started_at,
            limit,
            now,
            claim_timeout,
            tuple(sorted(exclude_event_ids)),
        )

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> Sequence[SqlStoredEvent]:
        return await asyncio.to_thread(
            self._list_cleanup_candidates_sync,
            now,
            safety_margin,
            limit,
        )

    async def delete(self, event_id: str) -> None:
        await asyncio.to_thread(self._delete_sync, event_id)

    async def wait_for_database_copy_sync(self) -> None:
        await asyncio.to_thread(self._wait_for_database_copy_sync)

    async def synchronized_secondary_count(self) -> int:
        return await asyncio.to_thread(self._synchronized_secondary_count)

    async def get_cleanup_freeze_reason(self) -> str | None:
        return await asyncio.to_thread(self._get_cleanup_freeze_reason)

    async def set_cleanup_freeze(self, reason: str) -> None:
        await asyncio.to_thread(self._set_cleanup_freeze, reason)

    async def clear_cleanup_freeze(self) -> None:
        await asyncio.to_thread(self._clear_cleanup_freeze)

    def _get_sync(self, event_id: str) -> SqlStoredEvent | None:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT TOP (1) * FROM {self.table_name} WHERE event_id = ?",
                event_id,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return decode_sql_record(_row_mapping(row))
        finally:
            connection.close()

    def _upsert_new_sync(self, record: SqlStoredEvent) -> SqlStoredEvent:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                _select_for_upsert_sql(self.table_name),
                record.event.event_id,
            )
            row = cursor.fetchone()
            if row is not None:
                connection.commit()
                return decode_sql_record(_row_mapping(row))
            values = encode_sql_record(record)
            cursor.execute(
                _insert_sql(self.table_name),
                *(_insert_parameters(values)),
            )
            row = cursor.fetchone()
            connection.commit()
            if row is None:
                return record
            return decode_sql_record(_row_mapping(row))
        finally:
            connection.close()

    def _replace_sync(
        self,
        record: SqlStoredEvent,
        expected_version: int,
    ) -> SqlStoredEvent:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            values = encode_sql_record(record)
            cursor.execute(
                _update_sql(self.table_name),
                *(_update_parameters(values, expected_version=expected_version)),
            )
            row = cursor.fetchone()
            if cursor.rowcount == 0 or row is None:
                raise ClaimConflictError("record version precondition failed")
            connection.commit()
            return decode_sql_record(_row_mapping(row))
        finally:
            connection.close()

    def _delete_sync(self, event_id: str) -> None:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"DELETE FROM {self.table_name} WHERE event_id = ?",
                event_id,
            )
            connection.commit()
        finally:
            connection.close()

    def _claim_batch_pending_sync(
        self,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                _claim_batch_pending_sql(self.table_name),
                limit,
                now,
                now - claim_timeout,
                now - claim_timeout,
            )
            records = _fetch_records(cursor)
            connection.commit()
            return records
        finally:
            connection.close()

    def _claim_batch_pending_atomic_sync(
        self,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                _atomic_claim_batch_pending_sql(self.table_name),
                now - claim_timeout,
                limit,
                now,
                now,
            )
            records = _fetch_records(cursor)
            connection.commit()
            return records
        finally:
            connection.close()

    def _list_failover_replay_candidates_sync(
        self,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: tuple[str, ...],
    ) -> Sequence[SqlStoredEvent]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            sql, params = _failover_replay_candidates_sql(
                self.table_name,
                exclude_event_ids=exclude_event_ids,
            )
            cursor.execute(sql, failover_started_at, *params, limit)
            records = _fetch_records(cursor)
            connection.commit()
            return records
        finally:
            connection.close()

    def _claim_failover_replay_batch_atomic_sync(
        self,
        failover_started_at: datetime,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
        exclude_event_ids: tuple[str, ...],
    ) -> Sequence[SqlReplayClaimedRecord]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            sql, params = _atomic_failover_replay_claim_sql(
                self.table_name,
                exclude_event_ids=exclude_event_ids,
            )
            cursor.execute(
                sql,
                failover_started_at,
                now - claim_timeout,
                *params,
                limit,
                now,
            )
            records = _fetch_replay_claimed_records(cursor)
            connection.commit()
            return records
        finally:
            connection.close()

    def _list_cleanup_candidates_sync(
        self,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None,
    ) -> Sequence[SqlStoredEvent]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cutoff = now - safety_margin
            if limit is None:
                cursor.execute(_cleanup_candidates_sql(self.table_name), cutoff)
            else:
                require_positive_limit(limit, field_name="limit")
                cursor.execute(
                    _cleanup_candidates_sql(self.table_name, bounded=True),
                    limit,
                    cutoff,
                )
            records = _fetch_records(cursor)
            connection.commit()
            return records
        finally:
            connection.close()

    def _wait_for_database_copy_sync(self) -> None:
        if self.partner_server is None or self.partner_database is None:
            raise ConfigurationError(
                "partner_server and partner_database are required for "
                "sp_wait_for_database_copy_sync"
            )
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                "EXEC sys.sp_wait_for_database_copy_sync "
                "@partner_server = ?, @partner_database = ?",
                self.partner_server,
                self.partner_database,
            )
            connection.commit()
        except Exception as exc:
            raise RetryableStoreError("database copy sync wait failed") from exc
        finally:
            connection.close()

    def _synchronized_secondary_count(self) -> int:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) AS synchronized_secondaries "
                "FROM sys.dm_hadr_database_replica_states "
                "WHERE is_local = 0 AND synchronization_state_desc = "
                "'SYNCHRONIZED'"
            )
            row = cursor.fetchone()
            if row is None:
                return 0
            value = _row_mapping(row).get("synchronized_secondaries", 0)
            if not isinstance(value, int):
                raise RetryableStoreError("synchronized secondary count must be int")
            return value
        finally:
            connection.close()

    def _get_cleanup_freeze_reason(self) -> str | None:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT reason FROM {self.cleanup_state_table_name} "
                "WHERE control_key = ?",
                _CLEANUP_FREEZE_KEY,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            value = _row_mapping(row).get("reason")
            if value is None or isinstance(value, str):
                return value
            raise RetryableStoreError("cleanup freeze reason must be a string")
        finally:
            connection.close()

    def _set_cleanup_freeze(self, reason: str) -> None:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"MERGE {self.cleanup_state_table_name} AS target "
                "USING (SELECT ? AS control_key, ? AS reason) AS source "
                "ON target.control_key = source.control_key "
                "WHEN MATCHED THEN UPDATE SET reason = source.reason "
                "WHEN NOT MATCHED THEN INSERT (control_key, reason) "
                "VALUES (source.control_key, source.reason);",
                _CLEANUP_FREEZE_KEY,
                reason,
            )
            connection.commit()
        finally:
            connection.close()

    def _clear_cleanup_freeze(self) -> None:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"DELETE FROM {self.cleanup_state_table_name} WHERE control_key = ?",
                _CLEANUP_FREEZE_KEY,
            )
            connection.commit()
        finally:
            connection.close()


def encode_sql_record(record: SqlStoredEvent) -> dict[str, object]:
    event = record.event
    publish_result = record.publish_result
    return {
        "event_id": event.event_id,
        "status": record.status.value,
        "topic": event.topic,
        "kafka_key": event.key,
        "headers_json": _encode_headers(event.headers),
        "payload": event.payload,
        "schema_id": event.schema_id,
        "schema_version": event.schema_version,
        "ordering_key": event.ordering_key,
        "ordering_key_hash": _ordering_key_hash(event.ordering_key),
        "ordering_sequence": event.ordering_sequence,
        "publishing_mode": event.publishing_mode.value,
        "created_at_utc": event.created_at,
        "expires_at_utc": event.expires_at,
        "accepted_at_utc": record.accepted_at,
        "next_attempt_utc": record.next_attempt_at,
        "attempt_count": record.attempt_count,
        "claim_id": record.claim_token,
        "claimed_at_utc": record.claimed_at,
        "sent_at_utc": record.sent_at,
        "kafka_partition": publish_result.partition if publish_result else None,
        "kafka_offset": publish_result.offset if publish_result else None,
        "published_at_utc": publish_result.published_at if publish_result else None,
        "publish_metadata_json": (
            json.dumps(dict(publish_result.metadata), sort_keys=True)
            if publish_result
            else None
        ),
        "failed_at_utc": record.failed_at,
        "last_error_type": record.last_error_type,
        "last_error": record.last_error,
        "row_version": record.version,
    }


def decode_sql_record(row: Mapping[str, object]) -> SqlStoredEvent:
    publish_result = _decode_publish_result(row)
    event = OutboxEvent(
        event_id=_required_str(row, "event_id"),
        topic=_required_str(row, "topic"),
        payload=_required_bytes(row, "payload"),
        key=_optional_bytes(row, "kafka_key"),
        headers=_decode_headers(_optional_str(row, "headers_json")),
        created_at=_required_datetime(row, "created_at_utc"),
        expires_at=_required_datetime(row, "expires_at_utc"),
        ordering_key=_optional_str(row, "ordering_key"),
        ordering_sequence=_optional_int(row, "ordering_sequence"),
        publishing_mode=PublishingMode(
            _optional_str(row, "publishing_mode") or "UNORDERED"
        ),
        schema_id=_optional_str(row, "schema_id"),
        schema_version=_optional_str(row, "schema_version"),
    )
    return SqlStoredEvent(
        event=event,
        version=_version_value(row.get("row_version")),
        status=OutboxStatus(_required_str(row, "status")),
        accepted_at=_optional_datetime(row, "accepted_at_utc"),
        attempt_count=_required_int(row, "attempt_count"),
        claim_token=_optional_str(row, "claim_id"),
        claimed_at=_optional_datetime(row, "claimed_at_utc"),
        next_attempt_at=_optional_datetime(row, "next_attempt_utc"),
        sent_at=_optional_datetime(row, "sent_at_utc"),
        publish_result=publish_result,
        failed_at=_optional_datetime(row, "failed_at_utc"),
        last_error_type=_optional_str(row, "last_error_type"),
        last_error=_optional_str(row, "last_error"),
    )


def _import_pyodbc() -> Any:
    try:
        module: Any = import_module("pyodbc")
    except ModuleNotFoundError as exc:
        raise ConfigurationError(_SQL_EXTRA_MESSAGE) from exc
    return module


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ConfigurationError(f"invalid SQL identifier: {value!r}")
    return f"[{value}]"


def _insert_sql(table_name: str) -> str:
    return (
        f"INSERT INTO {table_name} "
        "(event_id, status, topic, kafka_key, headers_json, payload, schema_id, "
        "schema_version, publishing_mode, ordering_key, ordering_key_hash, "
        "ordering_sequence, created_at_utc, expires_at_utc, accepted_at_utc, "
        "next_attempt_utc, attempt_count, claimed_by, claim_id, claimed_at_utc, "
        "sent_at_utc, published_at_utc, publish_metadata_json, kafka_partition, "
        "kafka_offset, failed_at_utc, last_error_type, last_error) "
        "OUTPUT INSERTED.* VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?)"
    )


def _select_for_upsert_sql(table_name: str) -> str:
    return (
        f"SELECT TOP (1) * FROM {table_name} WITH (UPDLOCK, HOLDLOCK) "
        "WHERE event_id = ?"
    )


def _update_sql(table_name: str) -> str:
    return (
        f"UPDATE {table_name} SET status = ?, next_attempt_utc = ?, "
        "attempt_count = ?, claim_id = ?, claimed_at_utc = ?, sent_at_utc = ?, "
        "published_at_utc = ?, publish_metadata_json = ?, kafka_partition = ?, "
        "kafka_offset = ?, failed_at_utc = ?, last_error_type = ?, last_error = ? "
        "OUTPUT INSERTED.* "
        "WHERE event_id = ? AND row_version = ?"
    )


def _claim_batch_pending_sql(table_name: str) -> str:
    return (
        "WITH claimable AS ("
        f"SELECT TOP (?) * FROM {table_name} "
        "WITH (READPAST, UPDLOCK, ROWLOCK) "
        "WHERE (status = 'PENDING' AND "
        "(next_attempt_utc IS NULL OR next_attempt_utc <= ?)) "
        "OR (status = 'IN_FLIGHT' AND claimed_at_utc <= ?) "
        "ORDER BY topic, ordering_key_hash, ordering_sequence, created_at_utc"
        "), fresh_ordering_blockers AS ("
        f"SELECT blockers.* FROM {table_name} AS blockers "
        "WITH (READPAST, UPDLOCK, ROWLOCK) "
        "WHERE blockers.status = 'IN_FLIGHT' "
        "AND blockers.claimed_at_utc > ? "
        "AND blockers.ordering_key_hash IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM claimable AS c "
        "WHERE c.topic = blockers.topic "
        "AND c.ordering_key_hash = blockers.ordering_key_hash)"
        ") SELECT * FROM fresh_ordering_blockers "
        "UNION ALL SELECT * FROM claimable "
        "ORDER BY topic, ordering_key_hash, ordering_sequence, created_at_utc"
    )


def _atomic_claim_batch_pending_sql(table_name: str) -> str:
    return (
        "DECLARE @stale_claimed_at DATETIME2 = ?; "
        "DECLARE @claim_limit INT = ?; "
        "DECLARE @now DATETIME2 = ?; "
        "DECLARE @claimed_at DATETIME2 = ?; "
        "WITH claimable AS ("
        "SELECT candidate.event_id, candidate.topic, candidate.ordering_key_hash, "
        "candidate.ordering_sequence, candidate.created_at_utc, candidate.publishing_mode, "
        "ROW_NUMBER() OVER (PARTITION BY "
        "CASE WHEN candidate.publishing_mode != 'ORDERED' "
        "OR candidate.ordering_key_hash IS NULL "
        "THEN candidate.event_id ELSE candidate.topic + ':' + "
        "candidate.ordering_key_hash END "
        "ORDER BY candidate.topic, candidate.ordering_key_hash, "
        "COALESCE(candidate.ordering_sequence, 0), "
        "candidate.created_at_utc, candidate.event_id) AS ordering_rank "
        f"FROM {table_name} AS candidate WITH (READPAST, UPDLOCK, ROWLOCK) "
        "WHERE ((candidate.status = 'PENDING' AND "
        "(candidate.next_attempt_utc IS NULL OR candidate.next_attempt_utc <= @now)) "
        "OR (candidate.status = 'IN_FLIGHT' "
        "AND candidate.claimed_at_utc <= @stale_claimed_at)) "
        "AND NOT EXISTS ("
        f"SELECT 1 FROM {table_name} AS fresh_ordering_blockers "
        "WITH (READPAST, UPDLOCK, ROWLOCK) "
        "WHERE fresh_ordering_blockers.status = 'IN_FLIGHT' "
        "AND fresh_ordering_blockers.claimed_at_utc > @stale_claimed_at "
        "AND fresh_ordering_blockers.publishing_mode = 'ORDERED' "
        "AND candidate.publishing_mode = 'ORDERED' "
        "AND fresh_ordering_blockers.ordering_key_hash IS NOT NULL "
        "AND fresh_ordering_blockers.topic = candidate.topic "
        "AND fresh_ordering_blockers.ordering_key_hash = "
        "candidate.ordering_key_hash)"
        "), selected_claims AS ("
        "SELECT TOP (@claim_limit) event_id FROM claimable "
        "WHERE publishing_mode != 'ORDERED' "
        "OR ordering_key_hash IS NULL OR ordering_rank = 1 "
        "ORDER BY topic, ordering_key_hash, COALESCE(ordering_sequence, 0), "
        "created_at_utc, event_id"
        ") "
        "UPDATE target SET status = 'IN_FLIGHT', claim_id = NEWID(), "
        "claimed_at_utc = @claimed_at, attempt_count = attempt_count + 1 "
        "OUTPUT INSERTED.* "
        f"FROM {table_name} AS target "
        "INNER JOIN selected_claims ON selected_claims.event_id = target.event_id"
    )


def _failover_replay_candidates_sql(
    table_name: str,
    *,
    exclude_event_ids: tuple[str, ...],
) -> tuple[str, tuple[str, ...]]:
    exclusion = ""
    if exclude_event_ids:
        placeholders = ", ".join("?" for _ in exclude_event_ids)
        exclusion = f" AND event_id NOT IN ({placeholders})"
    return (
        "WITH replay_candidates AS ("
        f"SELECT * FROM {table_name} "
        f"WITH (READPAST, UPDLOCK, ROWLOCK, INDEX({SQL_REPLAY_INDEX_NAME})) "
        "WHERE expires_at_utc >= ? "
        "AND status IN ('PENDING', 'IN_FLIGHT', 'SENT')"
        f"{exclusion} "
        "), ranked_replay_candidates AS ("
        "SELECT *, ROW_NUMBER() OVER ("
        "PARTITION BY CASE WHEN ordering_key_hash IS NULL "
        "THEN event_id ELSE topic + ':' + ordering_key_hash END "
        "ORDER BY created_at_utc, event_id) AS ordering_rank "
        "FROM replay_candidates"
        ") "
        "SELECT TOP (?) * FROM ranked_replay_candidates "
        "WHERE ordering_key_hash IS NULL OR ordering_rank = 1 "
        "ORDER BY created_at_utc, event_id",
        exclude_event_ids,
    )


def _atomic_failover_replay_claim_sql(
    table_name: str,
    *,
    exclude_event_ids: tuple[str, ...],
) -> tuple[str, tuple[str, ...]]:
    exclusion = ""
    if exclude_event_ids:
        placeholders = ", ".join("?" for _ in exclude_event_ids)
        exclusion = f" AND candidate.event_id NOT IN ({placeholders})"
    return (
        "WITH replay_candidates AS ("
        "SELECT candidate.event_id, candidate.topic, candidate.ordering_key_hash, "
        "candidate.created_at_utc, "
        "ROW_NUMBER() OVER ("
        "PARTITION BY CASE WHEN candidate.ordering_key_hash IS NULL "
        "THEN candidate.event_id ELSE candidate.topic + ':' + "
        "candidate.ordering_key_hash END "
        "ORDER BY candidate.created_at_utc, candidate.event_id) AS ordering_rank "
        f"FROM {table_name} AS candidate "
        f"WITH (READPAST, UPDLOCK, ROWLOCK, INDEX({SQL_REPLAY_INDEX_NAME})) "
        "WHERE candidate.expires_at_utc >= ? "
        "AND candidate.status IN ('PENDING', 'IN_FLIGHT', 'SENT') "
        "AND NOT EXISTS ("
        f"SELECT 1 FROM {table_name} AS fresh_ordering_blockers "
        "WITH (READPAST, UPDLOCK, ROWLOCK) "
        "WHERE fresh_ordering_blockers.status = 'IN_FLIGHT' "
        "AND fresh_ordering_blockers.claimed_at_utc > ? "
        "AND fresh_ordering_blockers.publishing_mode = 'ORDERED' "
        "AND candidate.publishing_mode = 'ORDERED' "
        "AND fresh_ordering_blockers.ordering_key_hash IS NOT NULL "
        "AND fresh_ordering_blockers.event_id != candidate.event_id "
        "AND fresh_ordering_blockers.topic = candidate.topic "
        "AND fresh_ordering_blockers.ordering_key_hash = "
        "candidate.ordering_key_hash) "
        f"{exclusion}"
        "), selected_replay_claims AS ("
        "SELECT TOP (?) event_id FROM replay_candidates "
        "WHERE ordering_key_hash IS NULL OR ordering_rank = 1 "
        "ORDER BY created_at_utc, event_id"
        ") "
        "UPDATE target SET status = 'IN_FLIGHT', claim_id = NEWID(), "
        "claimed_at_utc = ?, attempt_count = attempt_count + 1 "
        "OUTPUT INSERTED.*, DELETED.status AS source_status "
        f"FROM {table_name} AS target "
        "INNER JOIN selected_replay_claims "
        "ON selected_replay_claims.event_id = target.event_id",
        exclude_event_ids,
    )


def _cleanup_candidates_sql(table_name: str, *, bounded: bool = False) -> str:
    top_clause = "TOP (?) " if bounded else ""
    return (
        f"SELECT {top_clause}* FROM {table_name} "
        "WHERE status = 'SENT' AND expires_at_utc < ? "
        "ORDER BY expires_at_utc, event_id"
    )


def _insert_parameters(values: Mapping[str, object]) -> tuple[object, ...]:
    return (
        values["event_id"],
        values["status"],
        values["topic"],
        values["kafka_key"],
        values["headers_json"],
        values["payload"],
        values["schema_id"],
        values["schema_version"],
        values["publishing_mode"],
        values["ordering_key"],
        values["ordering_key_hash"],
        values["ordering_sequence"],
        values["created_at_utc"],
        values["expires_at_utc"],
        values["accepted_at_utc"],
        values["next_attempt_utc"],
        values["attempt_count"],
        values["claim_id"],
        values["claimed_at_utc"],
        values["sent_at_utc"],
        values["published_at_utc"],
        values["publish_metadata_json"],
        values["kafka_partition"],
        values["kafka_offset"],
        values["failed_at_utc"],
        values["last_error_type"],
        values["last_error"],
    )


def _update_parameters(
    values: Mapping[str, object],
    *,
    expected_version: int,
) -> tuple[object, ...]:
    return (
        values["status"],
        values["next_attempt_utc"],
        values["attempt_count"],
        values["claim_id"],
        values["claimed_at_utc"],
        values["sent_at_utc"],
        values["published_at_utc"],
        values["publish_metadata_json"],
        values["kafka_partition"],
        values["kafka_offset"],
        values["failed_at_utc"],
        values["last_error_type"],
        values["last_error"],
        values["event_id"],
        _row_version_parameter(expected_version),
    )


def _row_mapping(row: object) -> Mapping[str, object]:
    if isinstance(row, Mapping):
        return cast("Mapping[str, object]", row)
    description = getattr(row, "cursor_description", None)
    if isinstance(description, Sequence):
        try:
            values = tuple(cast("Sequence[object]", row))
        except TypeError as exc:
            raise RetryableStoreError("pyodbc row values are not iterable") from exc
        names: list[str] = []
        for column in description:
            if not isinstance(column, Sequence) or not column:
                raise RetryableStoreError("pyodbc row description is invalid")
            name = column[0]
            if not isinstance(name, str):
                raise RetryableStoreError("pyodbc row column name must be a string")
            names.append(name)
        if len(names) != len(values):
            raise RetryableStoreError("pyodbc row description/value length mismatch")
        return dict(zip(names, values, strict=True))
    raise RetryableStoreError("pyodbc row mapping is required")


def _fetch_records(cursor: PyodbcCursorLike) -> tuple[SqlStoredEvent, ...]:
    records: list[SqlStoredEvent] = []
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        records.append(decode_sql_record(_row_mapping(row)))
    return tuple(records)


def _fetch_replay_claimed_records(
    cursor: PyodbcCursorLike,
) -> tuple[SqlReplayClaimedRecord, ...]:
    records: list[SqlReplayClaimedRecord] = []
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        values = _row_mapping(row)
        records.append(
            SqlReplayClaimedRecord(
                record=decode_sql_record(values),
                source_status=OutboxStatus(_required_str(values, "source_status")),
            )
        )
    return tuple(records)


def _encode_headers(headers: Mapping[str, bytes]) -> str:
    values = {
        name: base64.b64encode(value).decode("ascii")
        for name, value in sorted(headers.items())
    }
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def _decode_headers(value: str | None) -> dict[str, bytes]:
    if value is None:
        return {}
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise RetryableStoreError("headers_json must decode to an object")
    headers: dict[str, bytes] = {}
    for name, header_value in decoded.items():
        if not isinstance(name, str) or not isinstance(header_value, str):
            raise RetryableStoreError("headers_json entries must be strings")
        headers[name] = base64.b64decode(header_value.encode("ascii"))
    return headers


def _decode_publish_result(row: Mapping[str, object]) -> PublishResult | None:
    published_at = _optional_datetime(row, "published_at_utc") or _optional_datetime(
        row, "sent_at_utc"
    )
    if published_at is None:
        return None
    from durable_outbox.core.model import PublishResult

    metadata_json = _optional_str(row, "publish_metadata_json")
    metadata = _decode_str_mapping(metadata_json)
    return PublishResult(
        partition=_optional_int(row, "kafka_partition"),
        offset=_optional_int(row, "kafka_offset"),
        published_at=published_at,
        metadata=metadata,
    )


def _decode_str_mapping(value: str | None) -> dict[str, str]:
    if value is None:
        return {}
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise RetryableStoreError("publish metadata must decode to an object")
    result: dict[str, str] = {}
    for key, item in decoded.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise RetryableStoreError("publish metadata entries must be strings")
        result[key] = item
    return result


def _ordering_key_hash(ordering_key: str | None) -> str | None:
    if ordering_key is None:
        return None
    from hashlib import sha256

    return sha256(ordering_key.encode("utf-8")).hexdigest()


def _version_value(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int.from_bytes(value, byteorder="big")
    raise RetryableStoreError("row_version must be int or bytes")


def _row_version_parameter(value: int) -> bytes:
    return value.to_bytes(8, byteorder="big", signed=False)


def _required_str(row: Mapping[str, object], field_name: str) -> str:
    value = row.get(field_name)
    if not isinstance(value, str):
        raise RetryableStoreError(f"{field_name} must be a string")
    return value


def _optional_str(row: Mapping[str, object], field_name: str) -> str | None:
    value = row.get(field_name)
    if value is None or isinstance(value, str):
        return value
    raise RetryableStoreError(f"{field_name} must be a string")


def _required_bytes(row: Mapping[str, object], field_name: str) -> bytes:
    value = row.get(field_name)
    if isinstance(value, bytes):
        return value
    raise RetryableStoreError(f"{field_name} must be bytes")


def _optional_bytes(row: Mapping[str, object], field_name: str) -> bytes | None:
    value = row.get(field_name)
    if value is None or isinstance(value, bytes):
        return value
    raise RetryableStoreError(f"{field_name} must be bytes")


def _required_datetime(row: Mapping[str, object], field_name: str) -> datetime:
    value = row.get(field_name)
    if isinstance(value, datetime):
        return value
    raise RetryableStoreError(f"{field_name} must be a datetime")


def _optional_datetime(row: Mapping[str, object], field_name: str) -> datetime | None:
    value = row.get(field_name)
    if value is None or isinstance(value, datetime):
        return value
    raise RetryableStoreError(f"{field_name} must be a datetime")


def _required_int(row: Mapping[str, object], field_name: str) -> int:
    value = row.get(field_name)
    if isinstance(value, int):
        return value
    raise RetryableStoreError(f"{field_name} must be an int")


def _optional_int(row: Mapping[str, object], field_name: str) -> int | None:
    value = row.get(field_name)
    if value is None or isinstance(value, int):
        return value
    raise RetryableStoreError(f"{field_name} must be an int")
