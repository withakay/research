from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_sql_store.pyodbc import (
    PyodbcSqlConnectionSettings,
    PyodbcSqlOutboxClient,
    decode_sql_record,
    encode_sql_record,
)
from durable_outbox_sql_store.store import (
    SQL_ORDERED_INDEX_NAME,
    SQL_PENDING_INDEX_NAME,
    SQL_REPLAY_INDEX_NAME,
    SQL_SCHEMA,
    SQL_TABLE_NAME,
    AzureSqlSyncConfiguration,
    AzureSqlSyncOutboxStore,
    InMemorySqlOutboxClient,
    SqlAlwaysOnOutboxStore,
    SqlAtomicClaimClient,
    SqlAtomicReplayClaimClient,
    SqlOutboxClient,
    SqlReplayClaimedRecord,
    SqlStoredEvent,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from durable_outbox.core.time import Clock

__all__ = [
    "SQL_ORDERED_INDEX_NAME",
    "SQL_PENDING_INDEX_NAME",
    "SQL_REPLAY_INDEX_NAME",
    "SQL_SCHEMA",
    "SQL_TABLE_NAME",
    "AzureSqlSyncConfiguration",
    "AzureSqlSyncOutboxStore",
    "InMemorySqlOutboxClient",
    "PyodbcSqlConnectionSettings",
    "PyodbcSqlOutboxClient",
    "SqlAlwaysOnOutboxStore",
    "SqlAtomicClaimClient",
    "SqlAtomicReplayClaimClient",
    "SqlOutboxClient",
    "SqlReplayClaimedRecord",
    "SqlStoredEvent",
    "build_azure_sql_sync_store",
    "build_sql_always_on_store",
    "decode_sql_record",
    "encode_sql_record",
]


def build_azure_sql_sync_store(
    config: Mapping[str, object],
) -> AzureSqlSyncOutboxStore:
    """Build an Azure SQL sync store from durable outbox plugin configuration."""

    client = _configured_client(config)
    clock = cast("Clock | None", config.get("clock"))
    return AzureSqlSyncOutboxStore(
        client=client,
        claim_timeout=_optional_timedelta(config, "claim_timeout"),
        clock=clock,
    )


def build_sql_always_on_store(
    config: Mapping[str, object],
) -> SqlAlwaysOnOutboxStore:
    """Build a SQL Always On store from durable outbox plugin configuration."""

    client = _configured_client(config)
    clock = cast("Clock | None", config.get("clock"))
    return SqlAlwaysOnOutboxStore(
        required_synchronized_secondaries=_optional_int(
            config,
            "required_synchronized_secondaries",
            default=1,
        ),
        client=client,
        claim_timeout=_optional_timedelta(config, "claim_timeout"),
        clock=clock,
    )


def _configured_client(config: Mapping[str, object]) -> SqlOutboxClient:
    client = config.get("client")
    if client is not None:
        return cast("SqlOutboxClient", client)

    connection_string = config.get("connection_string")
    if not isinstance(connection_string, str):
        raise ConfigurationError(
            "SQL store plugin requires either client or connection_string config",
        )

    table_name = _optional_str(config, "table_name", default=SQL_TABLE_NAME)
    cleanup_state_table_name = _optional_str(
        config,
        "cleanup_state_table_name",
        default="durable_outbox_cleanup_state",
    )
    connect_timeout_seconds = _optional_int(
        config,
        "connect_timeout_seconds",
        default=30,
    )
    return PyodbcSqlOutboxClient.from_connection_string(
        connection_string,
        table_name=table_name,
        cleanup_state_table_name=cleanup_state_table_name,
        connect_timeout_seconds=connect_timeout_seconds,
        partner_server=_optional_str_or_none(config, "partner_server"),
        partner_database=_optional_str_or_none(config, "partner_database"),
    )


def _optional_timedelta(
    config: Mapping[str, object],
    name: str,
) -> timedelta:
    value = config.get(name)
    if value is None:
        return timedelta(minutes=5)
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return timedelta(seconds=value)
    raise ConfigurationError(
        f"SQL store plugin config {name!r} must be a timedelta or seconds int",
    )


def _optional_str(
    config: Mapping[str, object],
    name: str,
    *,
    default: str,
) -> str:
    value = config.get(name, default)
    if isinstance(value, str):
        return value
    raise ConfigurationError(f"SQL store plugin config {name!r} must be a string")


def _optional_str_or_none(config: Mapping[str, object], name: str) -> str | None:
    value = config.get(name)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise ConfigurationError(f"SQL store plugin config {name!r} must be a string")


def _optional_int(
    config: Mapping[str, object],
    name: str,
    *,
    default: int,
) -> int:
    value = config.get(name, default)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ConfigurationError(f"SQL store plugin config {name!r} must be an int")
