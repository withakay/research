import os
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class AppSettings:
    blob_connection_string: str
    blob_container_name: str
    kafka_bootstrap_servers: str
    default_ttl: timedelta = timedelta(minutes=15)

    @classmethod
    def from_env(cls) -> AppSettings:
        blob_connection_string = _first_env(
            "DURABLE_OUTBOX_AZURITE_CONNECTION_STRING",
            "DURABLE_OUTBOX_BLOB_CONNECTION_STRING",
            "ConnectionStrings__blobs",
            "ConnectionStrings__storage",
        )
        kafka_bootstrap_servers = _first_env(
            "DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS",
            "ConnectionStrings__kafka",
        )
        return cls(
            blob_connection_string=blob_connection_string,
            blob_container_name=os.environ.get(
                "DURABLE_OUTBOX_AZURITE_CONTAINER",
                "durable-outbox-fastapi",
            ),
            kafka_bootstrap_servers=kafka_bootstrap_servers,
        )


def _first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    joined = ", ".join(names)
    raise RuntimeError(f"missing required configuration: one of {joined}")
