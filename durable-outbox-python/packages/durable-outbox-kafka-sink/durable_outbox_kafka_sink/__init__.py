from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_kafka_sink.sink import (
    KafkaProducerConfig,
    KafkaProducerFactory,
    KafkaProducerLike,
    KafkaSink,
)

if TYPE_CHECKING:
    from durable_outbox.telemetry.tracing import Tracer

__all__ = [
    "KafkaProducerConfig",
    "KafkaProducerFactory",
    "KafkaProducerLike",
    "KafkaSink",
    "build_kafka_sink",
]


def build_kafka_sink(config: Mapping[str, object]) -> KafkaSink:
    """Build a Kafka sink from durable outbox plugin configuration."""

    producer = config.get("producer")
    producer_factory = cast(
        "KafkaProducerFactory | None", config.get("producer_factory")
    )
    kafka_config = _producer_config(config)
    if producer is not None:
        return KafkaSink(
            producer=cast("KafkaProducerLike", producer),
            config=kafka_config,
            delivery_timeout_seconds=_optional_float(
                config, "delivery_timeout_seconds", default=30.0
            ),
            poll_interval_seconds=_optional_float(
                config, "poll_interval_seconds", default=0.05
            ),
            close_timeout_seconds=_optional_float(
                config, "close_timeout_seconds", default=10.0
            ),
            tracer=cast("Tracer | None", config.get("tracer")),
        )
    return KafkaSink.from_config(
        kafka_config,
        producer_factory=producer_factory,
        delivery_timeout_seconds=_optional_float(
            config, "delivery_timeout_seconds", default=30.0
        ),
        poll_interval_seconds=_optional_float(
            config, "poll_interval_seconds", default=0.05
        ),
        close_timeout_seconds=_optional_float(
            config, "close_timeout_seconds", default=10.0
        ),
        tracer=cast("Tracer | None", config.get("tracer")),
    )


def _producer_config(config: Mapping[str, object]) -> KafkaProducerConfig:
    values = config.get("config", {})
    if not isinstance(values, Mapping):
        raise ConfigurationError("Kafka sink plugin config 'config' must be a mapping")
    producer_values = cast("Mapping[str, object]", values)
    certified_mode = config.get("certified_mode", True)
    if not isinstance(certified_mode, bool):
        raise ConfigurationError(
            "Kafka sink plugin config 'certified_mode' must be a bool"
        )
    return KafkaProducerConfig(values=producer_values, certified_mode=certified_mode)


def _optional_float(
    config: Mapping[str, object],
    name: str,
    *,
    default: float,
) -> float:
    value = config.get(name, default)
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise ConfigurationError(f"Kafka sink plugin config {name!r} must be numeric")
