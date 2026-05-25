import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import import_module
from time import monotonic
from typing import TYPE_CHECKING, Any, Protocol, cast

from durable_outbox.core.errors import (
    ConfigurationError,
    NonRetryablePublishError,
    RetryablePublishError,
)
from durable_outbox.core.model import OutboxEvent, PublishingMode, PublishResult

if TYPE_CHECKING:
    from confluent_kafka import Producer as ConfluentProducer
else:
    ConfluentProducer = Any

type DeliveryCallback = Callable[[object, object], None]
type KafkaProducerFactory = Callable[[dict[str, object]], KafkaProducerLike]

_DEFAULT_DELIVERY_TIMEOUT_SECONDS = 30.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.05
_DEFAULT_CLOSE_TIMEOUT_SECONDS = 10.0
_CERTIFIED_SECURITY_PROTOCOLS = frozenset({"SSL", "SASL_SSL"})
_NON_RETRYABLE_ERROR_NAMES = frozenset(
    {
        "TOPIC_AUTHORIZATION_FAILED",
        "GROUP_AUTHORIZATION_FAILED",
        "CLUSTER_AUTHORIZATION_FAILED",
        "INVALID_CONFIG",
        "UNKNOWN_TOPIC_OR_PART",
        "INVALID_TOPIC_EXCEPTION",
    }
)


class KafkaProducerLike(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> None: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True, slots=True)
class KafkaProducerConfig:
    values: Mapping[str, object] = field(default_factory=dict)
    certified_mode: bool = True

    def validated(self) -> dict[str, object]:
        config = {
            "acks": "all",
            "enable.idempotence": True,
            "retries": 2_147_483_647,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "zstd",
            "linger.ms": 5,
            "security.protocol": "SASL_SSL",
            **dict(self.values),
        }
        if self.certified_mode:
            if config.get("acks") != "all":
                raise ConfigurationError("certified Kafka sink requires acks=all")
            if config.get("enable.idempotence") is not True:
                raise ConfigurationError(
                    "certified Kafka sink requires enable.idempotence=true"
                )
            protocol = str(config.get("security.protocol", "")).upper()
            if protocol not in _CERTIFIED_SECURITY_PROTOCOLS:
                allowed = ", ".join(sorted(_CERTIFIED_SECURITY_PROTOCOLS))
                raise ConfigurationError(
                    "certified Kafka sink requires security.protocol in "
                    f"{{{allowed}}}; got {protocol or 'unset'}"
                )
        return config


class KafkaSink:
    def __init__(
        self,
        *,
        producer: KafkaProducerLike,
        config: KafkaProducerConfig | None = None,
        delivery_timeout_seconds: float = _DEFAULT_DELIVERY_TIMEOUT_SECONDS,
        poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
        close_timeout_seconds: float = _DEFAULT_CLOSE_TIMEOUT_SECONDS,
    ) -> None:
        self.producer = producer
        self.config = (config or KafkaProducerConfig()).validated()
        self.delivery_timeout_seconds = delivery_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.close_timeout_seconds = close_timeout_seconds

    @classmethod
    def from_config(
        cls,
        config: KafkaProducerConfig | None = None,
        *,
        producer_factory: KafkaProducerFactory | None = None,
        delivery_timeout_seconds: float = _DEFAULT_DELIVERY_TIMEOUT_SECONDS,
        poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
        close_timeout_seconds: float = _DEFAULT_CLOSE_TIMEOUT_SECONDS,
    ) -> KafkaSink:
        producer_config = config or KafkaProducerConfig()
        validated_config = producer_config.validated()
        factory = producer_factory or _confluent_producer_factory
        return cls(
            producer=factory(validated_config),
            config=KafkaProducerConfig(validated_config, certified_mode=False),
            delivery_timeout_seconds=delivery_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            close_timeout_seconds=close_timeout_seconds,
        )

    async def publish(self, event: OutboxEvent) -> PublishResult:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[PublishResult] = loop.create_future()

        def resolve_exception(error: BaseException) -> None:
            if not future.done():
                future.set_exception(error)

        def resolve_result(result: PublishResult) -> None:
            if not future.done():
                future.set_result(result)

        def on_delivery(error: object, message: object) -> None:
            if error is not None:
                loop.call_soon_threadsafe(
                    resolve_exception,
                    _classify_error(error),
                )
                return
            partition = getattr(message, "partition", lambda: None)()
            offset = getattr(message, "offset", lambda: None)()
            loop.call_soon_threadsafe(
                resolve_result,
                PublishResult(
                    partition=partition,
                    offset=offset,
                    published_at=datetime.now(UTC),
                ),
            )

        await asyncio.to_thread(
            self.producer.produce,
            event.topic,
            key=_kafka_key(event),
            value=event.payload,
            headers=_headers(event),
            on_delivery=on_delivery,
        )
        deadline = monotonic() + self.delivery_timeout_seconds
        while not future.done():
            await asyncio.to_thread(self.producer.poll, self.poll_interval_seconds)
            if future.done():
                break
            if monotonic() >= deadline:
                raise RetryablePublishError(
                    f"Kafka delivery timed out after {self.delivery_timeout_seconds:g}s"
                )
            await asyncio.sleep(0)
        return await future

    def close(self) -> None:
        self.producer.flush(self.close_timeout_seconds)


def _headers(event: OutboxEvent) -> list[tuple[str, bytes]]:
    headers = list(event.headers.items())
    headers.append(("event_id", event.event_id.encode("utf-8")))
    return headers


def _kafka_key(event: OutboxEvent) -> bytes | None:
    if (
        event.publishing_mode is PublishingMode.ORDERED
        and event.ordering_key is not None
    ):
        return event.ordering_key.encode("utf-8")
    return event.key


def _classify_error(error: object) -> RetryablePublishError | NonRetryablePublishError:
    message = str(error)
    if _is_non_retryable_error(error, message):
        return NonRetryablePublishError(message)
    return RetryablePublishError(message)


def _is_non_retryable_error(error: object, message: str) -> bool:
    _ = message
    retriable = getattr(error, "retriable", None)
    if callable(retriable) and retriable():
        return False

    name_value = _error_name(error)
    return name_value in _NON_RETRYABLE_ERROR_NAMES


def _error_name(error: object) -> str | None:
    name = getattr(error, "name", None)
    if callable(name):
        value = name()
    else:
        value = name
    if isinstance(value, str):
        return value
    return None


def _confluent_producer_factory(config: dict[str, object]) -> KafkaProducerLike:
    try:
        module: Any = import_module("confluent_kafka")
    except ImportError as exc:
        raise ConfigurationError(
            "Kafka sink requires the kafka extra: install durable-outbox[kafka]"
        ) from exc
    producer_cls = cast(type[ConfluentProducer], module.Producer)
    return cast(KafkaProducerLike, producer_cls(config))
