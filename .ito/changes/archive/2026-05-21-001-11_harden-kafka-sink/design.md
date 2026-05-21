<!-- ITO:START -->
## Approach

Keep `KafkaSink` behind `MessageSink`, but add a constructor for real `confluent_kafka.Producer`. A background-safe publish path converts delivery callbacks to futures while polling until ack or timeout.

## Contracts / Interfaces

- `KafkaProducerConfig.validated()` keeps certified defaults.
- `KafkaSink.from_config()` constructs the real producer when dependency is installed.
- `publish()` returns only after ack and raises durable outbox errors otherwise.
- `close()` flushes outstanding messages with a bounded timeout.

## Decisions

- Deterministic config and authorization errors are non-retryable.
- Broker unavailability, timeout, throttling, and network failures are retryable.
- Strict ordered mode can lower `max.in.flight.requests.per.connection` to `1`.

## Verification Strategy

Use fake producer/message/error objects for unit tests and mark live Kafka tests optional.
<!-- ITO:END -->
