# durable-outbox-fastapi

FastAPI HTTP publisher built on `durable-outbox`.

The service accepts JSON payloads over HTTP, writes them to a durable outbox
store, and dispatches accepted events to Kafka using the core
`durable-outbox-python` package.

## Run Locally

```bash
uv sync --group dev
uv run uvicorn durable_outbox_fastapi.app:create_app --factory --host 127.0.0.1 --port 18088
```

Required service configuration:

```bash
export DURABLE_OUTBOX_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true"
export DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

Publish a JSON payload:

```bash
curl -X POST http://127.0.0.1:18088/topics/orders/messages \
  -H 'content-type: application/json' \
  -d '{"order_id":"order-1"}'
```

## Aspire Integration

The integration AppHost starts Azurite, Kafka, Kafka UI, this FastAPI service,
and HTTP-driven pytest integration tests:

```bash
cd integration/aspire
export ASPIRE_CONTAINER_RUNTIME=podman
aspire run --apphost DurableOutbox.FastApi.Integration.AppHost/DurableOutbox.FastApi.Integration.AppHost.csproj
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv build
```
