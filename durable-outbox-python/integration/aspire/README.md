# Aspire integration suite

This AppHost runs the durable outbox integration dependencies:

- Azurite through the Aspire Azure Storage hosting integration.
- Kafka through the Aspire Kafka hosting integration.
- The Python integration tests as an executable resource using `uv`.

## Prerequisites

Install a local container runtime supported by Aspire. With Podman:

```bash
podman machine init
podman machine start
export ASPIRE_CONTAINER_RUNTIME=podman
```

Install the Aspire CLI if it is not already available:

```bash
dotnet tool install -g Aspire.Cli
```

## Run

From this directory:

```bash
aspire run --apphost DurableOutbox.Integration.AppHost/DurableOutbox.Integration.AppHost.csproj
```

The AppHost passes Aspire connection strings into the Python test process via
`ConnectionStrings__blobs` and `ConnectionStrings__kafka`. The tests also accept
manual overrides:

```bash
export DURABLE_OUTBOX_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true"
export DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
uv run --extra azure --extra kafka pytest -m integration tests/integration
```
