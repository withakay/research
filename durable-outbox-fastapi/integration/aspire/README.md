# FastAPI Aspire integration suite

This AppHost starts:

- Azurite through Aspire Azure Storage hosting.
- Kafka through Aspire Kafka hosting.
- The `durable-outbox-fastapi` service with `uvicorn`.
- Pytest integration tests that publish through the HTTP API and verify both
  Azurite state and Kafka delivery.

Run with Podman:

```bash
export ASPIRE_CONTAINER_RUNTIME=podman
aspire run --apphost DurableOutbox.FastApi.Integration.AppHost/DurableOutbox.FastApi.Integration.AppHost.csproj
```
