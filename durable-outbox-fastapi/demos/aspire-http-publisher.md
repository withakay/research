# FastAPI Aspire HTTP Publisher Integration Demo

*2026-05-25T06:09:09Z by Showboat 0.6.1*
<!-- showboat-id: 8ea47f32-321e-47bd-897b-42abb3fc8ada -->

This demo proves the FastAPI package publishes an HTTP payload through durable-outbox-python, persists the outbox event in Azurite, delivers it to Kafka, and marks the outbox record SENT.

```bash
if [ -x ./demos/scripts/run_aspire_http_publisher_demo.sh ]; then ASPIRE_CONTAINER_RUNTIME=podman ./demos/scripts/run_aspire_http_publisher_demo.sh; else cd durable-outbox-fastapi && ASPIRE_CONTAINER_RUNTIME=podman ./demos/scripts/run_aspire_http_publisher_demo.sh; fi
```

```output
demo=durable-outbox-fastapi-aspire-http-publisher
apphost=DurableOutbox.FastApi.Integration.AppHost/DurableOutbox.FastApi.Integration.AppHost.csproj
container_runtime=podman
integration_resource=durable-outbox-fastapi-integration-tests
integration_state=Finished
integration_exit_code=0
resource_health.durable-outbox-fastapi=Healthy
resource_health.blobs=Healthy
resource_health.kafka=Healthy
```
