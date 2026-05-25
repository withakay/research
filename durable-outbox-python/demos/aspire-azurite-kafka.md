# Durable Outbox Aspire Azurite Kafka Integration Demo

*2026-05-25T06:10:33Z by Showboat 0.6.1*
<!-- showboat-id: e56da83c-14b4-4429-bffc-255f82a0b53e -->

This demo proves durable-outbox-python dispatches an accepted Blob outbox event through both the local-file integration path and a real Kafka sink under the Aspire-managed Azurite and Kafka services.

```bash
if [ -x ./demos/scripts/run_aspire_azurite_kafka_demo.sh ]; then ASPIRE_CONTAINER_RUNTIME=podman ./demos/scripts/run_aspire_azurite_kafka_demo.sh; else cd durable-outbox-python && ASPIRE_CONTAINER_RUNTIME=podman ./demos/scripts/run_aspire_azurite_kafka_demo.sh; fi
```

```output
demo=durable-outbox-python-aspire-azurite-kafka
apphost=DurableOutbox.Integration.AppHost/DurableOutbox.Integration.AppHost.csproj
container_runtime=podman
integration_resource=durable-outbox-integration-tests
integration_state=Finished
integration_exit_code=0
resource_health.blobs=Healthy
resource_health.kafka=Healthy
```
