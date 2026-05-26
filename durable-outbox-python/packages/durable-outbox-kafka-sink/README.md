# durable-outbox-kafka-sink

Kafka sink provider package for `durable-outbox`.

Install this package when an application needs to publish durable outbox events
to Kafka:

```bash
uv add durable-outbox durable-outbox-kafka-sink
```

Load by plugin name:

```python
from durable_outbox import load_sink

sink = load_sink("kafka", {"config": {"bootstrap.servers": "localhost:9092"}})
```
