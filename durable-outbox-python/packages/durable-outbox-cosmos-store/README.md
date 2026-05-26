# durable-outbox-cosmos-store

Azure Cosmos DB store provider package for `durable-outbox`.

```bash
uv add durable-outbox durable-outbox-cosmos-store
```

```python
from durable_outbox import load_store

store = load_store("cosmos", {"client": cosmos_client})
```
