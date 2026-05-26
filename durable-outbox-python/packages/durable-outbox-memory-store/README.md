# durable-outbox-memory-store

In-memory store provider package for `durable-outbox` tests and local
development.

```bash
uv add durable-outbox durable-outbox-memory-store
```

```python
from durable_outbox import load_store

store = load_store("memory")
```
