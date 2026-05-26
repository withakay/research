# durable-outbox-blob-store

Azure Blob and dual-region Blob store provider package for `durable-outbox`.

```bash
uv add durable-outbox durable-outbox-blob-store
```

```python
from durable_outbox import load_store

store = load_store("blob", {"client": blob_client})
```
