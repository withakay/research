# durable-outbox-file-sink

File sink plugin package for `durable-outbox`.

Install it alongside the core package, then load it through the sink plugin
registry:

```python
from durable_outbox.plugins import load_sink

sink = load_sink("file", {"path": "published.jsonl"})
```
