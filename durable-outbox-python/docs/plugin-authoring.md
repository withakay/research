# Plugin Authoring

Durable outbox plugins are ordinary Python packages that depend on
`durable-outbox` and expose factory functions through package entry points.
Applications install those packages from a pip-compatible registry or from a
local path, then load the named store or sink with `load_store()` or
`load_sink()`.

The first-party packages `durable-outbox-file-sink`,
`durable-outbox-kafka-sink`, `durable-outbox-memory-store`,
`durable-outbox-blob-store`, `durable-outbox-cosmos-store`, and
`durable-outbox-sql-store` are reference implementations for this shape.
The core `durable-outbox` package intentionally contains no concrete stores or
sinks.

## Plugin Contracts

Sink plugins use the `durable_outbox.sinks` entry point group and return an
object satisfying `MessageSink`.

Store plugins use the `durable_outbox.stores` entry point group and return an
object satisfying `DurableOutboxStore`.

Factories receive a `Mapping[str, object]` configuration dictionary. Validate
required values in the factory and raise `ConfigurationError` for missing or
invalid configuration.

## Sink Package

Minimal package layout:

```text
durable-outbox-example-sink/
  pyproject.toml
  durable_outbox_example_sink/
    __init__.py
    py.typed
```

Minimal `pyproject.toml`:

```toml
[build-system]
requires = ["uv_build>=0.11.8,<0.12.0"]
build-backend = "uv_build"

[project]
name = "durable-outbox-example-sink"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["durable-outbox"]

[project.entry-points."durable_outbox.sinks"]
example-file = "durable_outbox_example_sink:build_sink"

[tool.uv.build-backend]
module-root = ""
module-name = "durable_outbox_example_sink"
```

Minimal factory module:

```python
from __future__ import annotations

from collections.abc import Mapping

from durable_outbox.core.errors import ConfigurationError
from durable_outbox.core.model import OutboxEvent, PublishResult
from durable_outbox.core.sink import MessageSink


class ExampleSink:
    async def publish(self, event: OutboxEvent) -> PublishResult:
        raise NotImplementedError


def build_sink(config: Mapping[str, object]) -> MessageSink:
    path = config.get("path")
    if not isinstance(path, str):
        raise ConfigurationError("example-file sink requires string path")
    return ExampleSink()
```

Applications load the sink by its entry point name:

```python
from durable_outbox import load_sink

sink = load_sink("example-file", {"path": "published.jsonl"})
```

First-party sink packages follow the same pattern. For example,
`durable-outbox-kafka-sink` registers plugin name `kafka` and exports concrete
types from `durable_outbox_kafka_sink`.

## Store Package

Minimal package layout:

```text
durable-outbox-example-store/
  pyproject.toml
  durable_outbox_example_store/
    __init__.py
    py.typed
```

Minimal `pyproject.toml`:

```toml
[build-system]
requires = ["uv_build>=0.11.8,<0.12.0"]
build-backend = "uv_build"

[project]
name = "durable-outbox-example-store"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["durable-outbox"]

[project.entry-points."durable_outbox.stores"]
example-sql = "durable_outbox_example_store:build_store"

[tool.uv.build-backend]
module-root = ""
module-name = "durable_outbox_example_store"
```

Minimal factory module:

```python
from __future__ import annotations

from collections.abc import Mapping

from durable_outbox.core.errors import ConfigurationError
from durable_outbox.core.store import DurableOutboxStore


class ExampleStore:
    pass


def build_store(config: Mapping[str, object]) -> DurableOutboxStore:
    connection_string = config.get("connection_string")
    if not isinstance(connection_string, str):
        raise ConfigurationError("example-sql store requires connection_string")
    return ExampleStore()  # implement DurableOutboxStore
```

Applications load the store by its entry point name:

```python
from durable_outbox import load_store

store = load_store("example-sql", {"connection_string": "Driver={ODBC Driver 18};..."})
```

First-party store packages follow the same pattern:

- `durable-outbox-memory-store` registers `memory`
- `durable-outbox-blob-store` registers `blob` and `dual-region-blob`
- `durable-outbox-cosmos-store` registers `cosmos`
- `durable-outbox-sql-store` registers `azure-sql-sync` and `sql-always-on`

## Installing Plugins

Install registry-published plugins like any other Python dependency:

```bash
pip install durable-outbox-example-sink
uv add durable-outbox-example-sink
uv add durable-outbox-kafka-sink
uv add durable-outbox-memory-store
uv add durable-outbox-blob-store
uv add durable-outbox-cosmos-store
```

Install local plugins while developing them:

```bash
uv pip install -e ../durable-outbox-example-sink
uv add ../durable-outbox-example-store
```

After installation, applications can inspect names without importing provider
implementation modules:

```python
from durable_outbox import available_sinks, available_stores

print(available_sinks())
print(available_stores())
```

## Verification

Plugin packages should have their own tests and quality gates:

```bash
uv sync --group dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv build
```

Add a small entry point test that installs the plugin package and confirms the
loader can discover and instantiate it:

```python
from durable_outbox import available_sinks, load_sink


def test_example_sink_plugin_loads() -> None:
    assert "example-file" in available_sinks()
    sink = load_sink("example-file", {"path": "published.jsonl"})
    assert sink is not None
```

Store plugin authors should also run the durable outbox provider contract
matrix:

```python
from durable_outbox.testing import ProviderContract, run_provider_contract


async def test_provider_contract() -> None:
    await run_provider_contract(ProviderContract(store_factory=make_store))
```

The provider contract covers idempotent puts, incompatible duplicate detection,
claim and retry transitions, failover replay, ordered-key blocking, cleanup
freeze/resume, and admin repair/replay behavior.

## Publishing

Before publishing a plugin package:

- Include `py.typed` when the package is typed.
- Keep provider SDKs in the plugin package dependencies, not in
  `durable-outbox`.
- Keep entry point names stable because applications use them in configuration.
- Document required configuration keys and their expected types.
- Build and inspect the wheel before upload.
