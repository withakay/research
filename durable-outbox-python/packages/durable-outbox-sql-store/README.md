# durable-outbox-sql-store

SQL store plugin package for `durable-outbox`.

Install it alongside the core package, then load an Azure SQL sync or SQL Server
Always On store through the store plugin registry:

```python
from durable_outbox.plugins import load_store

store = load_store(
    "azure-sql-sync",
    {"connection_string": "Driver={ODBC Driver 18 for SQL Server};..."},
)
```
