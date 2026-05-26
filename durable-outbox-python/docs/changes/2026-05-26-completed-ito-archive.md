# Completed Ito Change Archive - 2026-05-26

This document records the completed durable outbox Ito changes archived on
2026-05-26. The Ito archive itself lives under `.ito/changes/archive`, which is
an ignored local symlink, so this repo-tracked note provides the durable docs
record for review and commit history.

## Archived Changes

### `001.01-02_generalize-dispatcher-ack-path`

Generalized the dispatcher acknowledgement path so sink publish failures are
separated from post-ack store update failures. Dispatcher metrics now use
generic outbox publish naming, and dispatch summaries include explicit
post-ack conflict counts.

Affected areas:
- `durable-outbox-core`
- `durable-outbox-operations`
- `durable_outbox.core.dispatcher`
- Dispatcher tests and operations documentation

### `001.01-03_harden-core-contracts`

Hardened core contracts by rejecting ambiguous or provider-dependent inputs at
the boundary. The change covers naive datetimes, invalid ordered event metadata,
non-positive claim and replay limits, payload-size enforcement, and duplicate
event conflict normalization.

Affected areas:
- `durable-outbox-core`
- `durable-outbox-provider-contract`
- `durable_outbox.core`
- Store implementations and provider contract tests

### `001.01-04_inject-store-clocks`

Added optional clock injection to store implementations so lifecycle timestamps
can be deterministic in tests while preserving system-clock defaults for
production callers.

Affected areas:
- `durable-outbox-core`
- `durable-outbox-provider-contract`
- In-memory, Blob, Cosmos, and SQL stores
- Store timestamp tests

### `001.10-01_add-provider-cas-contracts`

Added compare-and-set provider contracts for Cosmos and SQL backed stores.
Stored event models now carry record versions, provider protocols expose
conditional updates, and claim/terminal transitions use CAS boundaries so only
one claimant can win.

Affected areas:
- `durable-outbox-cosmos-provider`
- `durable-outbox-sql-provider`
- `durable-outbox-provider-contract`
- `durable_outbox.stores.cosmos`
- `durable_outbox_sql_store`
- Shared provider concurrency tests

## Repository Note

Ito's archived change directories are intentionally not tracked in this
repository because `.ito/changes`, `.ito/specs`, `.ito/modules`, `.ito/audit`,
and `.ito/workflows` are ignored symlinks into local Ito storage. Commit this
file when a visible repository archive record is needed.
