# Design: Provider Compare-And-Set

## Conditional Update Contract
Provider clients expose `replace(record, expected_version=...) -> StoredEvent`. A successful replace increments the version and returns the persisted copy. A version mismatch raises `ClaimConflictError`.

## Why Versions
The in-memory clients are test doubles for real provider ETag/row-version semantics. Explicit versions keep the production protocol honest without binding the public store API to any single backend's concurrency token type.

## Claim Flow
Stores list candidate copies, mutate a candidate copy to `IN_FLIGHT`, and replace it with the expected version. Losing races are skipped so another instance can continue claiming other records.
