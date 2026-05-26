# Change: Add provider compare-and-set contracts

## Why
Cosmos and SQL provider clients expose get/list/insert/delete operations but no compare-and-set update boundary. That is not enough to model distributed single-winner claiming or stale owner protection for real providers.

## What Changes
- Add record version fields to Cosmos and SQL stored event models.
- Add conditional update methods to provider client protocols.
- Route claim and terminal transitions through compare-and-set updates.
- Add shared-client tests that prove only one claimant wins.

## Impact
- Affected specs: `durable-outbox-cosmos-provider`, `durable-outbox-sql-provider`, `durable-outbox-provider-contract`
- Affected code: `durable_outbox.stores.cosmos`, `durable_outbox.stores.sql`, provider tests
