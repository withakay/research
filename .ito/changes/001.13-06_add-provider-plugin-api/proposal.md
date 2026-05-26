# Change: Add Provider Plugin API

## Why
Stores and sinks should be configurable through independently installed packages instead of direct imports from the core distribution. A small plugin registry gives the core package a stable extension point before providers are extracted.

## What Changes
- Add entry point based discovery for durable outbox store and sink factories.
- Add loader APIs for resolving a named store or sink from configuration.
- Define factory protocols that return existing `DurableOutboxStore` and `MessageSink` implementations.
- Treat missing plugin packages and invalid plugin factories as `ConfigurationError` cases.
- Do not keep compatibility imports for extracted providers; this PoC can break cleanly.

## Impact
- Affected specs: `durable-outbox-plugin-api`, `durable-outbox-core`, `durable-outbox-packaging`
- Affected code: new core plugin module, public exports, tests, README/provider docs
- Sequencing: implement after workspace/build setup and before extracting provider packages.
