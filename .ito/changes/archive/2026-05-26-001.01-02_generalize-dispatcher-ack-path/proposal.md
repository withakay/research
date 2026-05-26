# Change: Generalize dispatcher acknowledgement path

## Why
The dispatcher is sink-agnostic by design, but its metrics are Kafka-specific and it treats store update failures after a sink acknowledgement like publish failures. That obscures the at-least-once boundary and can trigger incorrect retry state transitions.

## What Changes
- Rename dispatcher metrics to generic outbox publish names while keeping behavior sink-neutral.
- Separate sink publish failures from post-ack store update failures.
- Return explicit summary counts for post-ack store update conflicts.

## Impact
- Affected specs: `durable-outbox-core`, `durable-outbox-operations`
- Affected code: `durable_outbox.core.dispatcher`, tests, docs
