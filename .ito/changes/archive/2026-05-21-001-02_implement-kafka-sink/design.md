<!-- ITO:START -->
## Context

This change is part of the `durable-outbox-python` module derived from `durable-outbox-python/reusable_durable_outbox_rpo0_proposal.md`. The package must preserve accepted events with at-least-once delivery and explicit provider capability declarations.

## Goals / Non-Goals

**Goals:**

- Deliver implement kafka sink as a focused package increment.
- Preserve the accepted-event and at-least-once contracts defined by the durable outbox proposal.
- Keep provider-specific concerns behind stable core contracts.

**Non-Goals:**

- Exactly-once end-to-end delivery.
- Consumer-side deduplication implementation.
- Kafka cluster DR, topic lifecycle management, or cloud infrastructure provisioning.

## Approach

Keep Kafka behind the MessageSink protocol. Use an async wrapper around confluent-kafka delivery callbacks, validate certified producer configuration during construction, and expose failure classes that the core dispatcher already understands.

## Contracts / Interfaces

- Core package contracts use asynchronous Python protocols and dataclasses.
- Provider capability declarations must state whether accepted events are certified RPO=0.
- Public behavior is specified in `specs/durable-outbox-kafka-sink/spec.md`.

## Data / State

The relevant lifecycle states are `PENDING`, `IN_FLIGHT`, `SENT`, and `FAILED`. Provider internals may add hidden states such as `PREPARED`, but dispatchers must only process accepted events.

## Decisions

- Use at-least-once semantics and require consumers to dedupe by `event_id`.
- Treat payloads as opaque bytes or claim-check references.
- Prefer provider certification tests over provider-specific assertions in core tests.

## Risks / Trade-offs

- Behavioral contracts are stateful -> mitigate with provider certification tests and failure injection.
- RPO=0 declarations can be misconfigured -> mitigate with explicit capability validation.
- Additional durability boundaries may add latency -> expose metrics and document certified versus non-certified modes.

## Verification Strategy

Run the targeted pytest suite for this change and run `ito validate 001-02_implement-kafka-sink --strict`. Provider changes must pass the shared provider contract tests.

## Migration / Rollback

This is initial package work. Rollback is removing the new package increment before adoption by the EVA publisher.

## Open Questions

Open questions from the source proposal remain tracked at module level, especially retention TTL authority, maximum event size, replay rate, tenant isolation, and FAILED retention.
<!-- ITO:END -->
