<!-- ITO:START -->
## Why

The core RPO=0 contract for Blob requires application-level dual writes, not Azure asynchronous geo-replication labels. This change adds the certified dual-region acceptance and failover replay behavior required to avoid accepted-event loss during regional failover.

## What Changes

- Implement dual-region put with PREPARED then accepted=true transitions.
- Return success only after both regional outboxes have accepted the event.
- Add partial-write repair for ambiguous timeout and regional divergence cases.
- Implement failover replay using expires_at >= failover_started_at, including SENT events.
- Freeze cleanup during failover replay and resume it after replay completion.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-blob-rpo0`: Add Dual Region Blob RPO0 And Failover Replay.

### Modified Capabilities

None.

## Impact

Adds certified RPO=0 Blob provider behavior, failover operations, and additional provider contract tests. Acceptance latency increases because both regions must commit before success.
<!-- ITO:END -->
