<!-- ITO:START -->
## Why

RPO=0 correctness depends on ambiguous timeout and partial-write cases, not only the successful dual-write path.

## What Changes

- Add explicit repair scenarios for prepared/missing and accepted/missing regional divergence.
- Ensure dispatchers never claim unaccepted PREPARED records.
- Add failover replay tests for `PENDING`, `IN_FLIGHT`, and `SENT` records selected by `expires_at >= failover_started_at`.
- Add cleanup-freeze tests across replay start, failure, and completion.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: tests
- **Design Needed**: yes
- **Design Reason**: RPO=0 is a write-acknowledgement contract with failure-mode requirements.

## Capabilities

### Modified Capabilities

- `durable-outbox-blob-rpo0`

## Impact

Blob RPO=0 certification becomes failure-mode driven rather than success-path-only.
<!-- ITO:END -->
