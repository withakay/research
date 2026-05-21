<!-- ITO:START -->
## Approach

Model dual-region acceptance as two internal stages: PREPARED and accepted. Repair is idempotent and derives missing regional state from a valid accepted or prepared source record according to policy.

## Contracts / Interfaces

- `put()` returns success only when both regions are accepted.
- Repair can complete partial writes without exposing unaccepted records to normal dispatch.
- Failover replay freezes cleanup before selecting candidates.

## Verification Strategy

Add table-driven partial-write tests and replay predicate tests using a fake dual-region provider client before binding to live Blob.
<!-- ITO:END -->
