<!-- ITO:START -->
## Approach

Create narrow protocols for status and administrative transitions. Hosting services remain responsible for authentication and authorization. The package records audit metadata and never logs or mutates opaque payload content.

## Contracts / Interfaces

- `OutboxStatusReader` summarizes counts and event metadata.
- `OutboxAdminActions` supports manual replay and FAILED-to-PENDING repair.
- `AuditSink` records operator, reason, timestamp, and event id.

## Verification Strategy

Unit-test protocol-backed admin service against fake store adapters and ensure payload bytes are never emitted in status or audit records.
<!-- ITO:END -->
