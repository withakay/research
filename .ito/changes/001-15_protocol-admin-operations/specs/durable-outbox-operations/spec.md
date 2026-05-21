<!-- ITO:START -->
## MODIFIED Requirements

### Requirement: Protocol Backed Admin Hooks
Admin operations SHALL depend on status and repair protocols rather than fake-store internals.

#### Scenario: real adapter provides admin hooks
- **WHEN** the admin service is constructed
- **THEN** it can inspect and repair events without requiring `FakeOutboxStore`

### Requirement: Audit Metadata
Manual repair and replay actions SHALL record event id, operator identity supplied by the host, reason, and timestamp without recording payload bytes.

#### Scenario: operator repairs a failed event
- **WHEN** repair succeeds
- **THEN** audit metadata is emitted and the event is returned to PENDING
<!-- ITO:END -->
