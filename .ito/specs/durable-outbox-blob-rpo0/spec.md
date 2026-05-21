<!-- ITO:START -->
## ADDED Requirements

### Requirement: Dual Region RPO0 Acceptance
The dual-region Blob store SHALL declare RPO=0 only when put returns success after the event is accepted in both configured regional outboxes.

#### Scenario: dual write succeeds
- **WHEN** Region A and Region B both record accepted=true for event_id
- **THEN** put returns an accepted receipt with rpo_zero true

### Requirement: Prepared State Repair
The dual-region Blob store SHALL use an internal PREPARED state and idempotent repair flow so partial writes converge without exposing unaccepted events to dispatchers.

#### Scenario: Region A accepted and Region B is missing
- **WHEN** repair runs with access to the Region A record
- **THEN** Region B is written and marked accepted before the event becomes dispatchable there

### Requirement: Failover Replay Predicate
Failover replay SHALL include accepted PENDING, IN_FLIGHT, and SENT events whose expires_at is greater than or equal to failover_started_at.

#### Scenario: event was SENT before failover
- **WHEN** expires_at is after failover_started_at
- **THEN** the failover replayer republishes the event

### Requirement: Cleanup Freeze
The store SHALL freeze cleanup at failover start and SHALL keep cleanup frozen until replay completion is recorded.

#### Scenario: failover begins
- **WHEN** cleanup is scheduled
- **THEN** cleanup skips deletion while failover_freeze is true

<!-- ITO:END -->
