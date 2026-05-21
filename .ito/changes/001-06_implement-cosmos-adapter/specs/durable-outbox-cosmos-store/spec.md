<!-- ITO:START -->
## ADDED Requirements

### Requirement: Cosmos Idempotent Acceptance
The Cosmos store SHALL create one item per event_id and SHALL treat duplicate creates as idempotent success for the same accepted event.

#### Scenario: duplicate put occurs
- **WHEN** an item with the event id already exists
- **THEN** the store returns an accepted receipt without duplicating the item

### Requirement: Cosmos Partitioning
The Cosmos store SHALL partition unordered events by topic bucket and ordered events by topic plus ordering key hash.

#### Scenario: ordered event is stored
- **WHEN** ordering_key is present
- **THEN** the partition key colocates same-key events for ordered coordination

### Requirement: Cosmos Claiming
The Cosmos store SHALL use ETag conditional updates so only one publisher can transition a PENDING event to IN_FLIGHT.

#### Scenario: two publishers claim one item
- **WHEN** both use the current ETag
- **THEN** only one patch succeeds

### Requirement: Cosmos RPO0 Capability Validation
The Cosmos adapter SHALL declare RPO=0 only when configured for strong consistency, more than one region, and a single write region.

#### Scenario: multi-write or session consistency is configured
- **WHEN** capabilities are evaluated
- **THEN** rpo_zero_for_accepted_events is false or startup validation fails for certified mode

<!-- ITO:END -->
