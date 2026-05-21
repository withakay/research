<!-- ITO:START -->
## MODIFIED Requirements

### Requirement: Partial Write Repair Matrix
The dual-region Blob store SHALL repair prepared/missing, accepted/prepared, accepted/missing, and both-prepared cases idempotently.

#### Scenario: primary accepted and secondary missing
- **WHEN** repair runs with a policy-permitted source record
- **THEN** the secondary is written and accepted before it is dispatchable

### Requirement: Prepared Records Are Not Dispatchable
Normal dispatch SHALL ignore records that are not accepted, including internal PREPARED records.

#### Scenario: timeout after prepare
- **WHEN** dispatcher claims events
- **THEN** no unaccepted prepared record is returned
<!-- ITO:END -->
