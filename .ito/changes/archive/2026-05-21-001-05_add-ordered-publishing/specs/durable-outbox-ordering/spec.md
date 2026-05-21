<!-- ITO:START -->
## ADDED Requirements

### Requirement: Ordered Event Validation
The package SHALL require ordered-mode events to carry an ordering_key and SHALL prefer producer-supplied ordering_sequence for robust sequencing.

#### Scenario: ordered event is missing ordering_key
- **WHEN** put or dispatch validation runs
- **THEN** the event is rejected with a deterministic validation error

### Requirement: Per Key Sequential Dispatch
The dispatcher SHALL publish at most one IN_FLIGHT event per ordering key and SHALL block later same-key events until the earlier event is SENT or FAILED.

#### Scenario: two events share an ordering key
- **WHEN** the first event is pending or in flight
- **THEN** the second event is not published before the first reaches a terminal dispatch state

### Requirement: Different Key Concurrency
Ordered mode SHALL allow events with different ordering keys to be claimed and published concurrently.

#### Scenario: events use different ordering keys
- **WHEN** ordered dispatcher runs with available workers
- **THEN** both keys can progress without waiting on each other

### Requirement: Blob Ordering Locks
The Blob implementation SHALL coordinate per-key ordered publishing with lock blobs or leases that expire safely after dispatcher failure.

#### Scenario: dispatcher crashes while holding key lock
- **WHEN** lease expires
- **THEN** another dispatcher can acquire the key and continue publishing

<!-- ITO:END -->
