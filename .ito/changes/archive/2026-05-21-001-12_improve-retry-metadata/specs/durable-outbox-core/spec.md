<!-- ITO:START -->
## MODIFIED Requirements

### Requirement: Retry Attempt Metadata
Claimed events SHALL expose the current attempt count so dispatchers can compute retry backoff from durable store state.

#### Scenario: repeated transient failures occur
- **WHEN** the same event is claimed and fails multiple times
- **THEN** each retry uses the incremented attempt count to schedule a later next attempt time

### Requirement: Retry Delay Cap
Retry policy SHALL cap exponential backoff at the configured maximum delay.

#### Scenario: attempt count is high
- **WHEN** retry delay is computed
- **THEN** the delay does not exceed the configured maximum
<!-- ITO:END -->
