<!-- ITO:START -->
## ADDED Requirements

### Requirement: Outbox Metrics
The package SHALL expose metrics for pending, in-flight, sent, failed, oldest pending age, claim conflicts, stale reclaims, put latency, publish latency, failover replay, and cleanup freeze state.

#### Scenario: dispatcher and store operations run
- **WHEN** metrics adapter is configured
- **THEN** standard metrics are emitted with store, topic, and environment labels where applicable

### Requirement: Status And Admin Hooks
The package SHALL expose service-level hooks for status inspection, manual replay, and FAILED-to-PENDING repair without logging or mutating opaque payloads.

#### Scenario: operator requests failed event repair
- **WHEN** authorization is handled by the hosting service
- **THEN** the package transitions the repaired event to PENDING and records audit metadata

### Requirement: Failover Runbooks And Alerts
The operational package SHALL document alerts and runbooks for backpressure, failed events, stuck replay, cleanup during failover, and degraded RPO=0 providers.

#### Scenario: failover drill is executed
- **WHEN** runbook steps are followed
- **THEN** cleanup remains frozen until replay completion is confirmed

### Requirement: Load And Failure Injection
The package SHALL include load and failure-injection tests for MVP throughput and no-loss behavior under Kafka, process, and storage failure modes.

#### Scenario: Kafka ack succeeds then mark_sent fails
- **WHEN** failure injection triggers the crash window
- **THEN** the event remains replayable and may be duplicated but is not lost

<!-- ITO:END -->
