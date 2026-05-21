<!-- ITO:START -->
## Why

A durable outbox is operationally useful only if teams can observe backlog, failures, replay progress, cleanup state, and provider degradation. This change adds the status, metrics, alerting, runbook, load-test, and failure-injection work needed before production use.

## What Changes

- Add metrics adapter and standard metric names for outbox, Kafka, failover, and cleanup behavior.
- Add tracing hooks and status API integration points.
- Add admin replay and failed-event repair hooks with authorization boundaries for service integration.
- Define dashboards, alerts, failover runbook, and manual replay runbook.
- Add load tests and failure-injection tests for the 1000 messages/min/topic MVP target.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-operations`: Add Operational Hardening.

### Modified Capabilities

None.

## Impact

Adds optional OpenTelemetry and API integration surface. Operational endpoints must be protected by the hosting service.
<!-- ITO:END -->
