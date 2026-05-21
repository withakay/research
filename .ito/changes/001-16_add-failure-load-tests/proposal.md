<!-- ITO:START -->
## Why

The most important at-least-once guarantee is the crash window where Kafka acknowledges but `mark_sent` fails. The package also needs a repeatable MVP throughput check.

## What Changes

- Add failure-injection tests for ack-before-mark-sent, process restart, retryable store write timeout, claim conflict, and failover replay duplicates.
- Add lightweight load tests for 1000 messages/min/topic using fake providers.
- Mark slow or integration load tests separately from fast unit tests.
- Record expected duplicate/no-loss outcomes in test assertions.

## Change Shape

- **Type**: feature
- **Risk**: medium
- **Stateful**: yes
- **Public Contract**: tests
- **Design Needed**: yes
- **Design Reason**: Verification must exercise failure windows, not only normal paths.

## Capabilities

### New Capabilities

- `durable-outbox-verification`: Failure injection and load verification suite.

## Impact

No-loss behavior becomes executable evidence rather than only documentation.
<!-- ITO:END -->
