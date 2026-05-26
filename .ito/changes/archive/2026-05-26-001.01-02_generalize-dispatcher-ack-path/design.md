# Design: Dispatcher Ack Boundary

## Ack Boundary
Once `sink.publish(event)` returns, downstream publication has been acknowledged. If `store.mark_sent()` then fails, the dispatcher must not call `mark_pending_after_retryable_failure()` as though the publish failed. The store can reclaim an in-flight event after the claim timeout, preserving at-least-once delivery without corrupting the state transition.

## Metrics
Generic names use `outbox_publish_*` so the same dispatcher can be used with Kafka or any other sink.
