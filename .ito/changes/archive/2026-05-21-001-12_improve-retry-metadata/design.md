<!-- ITO:START -->
## Approach

Extend `ClaimedEvent` with an `attempt_count` field. Stores set it when a claim is issued. The dispatcher passes that count into `RetryPolicy.next_attempt_at()`. Retry policy remains deterministic by default and accepts optional jitter later.

## Contracts / Interfaces

- `ClaimedEvent.attempt_count` represents the claim attempt that is currently being processed.
- Retryable publish failures use `attempt_count` to schedule the next attempt.
- Existing stores default to attempt count `1` when not provided.

## Decisions

- Keep backward compatibility by giving the new dataclass field a default.
- Avoid random jitter until a deterministic testable jitter hook is introduced.

## Verification Strategy

Add tests that a second failed dispatch schedules a later retry than the first and respects the maximum delay cap.
<!-- ITO:END -->
