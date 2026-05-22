class DurableOutboxError(Exception):
    """Base class for durable outbox errors."""


class ValidationError(DurableOutboxError, ValueError):
    """Raised when envelope metadata is invalid."""


class ConfigurationError(DurableOutboxError):
    """Raised when adapter configuration cannot satisfy the requested contract."""


class RetryablePublishError(DurableOutboxError):
    """Raised when publishing may succeed after retry."""


class NonRetryablePublishError(DurableOutboxError):
    """Raised when a deterministic sink error should mark an event failed."""


class RetryableStoreError(DurableOutboxError):
    """Raised when a store operation has an ambiguous or retryable result."""


class ClaimConflictError(DurableOutboxError):
    """Raised by stores when an optimistic claim update loses a race."""


class DuplicateEventConflictError(DurableOutboxError):
    """Raised when a duplicate event_id is reused for incompatible event data."""
