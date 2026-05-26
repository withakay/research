from importlib.metadata import PackageNotFoundError, version

from durable_outbox.core.admin import AdminActionStatus
from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.cleanup import CleanupPolicy, CleanupScheduler
from durable_outbox.core.dispatcher import DispatchSummary, OutboxDispatcher
from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    DuplicateEventConflictError,
    DurableOutboxError,
    NonRetryablePublishError,
    RetryablePublishError,
    RetryableStoreError,
    ValidationError,
)
from durable_outbox.core.model import (
    AcceptedReceipt,
    ClaimedEvent,
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.core.retry import RetryPolicy
from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.plugins import (
    SinkFactory,
    StoreFactory,
    available_sinks,
    available_stores,
    load_sink,
    load_store,
)

try:
    __version__ = version("durable-outbox")
except PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = [
    "AcceptedReceipt",
    "AdminActionStatus",
    "ClaimConflictError",
    "ClaimedEvent",
    "CleanupPolicy",
    "CleanupScheduler",
    "ConfigurationError",
    "DispatchSummary",
    "DuplicateEventConflictError",
    "DurableOutboxError",
    "DurableOutboxStore",
    "MessageSink",
    "NonRetryablePublishError",
    "OutboxCapabilities",
    "OutboxDispatcher",
    "OutboxEvent",
    "OutboxStatus",
    "PublishResult",
    "PublishingMode",
    "RetryPolicy",
    "RetryablePublishError",
    "RetryableStoreError",
    "SinkFactory",
    "StoreFactory",
    "ValidationError",
    "__version__",
    "available_sinks",
    "available_stores",
    "load_sink",
    "load_store",
]
