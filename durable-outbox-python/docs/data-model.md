# Durable Outbox Data Model

This package uses the Python dataclass field names as the canonical logical
model. Adapter storage names may differ where a backend has a stronger native
convention, but new JSON document shapes should prefer snake_case field names
and `*_at_epoch_ms` timestamp suffixes for indexed or queryable timestamps.

## Timestamp Conventions

- Canonical Python fields are timezone-aware `datetime` values named
  `*_at`.
- Blob metadata uses `*_at_epoch_ms` because metadata is string-only and often
  used for discovery or diagnostics.
- Blob JSON record content currently stores ISO-8601 strings under canonical
  `*_at` keys.
- Future Cosmos JSON documents should use snake_case keys and
  `*_at_epoch_ms` for queryable timestamps.
- SQL keeps `*_at_utc` and `next_attempt_utc` columns because `DATETIME2`
  values already carry typed UTC semantics in the schema.

## Adapter Field Rendering

| Canonical Field | Blob Rendering | Cosmos JSON Rendering | SQL Rendering |
|---|---|---|---|
| `event_id` | metadata `event_id`; JSON `event.event_id`; blob name hash | `id`, `event_id` if duplicated for queries | `event_id` |
| `topic` | metadata `topic`; JSON `event.topic` | `topic` | `topic` |
| `key` | JSON `event.key` base64 | `key` base64 | `kafka_key` |
| `headers` | JSON `event.headers` base64 values | `headers` base64 values | `headers_json` |
| `payload` | JSON `event.payload` base64 or claim-check data | `payload` base64 or claim-check URI | `payload` |
| `schema_id` | JSON `event.schema_id` | `schema_id` | `schema_id` |
| `schema_version` | JSON `event.schema_version` | `schema_version` | `schema_version` |
| `ordering_key` | JSON `event.ordering_key`; metadata may store `ordering_key_hash` | `ordering_key`, `ordering_key_hash` | `ordering_key`, `ordering_key_hash` |
| `ordering_sequence` | metadata and JSON `ordering_sequence` | `ordering_sequence` | `ordering_sequence` |
| `publishing_mode` | JSON `event.publishing_mode` | `publishing_mode` | `publishing_mode` |
| `created_at` | metadata `created_at_epoch_ms`; JSON `event.created_at` | `created_at_epoch_ms` | `created_at_utc` |
| `expires_at` | metadata `expires_at_epoch_ms`; JSON `event.expires_at` | `expires_at_epoch_ms` | `expires_at_utc` |
| `accepted` | metadata and JSON `accepted` | `accepted` | row existence plus status |
| `accepted_at` | JSON `accepted_at` | `accepted_at_epoch_ms` | `accepted_at_utc` |
| `status` | metadata and JSON `status` | `status` | `status` |
| `attempt_count` | metadata and JSON `attempt_count` | `attempt_count` | `attempt_count` |
| `claim_token` | JSON `claim_token`; metadata may expose only claim id | `claim_id` | `claim_id` |
| `claimed_at` | metadata `claimed_at_epoch_ms`; JSON `claimed_at` | `claimed_at_epoch_ms` | `claimed_at_utc` |
| `next_attempt_at` | JSON `next_attempt_at` | `next_attempt_at_epoch_ms` | `next_attempt_utc` |
| `sent_at` | metadata `sent_at_epoch_ms`; JSON `sent_at` | `sent_at_epoch_ms` | `sent_at_utc` |
| `publish_result.partition` | metadata `kafka_partition`; JSON `publish_result.partition` | `kafka_partition` | `kafka_partition` |
| `publish_result.offset` | metadata `kafka_offset`; JSON `publish_result.offset` | `kafka_offset` | `kafka_offset` |
| `publish_result.published_at` | JSON `publish_result.published_at` | `published_at_epoch_ms` | `published_at_utc` |
| `failed_at` | JSON `failed_at` | `failed_at_epoch_ms` | `failed_at_utc` |
| `last_error_type` | metadata and JSON `last_error_type` | `last_error_type` | `last_error_type` |
| `last_error` | metadata and JSON `last_error` | `last_error` | `last_error` |
| `cleanup_freeze.reason` | control blob JSON `reason` | control item `reason` | cleanup table `reason` |
| `cleanup_freeze.frozen_at` | future control blob `frozen_at_epoch_ms` | `frozen_at_epoch_ms` | `frozen_at_utc` |
| `event_fingerprint` | metadata `event_fingerprint` | optional `event_fingerprint` | optional computed diagnostic |

The canonical names above are a compatibility contract for documentation,
tests, and future provider implementations. Changing an adapter rendering is a
schema migration even when the Python field name stays stable.
