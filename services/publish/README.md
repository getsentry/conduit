# Publish

## Stream Lifecycle & Cleanup

### Activity Tracking

Each publish updates a stream's activity timestamp in Redis. This allows automatic cleanup of abandoned streams.

### Cleanup Worker

A background worker runs periodically (configurable via `CLEANUP_WORKER_INTERVAL_SEC`, default 300s) and deletes streams with no activity for a configurable duration (via `CLEANUP_STREAM_IDLE_SEC`, default 300s).

### Phase::End Behavior

Streams reaching `Phase::End` are:

1. Set to expire after `STREAM_TTL_SEC` seconds via Redis TTL
2. Untracked from the cleanup worker (if TTL succeeds)
3. Cleaned up by the worker if TTL setting fails (fallback)

This prevents memory leaks from crashed clients or incomplete streams while allowing proper TTL-based cleanup for completed streams.

## API Limits

### Message Size

Publish requests are limited to 32KB (configurable via `MAX_MESSAGE_SIZE_BYTES`).
Requests exceeding this limit are rejected with `413 Payload Too Large`.

Combined with the stream length limit of 500 messages, this bounds maximum stream size to approximately 16MB per stream.

Publishers handling large data should chunk it into multiple DELTA messages within the START/DELTA/END streaming pattern.

### Stream Length

Streams are automatically trimmed to approximately 500 messages (configurable via `MAX_STREAM_LEN`). Older messages are removed as new ones arrive.

## Design Decisions

### Track-Before-Publish Order

We track stream activity _before_ publishing to prevent orphaned streams.
If tracking fails, the publish is aborted. This assumes:

- Publishers retry on errors
- Streams publish frequently (real-time streaming use-case)
- Preventing orphaned streams is more important than optimistic publishing

If a tracking failure occurs on an existing stream close to the cleanup threshold, the publisher's retry will update the timestamp and prevent premature deletion.

### Worker Intervals

Both `CLEANUP_WORKER_INTERVAL_SEC` and `CLEANUP_STREAM_IDLE_SEC` can be tuned independently based on operational needs.

### Known Edge Cases

- **Sorted set bloat**: If untrack operations consistently fail, the sorted set accumulates entries for already deleted streams. The worker will attempt to delete non-existent streams (harmless) but the sorted set grows. If this becomes a problem, we can add a periodic SCAN to remove ghost entries.
