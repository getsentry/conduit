# Publish

## Stream Lifecycle & Cleanup

### Activity Tracking

Each publish updates a stream's activity timestamp in Redis. This allows automatic cleanup of abandoned streams.

### Cleanup Worker

A background worker runs every `WORKER_INTERVAL_SEC` and deletes streams with no activity for `STREAM_IDLE_SEC` seconds.

### Phase::End Behavior

Streams reaching `Phase::End` are:

1. Set to expire after `STREAM_TTL_SEC` seconds via Redis TTL
2. Untracked from the cleanup worker (if TTL succeeds)
3. Cleaned up by the worker if TTL setting fails (fallback)

This prevents memory leaks from crashed clients or incomplete streams while allowing proper TTL-based cleanup for completed streams.
