# Cleanup

Background service that removes inactive streams from Redis.

Runs on a configurable interval (`CLEANUP_INTERVAL_SEC`, default 120s) and deletes streams with no activity beyond the idle threshold (`CLEANUP_STREAM_IDLE_SEC`, default 120s).

Streams that complete normally via Phase::End are cleaned up via Redis TTL instead.
