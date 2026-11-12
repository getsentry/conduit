# Gateway load shedding

The Gateway checks load before accepting a new `GET /events/{org_id}` SSE connection. If any
enabled monitor decides to shed, the request receives `429 Too Many Requests` with
`Server is overloaded`; existing connections are not closed. Below the soft limit, requests
are accepted. Between the soft and hard limits, the chance of shedding increases linearly
from 0% to 100%; at or above the hard limit, every new connection is shed. Each shedding
decision increments `loadshedding.shed` with a `monitor` tag.

Set a monitor's `HARD_LIMIT` environment variable to enable it; without one it is disabled.
`SOFT_LIMIT` defaults to the hard limit (immediate shedding at the threshold) and is capped
at the hard limit. Limits are non-negative integers. `PERIOD_MS` defaults to 1000 ms; zero
falls back to the default. Enabled monitors are initialized at startup. Polled monitors run
on a dedicated thread and are polled once before the Gateway starts accepting requests.

| Monitor | Measurement | Hard limit (enables monitor) | Soft limit | Poll period |
| --- | --- | --- | --- | --- |
| Memory | Container anonymous memory in bytes (`anon` from cgroup v2 `memory.stat`, or `total_rss` from cgroup v1) | `LOADSHEDDING_MEMORY_HARD_LIMIT` (bytes) | `LOADSHEDDING_MEMORY_SOFT_LIMIT` (bytes) | `LOADSHEDDING_MEMORY_PERIOD_MS` (default: 1000 ms) |
| Concurrent clients | Process-wide count of connected SSE clients | `LOADSHEDDING_CONCURRENT_CLIENTS_HARD_LIMIT` (clients) | `LOADSHEDDING_CONCURRENT_CLIENTS_SOFT_LIMIT` (clients) | `LOADSHEDDING_CONCURRENT_CLIENTS_PERIOD_MS` (no effect; count is read live) |

The memory monitor requires the container's own memory cgroup; enabling it without a
readable cgroup prevents Gateway startup. If a later read fails, it retains the last value.
The concurrent-clients monitor reads the live count when deciding whether to shed, so
its configured poll period does not affect decisions.
