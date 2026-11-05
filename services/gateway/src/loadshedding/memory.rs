use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use super::MetricMonitor;

/// A cgroup stats file with `<key> <bytes>` lines.
#[derive(Debug, Clone, Copy)]
struct Source {
    path: &'static str,
    key: &'static str,
}

const CGROUP_V2: Source = Source {
    path: "/sys/fs/cgroup/memory.stat",
    key: "anon",
};

const CGROUP_V1: Source = Source {
    path: "/sys/fs/cgroup/memory/memory.stat",
    key: "total_rss",
};

/// Only exists in non-root cgroups. If it's missing, `/sys/fs/cgroup` is the
/// host's root cgroup (the container shares the host's cgroup namespace), and
/// its `memory.stat` would be node-wide.
const CGROUP_V2_NON_ROOT_MARKER: &str = "/sys/fs/cgroup/memory.current";

impl Source {
    async fn read(&self) -> Option<u64> {
        let contents = tokio::fs::read_to_string(self.path).await.ok()?;
        parse_field(&contents, self.key)
    }
}

fn parse_field(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next()? != key {
            return None;
        }
        parts.next()?.parse().ok()
    })
}

/// Anonymous memory usage (in bytes) of the current container, read from its
/// memory cgroup. Requires running in a container with its own cgroup.
pub struct MemoryMonitor {
    source: Source,
    last_value: AtomicU64,
}

impl MemoryMonitor {
    pub async fn new() -> anyhow::Result<Self> {
        let source = if tokio::fs::try_exists(CGROUP_V2_NON_ROOT_MARKER)
            .await
            .unwrap_or(false)
        {
            CGROUP_V2
        } else {
            CGROUP_V1
        };
        let value = source.read().await.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to read `{}` from {}; memory load shedding requires running \
                 in a container with its own memory cgroup",
                source.key,
                source.path
            )
        })?;
        Ok(Self {
            source,
            last_value: AtomicU64::new(value),
        })
    }
}

#[async_trait]
impl MetricMonitor for MemoryMonitor {
    async fn poll(&self) {
        match self.source.read().await {
            Some(value) => self.last_value.store(value, Ordering::Relaxed),
            None => tracing::warn!("Failed to read memory usage from {}", self.source.path),
        }
    }

    fn last_value(&self) -> u64 {
        self.last_value.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_cgroup_stat() {
        let contents = "anon 1048576\nfile 2097152\nanon_thp 0\n";
        assert_eq!(parse_field(contents, "anon"), Some(1048576));
        assert_eq!(parse_field(contents, "file"), Some(2097152));
        assert_eq!(parse_field(contents, "shmem"), None);
    }

    #[test]
    fn test_parse_field_invalid_value() {
        assert_eq!(parse_field("anon abc\n", "anon"), None);
        assert_eq!(parse_field("anon\n", "anon"), None);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_memory_monitor_polls() {
        let monitor = MemoryMonitor::new().await.unwrap();
        monitor.poll().await;
        assert!(monitor.last_value() > 0);
    }
}
