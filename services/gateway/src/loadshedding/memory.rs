use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use super::MetricMonitor;

/// A file with `<key> <value>` lines to read memory usage from.
#[derive(Debug, Clone, Copy)]
struct Source {
    path: &'static str,
    key: &'static str,
    /// Multiplier converting the value to bytes.
    unit: u64,
}

/// Tried in order; the first readable one is used.
const SOURCES: [Source; 3] = [
    // cgroup v2: anonymous memory of the container.
    Source {
        path: "/sys/fs/cgroup/memory.stat",
        key: "anon",
        unit: 1,
    },
    // cgroup v1
    Source {
        path: "/sys/fs/cgroup/memory/memory.stat",
        key: "total_rss",
        unit: 1,
    },
    // Not in a cgroup: this process's RSS, e.g. `VmRSS:   12345 kB`.
    Source {
        path: "/proc/self/status",
        key: "VmRSS:",
        unit: 1024,
    },
];

impl Source {
    async fn read(&self) -> Option<u64> {
        let contents = tokio::fs::read_to_string(self.path).await.ok()?;
        parse_field(&contents, self.key).map(|value| value * self.unit)
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

/// Memory usage (in bytes) of the current container, read from cgroup stats
/// (or `/proc/self/status` outside a cgroup). Linux only.
pub struct MemoryMonitor {
    source: Source,
    last_value: AtomicU64,
}

impl MemoryMonitor {
    pub async fn new() -> anyhow::Result<Self> {
        for source in SOURCES {
            if let Some(value) = source.read().await {
                return Ok(Self {
                    source,
                    last_value: AtomicU64::new(value),
                });
            }
        }
        let paths: Vec<_> = SOURCES.iter().map(|s| s.path).collect();
        anyhow::bail!("no memory usage source found, tried: {}", paths.join(", "))
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
    fn test_parse_field_proc_status() {
        let contents = "Name:\tgateway\nVmPeak:\t  20000 kB\nVmRSS:\t   12345 kB\n";
        assert_eq!(parse_field(contents, "VmRSS:"), Some(12345));
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
