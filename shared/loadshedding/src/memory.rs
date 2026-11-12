use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use super::MetricMonitor;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// A cgroup stats file with `<key> <bytes>` lines.
#[derive(Debug)]
struct Source {
    path: PathBuf,
    key: &'static str,
}

impl Source {
    /// Picks the container's memory cgroup stats under `cgroup_root`.
    async fn detect(cgroup_root: &Path) -> Self {
        // `memory.current` only exists in non-root cgroups. If it's missing,
        // `cgroup_root` is the host's root cgroup (the container shares the
        // host's cgroup namespace), and its `memory.stat` would be node-wide.
        let is_own_v2_cgroup = tokio::fs::try_exists(cgroup_root.join("memory.current"))
            .await
            .unwrap_or(false);
        if is_own_v2_cgroup {
            Self {
                path: cgroup_root.join("memory.stat"),
                key: "anon",
            }
        } else {
            Self {
                path: cgroup_root.join("memory/memory.stat"),
                key: "total_rss",
            }
        }
    }

    async fn read(&self) -> Option<u64> {
        let contents = tokio::fs::read_to_string(&self.path).await.ok()?;
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
        Self::with_cgroup_root(Path::new(CGROUP_ROOT)).await
    }

    async fn with_cgroup_root(cgroup_root: &Path) -> anyhow::Result<Self> {
        let source = Source::detect(cgroup_root).await;
        let value = source.read().await.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to read `{}` from {}; memory load shedding requires running \
                 in a container with its own memory cgroup",
                source.key,
                source.path.display()
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
            None => tracing::warn!(
                "Failed to read memory usage from {}",
                self.source.path.display()
            ),
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

    /// A fake cgroup filesystem in a temp dir, removed on drop.
    struct FakeCgroup(PathBuf);

    impl FakeCgroup {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("cgroup-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn write(&self, path: &str, contents: &str) {
            let path = self.0.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
    }

    impl Drop for FakeCgroup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

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

    #[tokio::test]
    async fn test_reads_own_cgroup_v2() {
        let cgroup = FakeCgroup::new();
        cgroup.write("memory.current", "4096\n");
        cgroup.write("memory.stat", "anon 1024\nfile 2048\n");

        let monitor = MemoryMonitor::with_cgroup_root(&cgroup.0).await.unwrap();
        assert_eq!(monitor.last_value(), 1024);

        cgroup.write("memory.stat", "anon 3072\nfile 2048\n");
        monitor.poll().await;
        assert_eq!(monitor.last_value(), 3072);
    }

    #[tokio::test]
    async fn test_refuses_root_cgroup_v2() {
        // The host's root cgroup has memory.stat but no memory.current.
        let cgroup = FakeCgroup::new();
        cgroup.write("memory.stat", "anon 999999999\n");

        let err = MemoryMonitor::with_cgroup_root(&cgroup.0)
            .await
            .err()
            .expect("should refuse the root cgroup");
        assert!(err.to_string().contains("own memory cgroup"), "{err}");
    }

    #[tokio::test]
    async fn test_reads_cgroup_v1() {
        let cgroup = FakeCgroup::new();
        cgroup.write("memory/memory.stat", "cache 2048\ntotal_rss 1024\n");

        let monitor = MemoryMonitor::with_cgroup_root(&cgroup.0).await.unwrap();
        assert_eq!(monitor.last_value(), 1024);
    }
}
