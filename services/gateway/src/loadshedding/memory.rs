use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use super::MetricMonitor;

/// Memory usage (in bytes) of the current container: the cgroup's anonymous
/// memory when running in one (Linux), otherwise this process's RSS.
pub struct MemoryMonitor {
    system: Mutex<System>,
    pid: Pid,
    last_value: AtomicU64,
}

impl MemoryMonitor {
    pub fn new() -> Result<Self, sysinfo::Error> {
        Ok(Self {
            system: Mutex::new(System::new()?),
            pid: sysinfo::get_current_pid()?,
            last_value: AtomicU64::new(0),
        })
    }
}

impl MetricMonitor for MemoryMonitor {
    fn poll(&self) {
        let Ok(mut system) = self.system.lock() else {
            return;
        };
        system.refresh_memory();
        let used = match system.cgroup_limits() {
            Some(limits) => limits.rss,
            None => {
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[self.pid]),
                    false,
                    ProcessRefreshKind::nothing().with_memory(),
                );
                system.process(self.pid).map_or(0, |p| p.memory())
            }
        };
        self.last_value.store(used, Ordering::Relaxed);
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
    fn test_memory_monitor_polls() {
        let monitor = MemoryMonitor::new().unwrap();
        monitor.poll();
        assert!(monitor.last_value() > 0);
    }
}
