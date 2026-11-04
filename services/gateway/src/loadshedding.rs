//! Load shedding based on periodically polled metrics.
//!
//! The [`LoadShedder`] holds a set of [`MetricMonitor`]s, each polled in the
//! background. When [`LoadShedder::should_shed`] is called, every monitor's
//! last value is evaluated against its soft/hard limits (see [`should_shed`]).
//! If any monitor decides to shed, the request is shed.
//!
//! Monitors are configured with `LOADSHEDDING_<METRIC>_HARD_LIMIT` and
//! `LOADSHEDDING_<METRIC>_SOFT_LIMIT` env vars, where `<METRIC>` is `MEMORY`
//! (bytes) or `CONNECT_TIMEOUT` (decayed count of recent Redis connection
//! acquire timeouts).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker::RedisClient;
use rand::{RngExt, rng};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::{Instant, MissedTickBehavior};

const MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Factor applied to the connect timeout value on every poll. With a steady
/// rate of `r` timeouts per poll interval, the value converges to
/// `r / (1 - CONNECT_TIMEOUT_DECAY)`.
const CONNECT_TIMEOUT_DECAY: f64 = 0.5;

/// Returns `true` if `value` is at or above `hard`. Between `soft` and `hard`,
/// sheds with a probability growing linearly from 0 at `soft` to 1 at `hard`.
/// Below `soft`, returns `false`.
pub fn should_shed(hard: u64, soft: u64, value: u64) -> bool {
    if value >= hard {
        return true;
    }
    if value < soft {
        return false;
    }
    let probability = (value - soft) as f64 / (hard - soft) as f64;
    rng().random::<f64>() < probability
}

/// A metric that is periodically polled in the background.
pub trait MetricMonitor: Send + Sync {
    /// Polls the metric value from the system and stores it.
    fn poll(&self);

    /// Returns the last polled value.
    fn last_value(&self) -> u64;

    /// Name used in logs and metric tags.
    fn name(&self) -> &'static str;
}

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

/// Decaying count of recent Redis connection acquire timeouts.
///
/// On every poll, the timeouts that happened since the previous poll are added
/// to the value after it is decayed by [`CONNECT_TIMEOUT_DECAY`].
pub struct ConnectTimeoutMonitor {
    total: Arc<AtomicU64>,
    last_total: AtomicU64,
    /// `f64` bits of the decayed value.
    value: AtomicU64,
}

impl ConnectTimeoutMonitor {
    pub fn new(total: Arc<AtomicU64>) -> Self {
        Self {
            last_total: AtomicU64::new(total.load(Ordering::Relaxed)),
            total,
            value: AtomicU64::new(0f64.to_bits()),
        }
    }
}

impl MetricMonitor for ConnectTimeoutMonitor {
    fn poll(&self) {
        let total = self.total.load(Ordering::Relaxed);
        let new = total.saturating_sub(self.last_total.swap(total, Ordering::Relaxed));
        let value = f64::from_bits(self.value.load(Ordering::Relaxed));
        let value = value * CONNECT_TIMEOUT_DECAY + new as f64;
        self.value.store(value.to_bits(), Ordering::Relaxed);
    }

    fn last_value(&self) -> u64 {
        f64::from_bits(self.value.load(Ordering::Relaxed)) as u64
    }

    fn name(&self) -> &'static str {
        "connect_timeout"
    }
}

#[derive(Debug, PartialEq)]
struct Limits {
    hard: u64,
    soft: u64,
}

struct LimitedMonitor {
    monitor: Arc<dyn MetricMonitor>,
    limits: Limits,
}

#[derive(Default)]
pub struct LoadShedder {
    monitors: Vec<LimitedMonitor>,
}

impl LoadShedder {
    /// Configures monitors from `LOADSHEDDING_*` env vars. A monitor is only
    /// enabled if its hard limit is set.
    pub fn from_env(redis: &RedisClient) -> anyhow::Result<Self> {
        let mut shedder = Self::default();
        if let Some(limits) = limits_from_env("MEMORY") {
            shedder.add(Arc::new(MemoryMonitor::new()?), limits);
        }
        if let Some(limits) = limits_from_env("CONNECT_TIMEOUT") {
            shedder.add(
                Arc::new(ConnectTimeoutMonitor::new(redis.connect_timeouts())),
                limits,
            );
        }
        Ok(shedder)
    }

    fn add(&mut self, monitor: Arc<dyn MetricMonitor>, limits: Limits) {
        tracing::info!(
            monitor = monitor.name(),
            hard_limit = limits.hard,
            soft_limit = limits.soft,
            "Load shedding monitor enabled"
        );
        self.monitors.push(LimitedMonitor { monitor, limits });
    }

    /// Polls every monitor once, then keeps polling them in the background.
    pub fn start(&self) {
        for m in &self.monitors {
            let monitor = m.monitor.clone();
            monitor.poll();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval_at(
                    Instant::now() + MONITOR_POLL_INTERVAL,
                    MONITOR_POLL_INTERVAL,
                );
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    monitor.poll();
                    metrics::gauge!("loadshedding.monitor.value", "monitor" => monitor.name())
                        .set(monitor.last_value() as f64);
                }
            });
        }
    }

    pub fn should_shed(&self) -> bool {
        self.monitors.iter().any(|m| {
            let shed = should_shed(m.limits.hard, m.limits.soft, m.monitor.last_value());
            if shed {
                metrics::counter!("loadshedding.shed", "monitor" => m.monitor.name()).increment(1);
            }
            shed
        })
    }
}

fn limits_from_env(metric: &str) -> Option<Limits> {
    let hard_var = format!("LOADSHEDDING_{metric}_HARD_LIMIT");
    let soft_var = format!("LOADSHEDDING_{metric}_SOFT_LIMIT");
    resolve_limits(
        &hard_var,
        env_limit(&hard_var),
        &soft_var,
        env_limit(&soft_var),
    )
}

fn env_limit(var: &str) -> Option<u64> {
    let value = std::env::var(var).ok()?;
    match value.parse() {
        Ok(limit) => Some(limit),
        Err(_) => {
            tracing::warn!("Ignoring {var}={value}: expected a non-negative integer");
            None
        }
    }
}

/// Without a hard limit the monitor is disabled. Without a soft limit the soft
/// limit equals the hard limit. A soft limit above the hard limit is clamped.
fn resolve_limits(
    hard_var: &str,
    hard: Option<u64>,
    soft_var: &str,
    soft: Option<u64>,
) -> Option<Limits> {
    let Some(hard) = hard else {
        if soft.is_some() {
            tracing::warn!("{soft_var} is set but {hard_var} is not; monitor disabled");
        }
        return None;
    };
    let soft = soft.unwrap_or(hard);
    if soft > hard {
        tracing::warn!("{soft_var} is above {hard_var}; using {hard_var} as the soft limit");
    }
    Some(Limits {
        hard,
        soft: soft.min(hard),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_shed_below_soft() {
        assert!(!should_shed(100, 50, 0));
        assert!(!should_shed(100, 50, 49));
    }

    #[test]
    fn test_should_shed_at_or_above_hard() {
        assert!(should_shed(100, 50, 100));
        assert!(should_shed(100, 50, 150));
    }

    #[test]
    fn test_should_shed_between_soft_and_hard_is_probabilistic() {
        // Probability is 0 at the soft limit.
        assert!(!should_shed(100, 50, 50));

        let shed_count = (0..1000).filter(|_| should_shed(100, 50, 99)).count();
        assert!(shed_count > 900, "shed {shed_count} out of 1000");

        let shed_count = (0..1000).filter(|_| should_shed(100, 50, 75)).count();
        assert!(
            (350..=650).contains(&shed_count),
            "shed {shed_count} out of 1000"
        );
    }

    #[test]
    fn test_should_shed_soft_equals_hard() {
        assert!(!should_shed(100, 100, 99));
        assert!(should_shed(100, 100, 100));
    }

    #[test]
    fn test_resolve_limits() {
        let resolve = |hard, soft| resolve_limits("HARD", hard, "SOFT", soft);
        assert_eq!(resolve(None, None), None);
        assert_eq!(resolve(None, Some(50)), None);
        assert_eq!(
            resolve(Some(100), None),
            Some(Limits {
                hard: 100,
                soft: 100
            })
        );
        assert_eq!(
            resolve(Some(100), Some(50)),
            Some(Limits {
                hard: 100,
                soft: 50
            })
        );
        assert_eq!(
            resolve(Some(100), Some(150)),
            Some(Limits {
                hard: 100,
                soft: 100
            })
        );
    }

    #[test]
    fn test_connect_timeout_monitor_decays() {
        let total = Arc::new(AtomicU64::new(3));
        let monitor = ConnectTimeoutMonitor::new(total.clone());

        // Timeouts from before the monitor was created are not counted.
        monitor.poll();
        assert_eq!(monitor.last_value(), 0);

        total.fetch_add(10, Ordering::Relaxed);
        monitor.poll();
        assert_eq!(monitor.last_value(), 10);

        monitor.poll();
        assert_eq!(monitor.last_value(), 5);

        total.fetch_add(4, Ordering::Relaxed);
        monitor.poll();
        // 5 * 0.5 + 4
        assert_eq!(monitor.last_value(), 6);
    }

    #[test]
    fn test_memory_monitor_polls() {
        let monitor = MemoryMonitor::new().unwrap();
        monitor.poll();
        assert!(monitor.last_value() > 0);
    }
}
