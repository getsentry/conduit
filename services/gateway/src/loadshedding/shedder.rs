use std::sync::Arc;
use std::time::Duration;

use broker::RedisClient;
use rand::{RngExt, rng};
use tokio::time::{Instant, MissedTickBehavior};

use super::config::{Limits, limits_from_env};
use super::{ConnectTimeoutMonitor, MemoryMonitor, MetricMonitor};

const MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(1);

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
}
