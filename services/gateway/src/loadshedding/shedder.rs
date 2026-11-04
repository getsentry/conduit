use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures_util::future::join_all;
use rand::{RngExt, rng};
use tokio::sync::oneshot;
use tokio::time::{Instant, MissedTickBehavior};

use super::config::{MonitorConfig, monitor_config_from_env};
use super::{MemoryMonitor, MetricMonitor};

const MONITOR_THREAD_NAME: &str = "loadshedding-monitors";

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

struct ConfiguredMonitor {
    monitor: Arc<dyn MetricMonitor>,
    config: MonitorConfig,
}

#[derive(Default)]
pub struct LoadShedder {
    monitors: Vec<ConfiguredMonitor>,
}

impl LoadShedder {
    /// Configures monitors from `LOADSHEDDING_*` env vars. A monitor is only
    /// enabled if its hard limit is set.
    pub async fn from_env() -> anyhow::Result<Self> {
        let mut shedder = Self::default();
        if let Some(config) = monitor_config_from_env("MEMORY") {
            shedder.add(Arc::new(MemoryMonitor::new().await?), config);
        }
        Ok(shedder)
    }

    fn add(&mut self, monitor: Arc<dyn MetricMonitor>, config: MonitorConfig) {
        tracing::info!(
            monitor = monitor.name(),
            hard_limit = config.limits.hard,
            soft_limit = config.limits.soft,
            period_ms = config.period.as_millis() as u64,
            "Load shedding monitor enabled"
        );
        self.monitors.push(ConfiguredMonitor { monitor, config });
    }

    /// Starts polling monitors on a dedicated thread with its own runtime, so
    /// monitors never compete with request handling. Returns once every
    /// monitor has been polled once.
    pub async fn start(&self) -> anyhow::Result<()> {
        if self.monitors.is_empty() {
            return Ok(());
        }
        let monitors: Vec<_> = self
            .monitors
            .iter()
            .map(|m| (m.monitor.clone(), m.config.period))
            .collect();
        let (ready_tx, ready_rx) = oneshot::channel();

        thread::Builder::new()
            .name(MONITOR_THREAD_NAME.to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                runtime.block_on(async move {
                    for (monitor, _) in &monitors {
                        monitor.poll().await;
                    }
                    let _ = ready_tx.send(Ok(()));
                    join_all(
                        monitors
                            .into_iter()
                            .map(|(monitor, period)| run_monitor(monitor, period)),
                    )
                    .await;
                });
            })?;

        ready_rx.await??;
        Ok(())
    }

    pub fn should_shed(&self) -> bool {
        self.monitors.iter().any(|m| {
            let limits = &m.config.limits;
            let shed = should_shed(limits.hard, limits.soft, m.monitor.last_value());
            if shed {
                metrics::counter!("loadshedding.shed", "monitor" => m.monitor.name()).increment(1);
            }
            shed
        })
    }
}

async fn run_monitor(monitor: Arc<dyn MetricMonitor>, period: Duration) {
    let mut interval = tokio::time::interval_at(Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        monitor.poll().await;
        metrics::gauge!("loadshedding.monitor.value", "monitor" => monitor.name())
            .set(monitor.last_value() as f64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use async_trait::async_trait;

    use super::super::config::Limits;
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

    #[derive(Default)]
    struct CountingMonitor {
        polls: AtomicU64,
        threads: Mutex<Vec<Option<String>>>,
    }

    #[async_trait]
    impl MetricMonitor for CountingMonitor {
        async fn poll(&self) {
            self.polls.fetch_add(1, Ordering::Relaxed);
            self.threads
                .lock()
                .unwrap()
                .push(thread::current().name().map(String::from));
        }

        fn last_value(&self) -> u64 {
            self.polls.load(Ordering::Relaxed)
        }

        fn name(&self) -> &'static str {
            "counting"
        }
    }

    #[tokio::test]
    async fn test_start_polls_on_dedicated_thread_with_configured_period() {
        let monitor = Arc::new(CountingMonitor::default());
        let mut shedder = LoadShedder::default();
        shedder.add(
            monitor.clone(),
            MonitorConfig {
                limits: Limits { hard: 10, soft: 5 },
                period: Duration::from_millis(50),
            },
        );

        shedder.start().await.unwrap();
        assert_eq!(monitor.last_value(), 1, "polled once before start returns");

        tokio::time::sleep(Duration::from_millis(275)).await;
        assert!(monitor.last_value() >= 4, "polls: {}", monitor.last_value());

        let threads = monitor.threads.lock().unwrap();
        assert!(
            threads
                .iter()
                .all(|name| name.as_deref() == Some(MONITOR_THREAD_NAME)),
            "polled on threads: {threads:?}"
        );
    }
}
