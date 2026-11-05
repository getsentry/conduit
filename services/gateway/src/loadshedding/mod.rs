//! Load shedding based on periodically polled metrics.
//!
//! The [`LoadShedder`] holds a set of [`MetricMonitor`]s. They are polled on
//! a dedicated OS thread running its own single-threaded tokio runtime,
//! separate from the runtime serving requests. When
//! [`LoadShedder::should_shed`] is called, every monitor's last value is read
//! (lock-free) and evaluated against its soft/hard limits (see
//! [`should_shed`]). If any monitor decides to shed, the request is shed.
//!
//! Each monitor is configured with these env vars, where `<METRIC>` is
//! `MEMORY` (bytes):
//!
//! - `LOADSHEDDING_<METRIC>_HARD_LIMIT`: enables the monitor
//! - `LOADSHEDDING_<METRIC>_SOFT_LIMIT`: defaults to the hard limit
//! - `LOADSHEDDING_<METRIC>_PERIOD_MS`: poll period, defaults to 1000

mod config;
mod memory;
mod shedder;

use async_trait::async_trait;

pub use memory::MemoryMonitor;
pub use shedder::{LoadShedder, should_shed};

/// A metric that is periodically polled on the load shedding monitor thread.
#[async_trait]
pub trait MetricMonitor: Send + Sync {
    /// Polls the metric value from the system and stores it.
    async fn poll(&self);

    /// Returns the last polled value.
    fn last_value(&self) -> u64;

    /// Name used in logs and metric tags.
    fn name(&self) -> &'static str;
}
