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
//! `MEMORY` (bytes) or `CONNECT_TIMEOUT` (Redis connection acquire timeouts
//! per second):
//!
//! - `LOADSHEDDING_<METRIC>_HARD_LIMIT`: enables the monitor
//! - `LOADSHEDDING_<METRIC>_SOFT_LIMIT`: defaults to the hard limit
//! - `LOADSHEDDING_<METRIC>_PERIOD_MS`: poll period, defaults to 1000

mod config;
mod connect_timeout;
mod memory;
mod monitor;
mod shedder;

pub use connect_timeout::ConnectTimeoutMonitor;
pub use memory::MemoryMonitor;
pub use monitor::MetricMonitor;
pub use shedder::{LoadShedder, should_shed};
