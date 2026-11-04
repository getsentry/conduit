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

mod config;
mod connect_timeout;
mod memory;
mod monitor;
mod shedder;

pub use connect_timeout::ConnectTimeoutMonitor;
pub use memory::MemoryMonitor;
pub use monitor::MetricMonitor;
pub use shedder::{LoadShedder, should_shed};
