//! Coordinated shutdown for services.
//!
//! [`Shutdown`] is a flag that long-lived work, such as open SSE streams,
//! checks to know when to finish up. [`on_signal`] returns a future that
//! resolves on SIGTERM or SIGINT, so the service can trigger the flag.

use std::future::Future;
use std::io;

use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Shutdown(CancellationToken);

impl Shutdown {
    pub fn new() -> Self {
        Self(CancellationToken::new())
    }

    /// Starts shutdown. Every clone sees it.
    pub fn trigger(&self) {
        self.0.cancel();
    }

    /// Returns `true` once shutdown has started.
    pub fn should_exit(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Resolves once shutdown has started.
    pub async fn wait(&self) {
        self.0.cancelled().await;
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

/// Registers SIGTERM and SIGINT handlers and returns a future that resolves
/// when either signal arrives. Handlers are registered when this is called,
/// not when the future is first polled, so a signal sent during startup is
/// not lost to the default action.
pub fn on_signal() -> io::Result<impl Future<Output = ()>> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trigger_is_seen_by_clones() {
        let shutdown = Shutdown::new();
        let clone = shutdown.clone();
        assert!(!clone.should_exit());

        shutdown.trigger();

        assert!(clone.should_exit());
        clone.wait().await;
    }
}
