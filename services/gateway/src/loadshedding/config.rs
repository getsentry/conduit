use std::time::Duration;

const DEFAULT_PERIOD: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq)]
pub struct Limits {
    pub hard: u64,
    pub soft: u64,
}

#[derive(Debug, PartialEq)]
pub struct MonitorConfig {
    pub limits: Limits,
    /// How often the monitor polls its metric.
    pub period: Duration,
}

impl MonitorConfig {
    /// Returns `None` if `LOADSHEDDING_<METRIC>_HARD_LIMIT` isn't set.
    pub fn from_env(metric: &str) -> Option<Self> {
        let hard_var = format!("LOADSHEDDING_{metric}_HARD_LIMIT");
        let soft_var = format!("LOADSHEDDING_{metric}_SOFT_LIMIT");
        let period_var = format!("LOADSHEDDING_{metric}_PERIOD_MS");
        let limits = resolve_limits(&hard_var, env_u64(&hard_var), &soft_var, env_u64(&soft_var))?;
        let period = resolve_period(&period_var, env_u64(&period_var));
        Some(Self { limits, period })
    }
}

fn env_u64(var: &str) -> Option<u64> {
    let value = std::env::var(var).ok()?;
    match value.parse() {
        Ok(parsed) => Some(parsed),
        Err(_) => {
            tracing::warn!("Ignoring {var}={value}: expected a non-negative integer");
            None
        }
    }
}

/// Without a hard limit the monitor is disabled. The soft limit defaults to,
/// and is capped at, the hard limit.
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
    Some(Limits {
        hard,
        soft: soft.unwrap_or(hard).min(hard),
    })
}

fn resolve_period(period_var: &str, period_ms: Option<u64>) -> Duration {
    match period_ms {
        None => DEFAULT_PERIOD,
        Some(0) => {
            tracing::warn!("{period_var} must be greater than 0; using {DEFAULT_PERIOD:?}");
            DEFAULT_PERIOD
        }
        Some(ms) => Duration::from_millis(ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_resolve_period() {
        assert_eq!(resolve_period("PERIOD", None), DEFAULT_PERIOD);
        assert_eq!(resolve_period("PERIOD", Some(0)), DEFAULT_PERIOD);
        assert_eq!(
            resolve_period("PERIOD", Some(250)),
            Duration::from_millis(250)
        );
    }
}
