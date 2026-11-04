#[derive(Debug, PartialEq)]
pub struct Limits {
    pub hard: u64,
    pub soft: u64,
}

/// Reads `LOADSHEDDING_<METRIC>_HARD_LIMIT` and `LOADSHEDDING_<METRIC>_SOFT_LIMIT`.
pub fn limits_from_env(metric: &str) -> Option<Limits> {
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
}
