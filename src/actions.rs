pub const MAX_ACTION_ATTEMPTS: i32 = 8;

const BACKOFF_BASE_SECS: u64 = 5;

const BACKOFF_CAP_SECS: u64 = 30 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,

    Drop,
}

pub fn backoff_secs(attempts: i32) -> u64 {
    let exp = attempts.clamp(1, 32) as u32 - 1;
    BACKOFF_BASE_SECS
        .saturating_mul(1u64 << exp.min(20))
        .min(BACKOFF_CAP_SECS)
}

pub fn backoff_secs_jittered(attempts: i32, seed: u64) -> u64 {
    let base = backoff_secs(attempts);
    let span = base / 4;
    if span == 0 {
        base
    } else {
        base + seed % (span + 1)
    }
}

pub fn is_permanent_error(err: &str) -> bool {
    let l = err.to_lowercase();
    [
        "not found",
        "no such",
        "doesn't exist",
        "does not exist",
        "non-existent",
        "nonexistent",
        "unknown mailbox",
        "already exists",
        "already in use",
        "invalid mailbox name",
        "invalid name",
    ]
    .iter()
    .any(|needle| l.contains(needle))
}

pub fn retry_decision(attempts: i32, err: &str) -> RetryDecision {
    if attempts >= MAX_ACTION_ATTEMPTS || is_permanent_error(err) {
        RetryDecision::Drop
    } else {
        RetryDecision::Retry
    }
}

#[cfg(test)]
#[path = "actions.test.rs"]
mod tests;
