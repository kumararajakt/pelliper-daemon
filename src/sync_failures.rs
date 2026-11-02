pub const MAX_SYNC_FAILURE_ATTEMPTS: i32 = 8;

pub const RETRY_DELAY_SECS: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,

    Drop,
}

pub fn retry_decision(attempts: i32) -> RetryDecision {
    if attempts >= MAX_SYNC_FAILURE_ATTEMPTS {
        RetryDecision::Drop
    } else {
        RetryDecision::Retry
    }
}

#[cfg(test)]
#[path = "sync_failures.test.rs"]
mod tests;
