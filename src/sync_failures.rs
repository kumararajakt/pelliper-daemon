//! Retry policy for the per-message sync-failure ledger.
//!
//! A header or body fetch that fails is recorded in the `sync_failures` table
//! (storage in `cache.rs`) and retried by the worker's priority loop
//! (`worker/sync_failures.rs`) instead of being logged and forgotten — the
//! prefetch queues live in memory, so an older message is otherwise never
//! retried. This module holds the parts that need no database or network, so
//! they can be unit-tested directly:
//!
//! - [`RETRY_DELAY_SECS`] — the gap between attempts.
//! - [`MAX_SYNC_FAILURE_ATTEMPTS`] — when to stop.
//! - [`retry_decision`] — retry or abandon.
//!
//! Unlike the action ledger there is no rollback and no jitter: the payload is
//! a fetch retry, not a user intent. A message that keeps failing is dropped
//! once the attempt cap is reached; the UI can still load its body on demand.

/// Attempts before a failing message is abandoned.
///
/// The same cap as the action ledger: a permanently broken message is not worth
/// retrying forever, and dropping it frees the background loop.
pub const MAX_SYNC_FAILURE_ATTEMPTS: i32 = 8;

/// Delay between retry attempts, in seconds.
///
/// Flectar uses a flat 60s and there is nothing to gain from exponential growth
/// here: a fetch failure is usually a transient network blip, and the attempt
/// cap already bounds the total work.
pub const RETRY_DELAY_SECS: i64 = 60;

/// What to do with a fetch failure after another failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Try again after [`RETRY_DELAY_SECS`].
    Retry,
    /// Give up and drop the row.
    Drop,
}

/// Decide whether a failed header/body fetch is retried or abandoned.
///
/// `attempts` is the number of attempts **including** the one that just failed;
/// once it reaches [`MAX_SYNC_FAILURE_ATTEMPTS`] the row is dropped so a
/// permanently broken message cannot be retried forever.
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
