//! Durable action-ledger policy.
//!
//! Non-send mutations (flags/move/delete) are written to the `pending_actions`
//! table before the server is touched, then attempted. A failed attempt is
//! rescheduled with backoff; a permanently-rejected one is dropped. This module
//! holds the parts of that policy that need no database or network, so they can
//! be unit-tested directly:
//!
//! - [`backoff_secs`] / [`backoff_secs_jittered`] — retry scheduling.
//! - [`is_permanent_error`] — errors retrying cannot fix.
//! - [`retry_decision`] — retry or give up.
//!
//! Storage lives in `cache.rs`; IMAP execution lives in `worker/actions.rs`.

/// Attempts before a queued mutation is abandoned.
///
/// Lower than the outbox cap: a stuck flag/move is cheaper to give up on than an
/// unsent message, and the server state remains authoritative (the next sync
/// re-reads whatever actually happened).
pub const MAX_ACTION_ATTEMPTS: i32 = 8;

/// Base of the exponential retry backoff, in seconds.
const BACKOFF_BASE_SECS: u64 = 5;

/// Ceiling for the exponential retry backoff, in seconds.
const BACKOFF_CAP_SECS: u64 = 30 * 60;

/// What to do with an action after another failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Try again later.
    Retry,
    /// Give up and drop the action.
    Drop,
}

/// Deterministic exponential backoff, in seconds, for a given attempt count.
///
/// `attempts` is 1-based: the delay after the first failure is
/// [`BACKOFF_BASE_SECS`], doubling per attempt and saturating at
/// [`BACKOFF_CAP_SECS`].
pub fn backoff_secs(attempts: i32) -> u64 {
    let exp = attempts.clamp(1, 32) as u32 - 1;
    BACKOFF_BASE_SECS
        .saturating_mul(1u64 << exp.min(20))
        .min(BACKOFF_CAP_SECS)
}

/// [`backoff_secs`] plus up to 25% jitter derived from `seed` (typically the
/// action id), so a backlog of actions does not retry in lockstep.
pub fn backoff_secs_jittered(attempts: i32, seed: u64) -> u64 {
    let base = backoff_secs(attempts);
    let span = base / 4;
    if span == 0 {
        base
    } else {
        base + seed % (span + 1)
    }
}

/// True when the server rejected the mutation in a way that retrying cannot fix
/// — the message or folder is gone, so the intent is stale.
///
/// Auth and transport failures are deliberately *not* permanent: they resolve on
/// reconnect, which is the whole point of keeping the action queued.
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
    ]
    .iter()
    .any(|needle| l.contains(needle))
}

/// Decide whether a failed action is retried or dropped.
///
/// `attempts` is the number of attempts **including** the one that just failed.
/// A permanently-rejected action, or one that has reached
/// [`MAX_ACTION_ATTEMPTS`], is dropped; everything else is retried.
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
