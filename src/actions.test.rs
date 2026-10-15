//! Policy tests for the durable action ledger: backoff growth, jitter bounds,
//! error classification, and the retry/drop decision.

use super::*;

#[test]
fn backoff_doubles_then_saturates() {
    assert_eq!(backoff_secs(1), BACKOFF_BASE_SECS);
    assert_eq!(backoff_secs(2), BACKOFF_BASE_SECS * 2);
    assert_eq!(backoff_secs(3), BACKOFF_BASE_SECS * 4);
    assert_eq!(backoff_secs(4), BACKOFF_BASE_SECS * 8);
    // Beyond the exponent cap the value is pinned to the ceiling, never wraps.
    assert_eq!(backoff_secs(40), BACKOFF_CAP_SECS);
    assert_eq!(backoff_secs(1000), BACKOFF_CAP_SECS);
    // Zero/negative attempts still yield at least the base.
    assert_eq!(backoff_secs(0), BACKOFF_BASE_SECS);
    assert_eq!(backoff_secs(-5), BACKOFF_BASE_SECS);
}

#[test]
fn jitter_stays_within_a_quarter_of_the_base() {
    let base = backoff_secs(3);
    for seed in 0..200u64 {
        let d = backoff_secs_jittered(3, seed);
        assert!(d >= base, "jitter {d} below base {base}");
        assert!(d <= base + base / 4, "jitter {d} above base + 25% {base}");
    }
}

#[test]
fn permanent_errors_are_recognised_case_insensitively() {
    assert!(is_permanent_error("NO such message"));
    assert!(is_permanent_error("Mailbox does not exist"));
    assert!(is_permanent_error("UNKNOWN MAILBOX"));
    assert!(is_permanent_error("folder doesn't exist"));
}

#[test]
fn transient_errors_are_not_treated_as_permanent() {
    // Auth and transport failures must stay queued: they resolve on reconnect.
    assert!(!is_permanent_error("authentication failed"));
    assert!(!is_permanent_error(
        "AUTHENTICATIONFAILED Invalid credentials"
    ));
    assert!(!is_permanent_error("connection reset by peer"));
    assert!(!is_permanent_error("timed out"));
}

#[test]
fn retry_is_bounded_and_drops_permanent_failures() {
    assert_eq!(retry_decision(1, "connection reset"), RetryDecision::Retry);
    assert_eq!(
        retry_decision(MAX_ACTION_ATTEMPTS - 1, "timed out"),
        RetryDecision::Retry
    );
    assert_eq!(
        retry_decision(MAX_ACTION_ATTEMPTS, "timed out"),
        RetryDecision::Drop
    );
    // A permanent error drops immediately, even on the first failure.
    assert_eq!(retry_decision(1, "no such message"), RetryDecision::Drop);
}
