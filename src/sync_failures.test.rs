use super::*;

#[test]
fn a_fetch_failure_is_retried_until_the_attempt_cap_then_dropped() {
    assert_eq!(retry_decision(1), RetryDecision::Retry);
    assert_eq!(
        retry_decision(MAX_SYNC_FAILURE_ATTEMPTS - 1),
        RetryDecision::Retry
    );
    assert_eq!(
        retry_decision(MAX_SYNC_FAILURE_ATTEMPTS),
        RetryDecision::Drop
    );

    assert_eq!(
        retry_decision(MAX_SYNC_FAILURE_ATTEMPTS + 5),
        RetryDecision::Drop
    );
}
