//! Execution of the per-message sync-failure ledger.
//!
//! A header or body fetch that fails is recorded in `sync_failures` (retry
//! policy in `crate::sync_failures`) and retried here: the worker's priority
//! loop calls [`Worker::retry_one_sync_failure`] once per iteration, after user
//! requests and queued actions, so a transient fetch failure heals in the
//! background instead of waiting for an unrelated re-sync to happen to include
//! the message.
//!
//! Batch header-window failures do not go through here: a failed `UID FETCH`
//! whose cursor is not advanced is retried by the next sync (#2), so only
//! per-message cases are recorded — a body fetch, or a single message whose
//! header row could not be persisted.

use super::Worker;
use crate::sync_failures::{retry_decision, RetryDecision, RETRY_DELAY_SECS};
use crate::types::{SyncFailure, SyncStage};

impl Worker {
    // -----------------------------------------------------------------------
    // Recording
    // -----------------------------------------------------------------------

    /// Record a header/body fetch failure and schedule its retry, or drop the
    /// row once the attempt cap is reached. Shared by every recording site so
    /// the policy lives in one place.
    pub(super) async fn note_sync_failure(
        &self,
        folder_path: &str,
        uid: i64,
        stage: SyncStage,
        err: &str,
    ) {
        let now = chrono::Utc::now().timestamp();
        let cache = self.cache.lock().await;
        match cache.record_sync_failure(
            self.account.id,
            folder_path,
            uid,
            stage,
            now + RETRY_DELAY_SECS,
            err,
        ) {
            Ok(attempts) => match retry_decision(attempts) {
                RetryDecision::Retry => tracing::warn!(
                    "Worker[{}]: {} fetch for {}/{} failed (attempt {}), retrying in {}s: {}",
                    self.account.id,
                    stage.as_str(),
                    folder_path,
                    uid,
                    attempts,
                    RETRY_DELAY_SECS,
                    err
                ),
                RetryDecision::Drop => {
                    let _ = cache.clear_sync_failure(self.account.id, folder_path, uid, stage);
                    tracing::error!(
                        "Worker[{}]: dropping {} fetch for {}/{} after {} attempts: {}",
                        self.account.id,
                        stage.as_str(),
                        folder_path,
                        uid,
                        attempts,
                        err
                    );
                }
            },
            Err(e) => tracing::error!(
                "Worker[{}]: failed to record {} fetch failure for {}/{}: {e}",
                self.account.id,
                stage.as_str(),
                folder_path,
                uid
            ),
        }
    }

    /// Record every message whose header block failed to persist, from
    /// [`Worker::write_summaries_to_cache`].
    pub(super) async fn note_header_write_failures(
        &self,
        folder_path: &str,
        failures: Vec<(i64, String)>,
    ) {
        for (uid, err) in failures {
            self.note_sync_failure(folder_path, uid, SyncStage::Header, &err)
                .await;
        }
    }

    // -----------------------------------------------------------------------
    // Retry
    // -----------------------------------------------------------------------

    /// Claim and retry one due header/body fetch. Returns `true` when one was
    /// claimed (whether it succeeded, was rescheduled, or was dropped), so the
    /// caller keeps looping; `false` when nothing is due.
    pub(super) async fn retry_one_sync_failure(&mut self) -> bool {
        let now = chrono::Utc::now().timestamp();
        let failure = {
            let cache = self.cache.lock().await;
            // Push the retry time out on claim so a retry that errors before it
            // can be rescheduled is not picked up again immediately.
            cache
                .claim_due_sync_failure(self.account.id, now, now + RETRY_DELAY_SECS)
                .unwrap_or(None)
        };
        match failure {
            Some(failure) => {
                self.retry_sync_failure(failure).await;
                true
            }
            None => false,
        }
    }

    /// Retry one recorded failure. On success its row is cleared; on failure it
    /// is rescheduled (or dropped once attempts run out) by the policy.
    async fn retry_sync_failure(&mut self, failure: SyncFailure) {
        let outcome = match failure.stage {
            SyncStage::Body => self
                .fetch_and_store_body(&failure.folder_path, failure.uid)
                .await
                .map(|_| ()),
            SyncStage::Header => {
                self.refetch_one_header(&failure.folder_path, failure.uid)
                    .await
            }
        };
        match outcome {
            Ok(()) => {
                let cache = self.cache.lock().await;
                let _ = cache.clear_sync_failure(
                    self.account.id,
                    &failure.folder_path,
                    failure.uid,
                    failure.stage,
                );
                tracing::info!(
                    "Worker[{}]: recovered {} fetch for {}/{} on attempt {}",
                    self.account.id,
                    failure.stage.as_str(),
                    failure.folder_path,
                    failure.uid,
                    failure.attempts + 1
                );
            }
            Err(e) => {
                self.note_sync_failure(&failure.folder_path, failure.uid, failure.stage, &e)
                    .await;
            }
        }
    }

    /// Fetch and persist a single message's header block, used by the header
    /// retry. Reuses the batched UID fetch with one UID; an empty result means
    /// the message is gone server-side, which counts as recovered.
    async fn refetch_one_header(&mut self, folder_path: &str, uid: i64) -> Result<(), String> {
        let summaries = self
            .fetch_uid_set(folder_path, &uid.to_string())
            .await
            .map_err(|e| e.to_string())?;
        // write_summaries_to_cache returns the messages it could not persist;
        // surface the first so the retry reschedules rather than clearing.
        let failures = self.write_summaries_to_cache(folder_path, &summaries).await;
        match failures.into_iter().next() {
            Some((_, err)) => Err(err),
            None => Ok(()),
        }
    }
}
