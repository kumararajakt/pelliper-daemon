use super::Worker;
use crate::sync_failures::{retry_decision, RetryDecision, RETRY_DELAY_SECS};
use crate::types::{SyncFailure, SyncStage};

impl Worker {
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

    pub(super) async fn retry_one_sync_failure(&mut self) -> bool {
        let now = chrono::Utc::now().timestamp();
        let failure = {
            let cache = self.cache.lock().await;

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

    async fn refetch_one_header(&mut self, folder_path: &str, uid: i64) -> Result<(), String> {
        let summaries = self
            .fetch_uid_set(folder_path, &uid.to_string())
            .await
            .map_err(|e| e.to_string())?;

        let failures = self.write_summaries_to_cache(folder_path, &summaries).await;
        match failures.into_iter().next() {
            Some((_, err)) => Err(err),
            None => Ok(()),
        }
    }
}
