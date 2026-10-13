//! Folder backfill: index the remaining (older) messages in chunks.

use super::{BackfillJob, Worker};
use crate::imap;
use std::collections::HashSet;

impl Worker {
    // -----------------------------------------------------------------------
    // Background work: backfill
    // -----------------------------------------------------------------------

    pub(super) async fn queue_backfill_for_folder(&mut self, folder_path: &str) {
        // Don't double-queue.
        if self
            .backfill_queue
            .iter()
            .any(|j| j.folder_path == folder_path)
        {
            return;
        }

        let all_uids = match self.fetch_all_uids(folder_path).await {
            Ok(uids) => uids,
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: backfill UID fetch failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                return;
            }
        };

        let (cached_uids, uidvalidity) = {
            let cache = self.cache.lock().await;
            (
                cache
                    .load_cached_uids(self.account.id, folder_path)
                    .unwrap_or_default(),
                cache
                    .folder_uidvalidity(self.account.id, folder_path)
                    .unwrap_or(0),
            )
        };
        let cached_set: HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids
            .into_iter()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            return;
        }

        tracing::info!(
            "Worker[{}]: queued {} messages for backfill in {}",
            self.account.id,
            missing.len(),
            folder_path
        );

        self.backfill_queue.push_back(BackfillJob {
            folder_path: folder_path.to_string(),
            uidvalidity,
            remaining: missing,
        });
    }

    /// Whether the live session still reports the UIDVALIDITY a backfill job
    /// was planned under. A change means the queued UIDs now name different
    /// messages, so the job must be dropped; the next sync re-seeds it. A job
    /// with no recorded UIDVALIDITY (0) cannot be checked and is accepted.
    pub(super) async fn backfill_uidvalidity_ok(
        &mut self,
        folder_path: &str,
        expected: i64,
    ) -> bool {
        if expected == 0 {
            return true;
        }
        if self.ensure_session().await.is_err() {
            return false;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return false,
        };
        match imap::folder_uidvalidity(session, folder_path).await {
            Ok(current) => current == 0 || current == expected,
            Err(e) => {
                tracing::warn!(
                    "Worker[{}]: UIDVALIDITY recheck for {} failed: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                false
            }
        }
    }

    async fn fetch_all_uids(
        &mut self,
        folder_path: &str,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;
        imap::fetch_all_uids(session, folder_path).await
    }

    pub(super) async fn run_backfill_chunk(&mut self, folder_path: &str, chunk: &[i64]) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };

        let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
        let uid_set_str = uid_set.join(",");

        match imap::fetch_summaries_by_uids(session, folder_path, &uid_set_str).await {
            Ok(summaries) => {
                let cache = self.cache.lock().await;
                for msg in &summaries {
                    if let Err(e) = cache.upsert_message(
                        self.account.id,
                        folder_path,
                        msg.uid,
                        msg.is_read,
                        msg.is_starred,
                        msg.has_attachments,
                        &msg.preview,
                    ) {
                        tracing::error!(
                            "Worker[{}]: upsert_message {}/{} failed: {e}",
                            self.account.id,
                            folder_path,
                            msg.uid
                        );
                        continue;
                    }
                    // After upsert_message: the header row is keyed to it.
                    if let Err(e) = cache.save_message_headers(
                        self.account.id,
                        folder_path,
                        msg.uid,
                        &msg.headers,
                    ) {
                        tracing::error!(
                            "Worker[{}]: save_message_headers {}/{} failed: {e}",
                            self.account.id,
                            folder_path,
                            msg.uid
                        );
                    }
                }
                tracing::debug!(
                    "Worker[{}]: backfilled {} messages in {}",
                    self.account.id,
                    summaries.len(),
                    folder_path
                );
            }
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: backfill chunk failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
            }
        }
    }
}
