//! Folder backfill: index the remaining (older) messages by walking down the
//! UID space in persisted chunks instead of re-running `UID SEARCH ALL`.

use super::{BackfillJob, Worker};
use crate::imap;

impl Worker {
    // -----------------------------------------------------------------------
    // Background work: backfill
    // -----------------------------------------------------------------------

    /// Plan history backfill for a folder, backed by the persisted
    /// `backfill_cursor` rather than a fresh `UID SEARCH ALL` diff. The first
    /// time around, the cursor is seeded just above the deepest cached UID so
    /// an existing cache is not re-downloaded (or at the folder's high-water
    /// mark on a cold seed); afterwards the walk simply resumes where it left
    /// off, and a completed folder is never re-planned.
    pub(super) async fn queue_backfill_for_folder(&mut self, folder_path: &str) {
        // Don't double-queue.
        if self
            .backfill_queue
            .iter()
            .any(|j| j.folder_path == folder_path)
        {
            return;
        }

        let (last_seen_uid, cursor) = {
            let cache = self.cache.lock().await;
            cache
                .folder_sync_state(self.account.id, folder_path)
                .unwrap_or((0, None))
        };
        let uidvalidity = {
            let cache = self.cache.lock().await;
            cache
                .folder_uidvalidity(self.account.id, folder_path)
                .unwrap_or(0)
        };

        // Seed the cursor the first time this folder needs history.
        let cursor = match cursor {
            Some(c) => c,
            None => {
                let min_uid = {
                    let cache = self.cache.lock().await;
                    cache
                        .min_cached_uid(self.account.id, folder_path)
                        .unwrap_or(None)
                };
                // Everything at or above the deepest cached UID is already
                // accounted for; only 1..min-1 is unproven.
                let base = match min_uid {
                    Some(m) if m > 1 => m - 1,
                    _ => last_seen_uid,
                };
                let seeded = base.max(0);
                let cache = self.cache.lock().await;
                let _ =
                    cache.set_folder_backfill_cursor(self.account.id, folder_path, Some(seeded));
                seeded
            }
        };

        if cursor <= 0 {
            return; // nothing left to download (empty or already complete)
        }

        tracing::info!(
            "Worker[{}]: backfilling history below {} in {}",
            self.account.id,
            cursor,
            folder_path
        );

        self.backfill_queue.push_back(BackfillJob {
            folder_path: folder_path.to_string(),
            uidvalidity,
            cursor,
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

    /// Fetch one history window `[lo, hi]` (a UID range, so sparse/expunged
    /// UIDs are simply absent) and persist the progress. Returns `false` when
    /// the fetch failed — the cursor is not advanced in that case, so the same
    /// window is retried next cycle instead of being skipped.
    pub(super) async fn run_backfill_chunk(&mut self, folder_path: &str, lo: i64, hi: i64) -> bool {
        let uid_set = format!("{lo}:{hi}");
        let summaries = match self.fetch_uid_set(folder_path, &uid_set).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: backfill window {uid_set} failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                return false;
            }
        };

        self.write_summaries_to_cache(folder_path, &summaries).await;
        let next = (lo - 1).max(0);
        {
            let cache = self.cache.lock().await;
            let _ = cache.set_folder_backfill_cursor(self.account.id, folder_path, Some(next));
        }
        tracing::debug!(
            "Worker[{}]: backfilled window {uid_set} ({} msg) in {}",
            self.account.id,
            summaries.len(),
            folder_path
        );
        true
    }
}
