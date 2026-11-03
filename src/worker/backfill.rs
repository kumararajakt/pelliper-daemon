use super::{BackfillJob, Worker};
use crate::imap;
use crate::types::DaemonSignal;
use std::collections::HashSet;

impl Worker {
    pub(super) async fn queue_backfill_for_folder(&mut self, folder_path: &str) {
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

        let cursor = match cursor {
            Some(c) => c,
            None => {
                let min_uid = {
                    let cache = self.cache.lock().await;
                    cache
                        .min_cached_uid(self.account.id, folder_path)
                        .unwrap_or(None)
                };

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
            return;
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

    pub(super) async fn maybe_queue_next_backfill_folder(&mut self) {
        if !self.backfill_queue.is_empty() {
            return;
        }
        let folders = {
            let cache = self.cache.lock().await;
            cache.load_folders(self.account.id).unwrap_or_default()
        };
        for folder in folders {
            if folder.noselect {
                continue;
            }
            let (last_seen_uid, cursor) = {
                let cache = self.cache.lock().await;
                cache
                    .folder_sync_state(self.account.id, &folder.path)
                    .unwrap_or((0, None))
            };
            let needs_backfill = match cursor {
                Some(c) => c > 0,
                None => last_seen_uid > 0,
            };
            if needs_backfill {
                self.queue_backfill_for_folder(&folder.path).await;
                if !self.backfill_queue.is_empty() {
                    break;
                }
            }
        }
    }

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

        let server_uids: HashSet<i64> = summaries.iter().map(|s| s.uid).collect();
        let to_delete = {
            let cache = self.cache.lock().await;
            let cached = cache
                .cached_uids_in_range(self.account.id, folder_path, lo, hi)
                .unwrap_or_default();
            let pending = cache
                .pending_action_uids(self.account.id, folder_path)
                .unwrap_or_default();
            cached
                .into_iter()
                .filter(|u| !server_uids.contains(u) && !pending.contains(u))
                .collect::<Vec<i64>>()
        };
        if !to_delete.is_empty() {
            let mut cache = self.cache.lock().await;
            if let Ok(removed) = cache.delete_messages(self.account.id, folder_path, &to_delete) {
                if removed > 0 {
                    let unread = cache
                        .count_unread(self.account.id, folder_path)
                        .unwrap_or(0);
                    let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
                    self.emit_signal(DaemonSignal::FolderUnreadChanged {
                        account_id: self.account.id,
                        folder_path: folder_path.to_string(),
                        unread_count: unread,
                    });
                    tracing::info!(
                        "Worker[{}]: backfill expunged {} message(s) in {} (window {uid_set})",
                        self.account.id,
                        removed,
                        folder_path
                    );
                }
            }
        }

        let failures = self.write_summaries_to_cache(folder_path, &summaries).await;
        self.note_header_write_failures(folder_path, failures).await;
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
