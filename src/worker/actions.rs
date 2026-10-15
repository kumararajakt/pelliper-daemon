//! Execution of the durable action ledger.
//!
//! Both paths into a mutation go through here: the interactive handlers enqueue
//! then call [`Worker::process_action`] once for immediate feedback, and the
//! worker's priority loop calls [`Worker::drain_one_action`] to retry whatever is
//! due. A successful action applies its local cache effect and is dropped; a
//! failed one is rescheduled with backoff or dropped when the error is permanent
//! (policy in `crate::actions`).
//!
//! This is what makes flags/move/delete survive being offline: the intent lives
//! in `pending_actions` until the server accepts it, rather than being logged
//! and forgotten (which is all gap #5's gating could do on its own).

use super::Worker;
use crate::actions::{backoff_secs_jittered, retry_decision, RetryDecision};
use crate::types::{ActionKind, MovePayload, PendingAction, SetFlagsPayload};

impl Worker {
    /// Claim and process one due queued action. Returns `true` when one was
    /// claimed (whether it succeeded, was rescheduled, or was dropped), so the
    /// caller loops; `false` when the queue is quiet.
    pub(super) async fn drain_one_action(&mut self) -> bool {
        let now = chrono::Utc::now().timestamp();
        let action = {
            let cache = self.cache.lock().await;
            cache.claim_due_action(self.account.id, now).unwrap_or(None)
        };
        match action {
            Some(action) => {
                self.process_action(action).await;
                true
            }
            None => false,
        }
    }

    /// Execute an action and reconcile the cache.
    ///
    /// On success the local effect is applied and the ledger row removed; on
    /// failure the row is rescheduled with backoff, or dropped when
    /// [`retry_decision`] says the error is permanent or attempts are exhausted.
    pub(super) async fn process_action(&mut self, action: PendingAction) {
        match self.execute_action(&action).await {
            Ok(()) => {
                self.apply_local_effect(&action).await;
                let cache = self.cache.lock().await;
                let _ = cache.complete_action(action.id);
                tracing::info!(
                    "Worker[{}]: applied {} action for {}/{} (attempt {})",
                    self.account.id,
                    action.kind.as_str(),
                    action.folder_path,
                    action.uid,
                    action.attempts + 1
                );
            }
            Err(e) => {
                let attempts = action.attempts + 1;
                match retry_decision(attempts, &e) {
                    RetryDecision::Retry => {
                        let delay = backoff_secs_jittered(attempts, action.id.max(0) as u64);
                        let not_before = chrono::Utc::now().timestamp() + delay as i64;
                        let cache = self.cache.lock().await;
                        let _ = cache.reschedule_action(action.id, not_before, &e);
                        tracing::warn!(
                            "Worker[{}]: {} action for {}/{} failed (attempt {}), retrying in {}s: {}",
                            self.account.id,
                            action.kind.as_str(),
                            action.folder_path,
                            action.uid,
                            attempts,
                            delay,
                            e
                        );
                    }
                    RetryDecision::Drop => {
                        let cache = self.cache.lock().await;
                        let _ = cache.complete_action(action.id);
                        tracing::error!(
                            "Worker[{}]: dropping {} action for {}/{} after {} attempt(s): {}",
                            self.account.id,
                            action.kind.as_str(),
                            action.folder_path,
                            action.uid,
                            attempts,
                            e
                        );
                    }
                }
            }
        }
    }

    /// Run the IMAP operation for an action, refreshing a stale OAuth token and
    /// retrying once — the same recovery the interactive path used before the
    /// ledger existed.
    async fn execute_action(&mut self, action: &PendingAction) -> Result<(), String> {
        self.ensure_session().await?;
        match self.try_action_op(action).await {
            Ok(()) => Ok(()),
            Err(e) if Self::is_auth_error(&e) && self.account.auth_type == "oauth" => {
                tracing::warn!(
                    "Worker[{}]: auth error running queued {}, refreshing token",
                    self.account.id,
                    action.kind.as_str()
                );
                self.invalidate_token();
                self.ensure_session().await?;
                self.try_action_op(action).await
            }
            Err(e) => Err(e),
        }
    }

    /// Dispatch one action to its IMAP operation. No auth retry here; the
    /// caller wraps it.
    async fn try_action_op(&mut self, action: &PendingAction) -> Result<(), String> {
        match action.kind {
            ActionKind::SetFlags => {
                let payload: SetFlagsPayload = serde_json::from_str(&action.payload)
                    .map_err(|e| format!("bad set_flags payload: {e}"))?;
                let (add, remove) = flags_to_add_remove(payload.is_read, payload.is_starred);
                self.do_set_flags(&action.folder_path, action.uid, &add, &remove)
                    .await
            }
            ActionKind::Move => {
                let payload: MovePayload = serde_json::from_str(&action.payload)
                    .map_err(|e| format!("bad move payload: {e}"))?;
                self.do_move(&action.folder_path, action.uid, &payload.dest)
                    .await
            }
            ActionKind::Delete => self.do_expunge(&action.folder_path, action.uid).await,
        }
    }

    /// Apply the action's local cache effect, now that the server accepted it.
    /// Mirrors the inline behaviour the handlers used before the ledger existed.
    async fn apply_local_effect(&mut self, action: &PendingAction) {
        match action.kind {
            ActionKind::SetFlags => {
                let payload: SetFlagsPayload =
                    serde_json::from_str(&action.payload).unwrap_or_default();
                let cache = self.cache.lock().await;
                let _ = cache.update_message_flags(
                    self.account.id,
                    &action.folder_path,
                    action.uid,
                    payload.is_read,
                    payload.is_starred,
                );
                let unread = cache
                    .count_unread(self.account.id, &action.folder_path)
                    .unwrap_or(0);
                let _ = cache.update_folder_unread(self.account.id, &action.folder_path, unread);
            }
            ActionKind::Move => {
                let payload: MovePayload = match serde_json::from_str(&action.payload) {
                    Ok(p) => p,
                    Err(_) => return,
                };
                {
                    let cache = self.cache.lock().await;
                    let _ = cache.move_message(
                        self.account.id,
                        &action.folder_path,
                        action.uid,
                        &payload.dest,
                    );
                    let unread = cache
                        .count_unread(self.account.id, &action.folder_path)
                        .unwrap_or(0);
                    let _ =
                        cache.update_folder_unread(self.account.id, &action.folder_path, unread);
                }
                if payload.refresh_dest {
                    if let Err(e) = self.sync_folder_first_page(&payload.dest).await {
                        tracing::error!(
                            "Worker[{}]: refresh {} after move failed: {}",
                            self.account.id,
                            payload.dest,
                            e
                        );
                    }
                }
            }
            ActionKind::Delete => {
                let cache = self.cache.lock().await;
                let _ = cache.delete_message(self.account.id, &action.folder_path, action.uid);
                let unread = cache
                    .count_unread(self.account.id, &action.folder_path)
                    .unwrap_or(0);
                let _ = cache.update_folder_unread(self.account.id, &action.folder_path, unread);
            }
        }
    }
}

/// Translate the `is_read` / `is_starred` options into IMAP flag add/remove
/// lists. Shared by the interactive handler and the retry drain so a queued flag
/// change replays exactly what the user asked for. `None` touches nothing.
pub(super) fn flags_to_add_remove(
    is_read: Option<bool>,
    is_starred: Option<bool>,
) -> (Vec<&'static str>, Vec<&'static str>) {
    let mut add = Vec::new();
    let mut remove = Vec::new();
    match is_read {
        Some(true) => add.push("\\Seen"),
        Some(false) => remove.push("\\Seen"),
        None => {}
    }
    match is_starred {
        Some(true) => add.push("\\Flagged"),
        Some(false) => remove.push("\\Flagged"),
        None => {}
    }
    (add, remove)
}
