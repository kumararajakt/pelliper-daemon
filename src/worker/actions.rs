use super::Worker;
use crate::actions::{backoff_secs_jittered, retry_decision, RetryDecision};
use crate::types::{
    ActionKind, DaemonSignal, MovePayload, PendingAction, RenameFolderPayload, SetFlagsPayload,
};

impl Worker {
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
            ActionKind::CreateFolder => self.do_create_folder(&action.folder_path).await,
            ActionKind::RenameFolder => {
                let payload: RenameFolderPayload = serde_json::from_str(&action.payload)
                    .map_err(|e| format!("bad rename payload: {e}"))?;
                self.do_rename_folder(&action.folder_path, &payload.new_name)
                    .await
            }
            ActionKind::DeleteFolder => self.do_delete_folder(&action.folder_path).await,
        }
    }

    async fn apply_local_effect(&mut self, action: &PendingAction) {
        match action.kind {
            ActionKind::SetFlags => {
                let payload: SetFlagsPayload =
                    serde_json::from_str(&action.payload).unwrap_or_default();
                let (flags, unread) = {
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
                    let _ =
                        cache.update_folder_unread(self.account.id, &action.folder_path, unread);
                    let flags = cache
                        .message_flags(self.account.id, &action.folder_path, action.uid)
                        .ok()
                        .flatten();
                    (flags, unread)
                };
                if let Some((is_read, is_starred)) = flags {
                    self.emit_signal(DaemonSignal::MessageFlagsChanged {
                        account_id: self.account.id,
                        folder_path: action.folder_path.clone(),
                        uid: action.uid,
                        is_read,
                        is_starred,
                    });
                }
                self.emit_signal(DaemonSignal::FolderUnreadChanged {
                    account_id: self.account.id,
                    folder_path: action.folder_path.clone(),
                    unread_count: unread,
                });
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
                    self.emit_signal(DaemonSignal::FolderUnreadChanged {
                        account_id: self.account.id,
                        folder_path: action.folder_path.clone(),
                        unread_count: unread,
                    });
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
                self.emit_signal(DaemonSignal::FolderUnreadChanged {
                    account_id: self.account.id,
                    folder_path: action.folder_path.clone(),
                    unread_count: unread,
                });
            }
            ActionKind::CreateFolder => {
                let (kind, _) = crate::imap::classify_with_source(&action.folder_path, &[]);
                {
                    let cache = self.cache.lock().await;
                    let _ = cache.upsert_folder(
                        self.account.id,
                        &action.folder_path,
                        0,
                        false,
                        kind.as_str(),
                    );
                }
                self.refresh_folders_from_server().await;
            }
            ActionKind::RenameFolder => {
                let payload: RenameFolderPayload = match serde_json::from_str(&action.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(
                            "Worker[{}]: failed to deserialize rename payload for {}: {e}",
                            self.account.id,
                            action.folder_path
                        );
                        return;
                    }
                };
                {
                    let mut cache = self.cache.lock().await;
                    let _ = cache.rename_folder(
                        self.account.id,
                        &action.folder_path,
                        &payload.new_name,
                    );
                }
                if self.current_folder.as_deref() == Some(&action.folder_path) {
                    self.current_folder = Some(payload.new_name);
                }
                self.refresh_folders_from_server().await;
            }
            ActionKind::DeleteFolder => {
                {
                    let cache = self.cache.lock().await;
                    let _ = cache.delete_folder(self.account.id, &action.folder_path);
                }
                if self.current_folder.as_deref() == Some(&action.folder_path) {
                    self.current_folder = None;
                }
                self.refresh_folders_from_server().await;
            }
        }
    }
}

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
