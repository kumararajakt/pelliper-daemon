//! Worker request dispatch: user-initiated operations (folders, flags,
//! move/delete, load body) driven by the D-Bus layer.

use super::{Worker, WorkerRequest};
use crate::imap;
use crate::types::{ActionKind, MovePayload, SetFlagsPayload, SyncStage};
use futures::StreamExt;

impl Worker {
    pub(super) async fn handle_request(&mut self, req: WorkerRequest) {
        match req {
            WorkerRequest::LoadBody {
                folder_path,
                uid,
                reply,
            } => {
                let html = self.handle_load_body(&folder_path, uid).await;
                let _ = reply.send(html);
            }
            WorkerRequest::SyncAll => {
                self.handle_sync_all().await;
            }
            WorkerRequest::SetIdleFolder { folder_path } => {
                self.on_folder_switch(folder_path).await;
            }
            WorkerRequest::SetFlags {
                folder_path,
                uid,
                is_read,
                is_starred,
            } => {
                self.handle_set_flags(&folder_path, uid, is_read, is_starred)
                    .await;
            }
            WorkerRequest::MoveMessage {
                folder_path,
                uid,
                dest_folder,
            } => {
                self.handle_move_message(&folder_path, uid, dest_folder)
                    .await;
            }
            WorkerRequest::DeleteMessage { folder_path, uid } => {
                self.handle_delete_message(&folder_path, uid).await;
            }
            WorkerRequest::CreateFolder { name } => {
                self.handle_create_folder(&name).await;
            }
            WorkerRequest::RenameFolder { old_name, new_name } => {
                self.handle_rename_folder(&old_name, &new_name).await;
            }
            WorkerRequest::DeleteFolder { name } => {
                self.handle_delete_folder(&name).await;
            }
            WorkerRequest::Shutdown => {
                self.shutdown = true;
                if let Some(mut s) = self.session.take() {
                    let _ = s.logout().await;
                }
            }
        }
    }

    async fn handle_load_body(&mut self, folder_path: &str, uid: i64) -> String {
        // Check cache first.
        {
            let cache = self.cache.lock().await;
            if let Ok(Some(html)) = cache.load_body(self.account.id, folder_path, uid) {
                return html;
            }
        }

        // Fetch from IMAP (retry once with a fresh connection).
        match self.fetch_single_body(folder_path, uid).await {
            Ok((html, has_attachments)) => {
                let cache = self.cache.lock().await;
                if let Err(e) = cache.save_body(self.account.id, folder_path, uid, &html) {
                    tracing::error!(
                        "Worker[{}]: save_body {}/{} failed: {e}",
                        self.account.id,
                        folder_path,
                        uid
                    );
                }
                let _ =
                    cache.update_has_attachment(self.account.id, folder_path, uid, has_attachments);
                html
            }
            Err(e) => {
                // On auth error, invalidate token so reconnect gets a fresh one
                if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" {
                    tracing::warn!(
                        "Worker[{}]: auth error in load_body, refreshing token",
                        self.account.id
                    );
                    self.invalidate_token();
                } else {
                    self.session = None;
                }
                match self.fetch_single_body(folder_path, uid).await {
                    Ok((html, has_attachments)) => {
                        let cache = self.cache.lock().await;
                        if let Err(e) = cache.save_body(self.account.id, folder_path, uid, &html) {
                            tracing::error!(
                                "Worker[{}]: save_body {}/{} failed: {e}",
                                self.account.id,
                                folder_path,
                                uid
                            );
                        }
                        let _ = cache.update_has_attachment(
                            self.account.id,
                            folder_path,
                            uid,
                            has_attachments,
                        );
                        html
                    }
                    Err(e) => {
                        let err = e.to_string();
                        // Both attempts failed: record it so the body is
                        // fetched in the background even if the UI never
                        // re-opens the message.
                        self.note_sync_failure(folder_path, uid, SyncStage::Body, &err)
                            .await;
                        format!("<p>Failed to load message: {err}</p>")
                    }
                }
            }
        }
    }

    async fn handle_sync_all(&mut self) {
        // SyncAll is the UI's startup trigger and the "sync now" switch. Make
        // sure the session is live first: `connect()` refreshes the folder list
        // from the server, so a cache holding stale messages but an empty
        // folder table (empty sidebar) self-heals instead of iterating zero
        // folders and doing nothing. `connect()` only lists folders — it does
        // not fetch first pages — so a fresh cache stays empty until something
        // requests a sync.
        if let Err(e) = self.ensure_session().await {
            tracing::error!("Worker[{}]: sync_all: no session: {}", self.account.id, e);
            return;
        }
        let folders = {
            let cache = self.cache.lock().await;
            cache.load_folders(self.account.id).unwrap_or_default()
        };

        for folder in &folders {
            if folder.noselect {
                continue;
            }
            match self.sync_folder_first_page(&folder.path).await {
                Ok(_summaries) => {}
                Err(e) => {
                    tracing::error!(
                        "Worker[{}]: sync folder {} failed: {}",
                        self.account.id,
                        folder.path,
                        e
                    );
                }
            }
        }
    }

    /// Record a mutation as a durable intent and try it immediately. On success
    /// that is the end of it; on failure the row stays queued and the worker's
    /// drain loop retries it with backoff (`worker/actions.rs`).
    ///
    /// Enqueue-before-attempt is the point: a crash or disconnect between the
    /// two leaves a recoverable row rather than a lost user action.
    async fn enqueue_and_process(
        &mut self,
        folder_path: &str,
        uid: i64,
        kind: ActionKind,
        payload: String,
    ) {
        let now = chrono::Utc::now().timestamp();
        let action = {
            let cache = self.cache.lock().await;
            match cache.enqueue_action(self.account.id, folder_path, uid, kind, &payload, now) {
                Ok(action) => action,
                Err(e) => {
                    tracing::error!(
                        "Worker[{}]: failed to enqueue {} action for {}/{}: {e}",
                        self.account.id,
                        kind.as_str(),
                        folder_path,
                        uid
                    );
                    return;
                }
            }
        };
        self.process_action(action).await;
    }

    /// Apply read/starred flag changes to a message (STORE + local cache),
    /// then refresh the folder's unread count.
    async fn handle_set_flags(
        &mut self,
        folder_path: &str,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    ) {
        let payload = serde_json::to_string(&SetFlagsPayload {
            is_read,
            is_starred,
        })
        .unwrap_or_else(|_| "{}".to_string());
        self.enqueue_and_process(folder_path, uid, ActionKind::SetFlags, payload)
            .await;
    }

    /// Internal helper: select folder and store flags (may fail with auth error).
    pub(super) async fn do_set_flags(
        &mut self,
        folder_path: &str,
        uid: i64,
        add: &[&str],
        remove: &[&str],
    ) -> Result<(), String> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| "no session".to_string())?;
        session
            .select(folder_path)
            .await
            .map_err(|e| e.to_string())?;
        imap::store_flags(session, uid, add, remove)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Move a message to a destination folder (or Trash when None). Uses
    /// UID MOVE where available, falling back to COPY + \Deleted + EXPUNGE.
    async fn handle_move_message(
        &mut self,
        folder_path: &str,
        uid: i64,
        dest_folder: Option<String>,
    ) {
        // Best-effort folder-list refresh: role resolution reads the cache, and
        // an offline account must still get its intent queued for retry, so a
        // failed connect here is not fatal (it will be attempted again by
        // `process_action`).
        let _ = self.ensure_session().await;

        let dest = match dest_folder {
            Some(d) if !d.is_empty() => d,
            _ => match self
                .resolve_role_folder(crate::types::FolderKind::Trash)
                .await
            {
                Some(t) => t,
                None => {
                    tracing::warn!(
                        "Worker[{}]: no Trash folder found for move",
                        self.account.id
                    );
                    return;
                }
            },
        };

        let payload = match serde_json::to_string(&MovePayload {
            dest,
            refresh_dest: true,
        }) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: failed to encode move payload for {}/{}: {e}",
                    self.account.id,
                    folder_path,
                    uid
                );
                return;
            }
        };
        self.enqueue_and_process(folder_path, uid, ActionKind::Move, payload)
            .await;
    }

    pub(super) async fn do_move(
        &mut self,
        folder_path: &str,
        uid: i64,
        dest: &str,
    ) -> Result<(), String> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| "no session".to_string())?;
        session
            .select(folder_path)
            .await
            .map_err(|e| e.to_string())?;
        // `uid_mv` fails (tagged error) when the server lacks RFC 6851; fall
        // back to COPY + \Deleted + EXPUNGE.
        if let Err(e) = session.uid_mv(uid.to_string(), dest).await {
            tracing::info!(
                "Worker[{}]: UID MOVE unsupported ({}), using COPY + EXPUNGE",
                self.account.id,
                e
            );
            session
                .uid_copy(uid.to_string(), dest)
                .await
                .map_err(|e| e.to_string())?;
            imap::store_flags(session, uid, &["\\Deleted"], &[])
                .await
                .map_err(|e| e.to_string())?;
            let expunge = session.expunge().await.map_err(|e| e.to_string())?;
            futures::pin_mut!(expunge);
            while expunge.next().await.is_some() {}
        }
        Ok(())
    }

    /// Internal helper: select folder, mark \Deleted, and EXPUNGE (may fail).
    pub(super) async fn do_expunge(&mut self, folder_path: &str, uid: i64) -> Result<(), String> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| "no session".to_string())?;
        session
            .select(folder_path)
            .await
            .map_err(|e| e.to_string())?;
        imap::store_flags(session, uid, &["\\Deleted"], &[])
            .await
            .map_err(|e| e.to_string())?;
        let expunge = session.expunge().await.map_err(|e| e.to_string())?;
        futures::pin_mut!(expunge);
        while expunge.next().await.is_some() {}
        Ok(())
    }

    /// Delete a message: if already in a Trash folder, permanently remove it
    /// (flag \Deleted + EXPUNGE); otherwise move it to Trash as a soft delete.
    async fn handle_delete_message(&mut self, folder_path: &str, uid: i64) {
        // Best-effort folder-list refresh only: role resolution reads the cache,
        // and an offline account must still get its intent queued for retry
        // (see the durable action ledger).
        let _ = self.ensure_session().await;

        // Consult the classified folder list, not the folder's name. `list_folders`
        // demotes name-matched impostors when the server exposes a real SPECIAL-USE
        // trash, so a user folder that merely happens to be called "Deleted" is a
        // normal folder here and takes the soft delete instead of an EXPUNGE.
        let in_trash = self
            .is_role_folder(folder_path, crate::types::FolderKind::Trash)
            .await;

        if in_trash {
            // Permanently delete: \Deleted + EXPUNGE.
            self.enqueue_and_process(folder_path, uid, ActionKind::Delete, "{}".to_string())
                .await;
        } else {
            // Soft delete: move to Trash. The destination is resolved now and
            // captured in the payload, so a retry after a restart does not
            // depend on the folder list still classifying the same way.
            let trash = match self
                .resolve_role_folder(crate::types::FolderKind::Trash)
                .await
            {
                Some(t) => t,
                None => {
                    tracing::warn!(
                        "Worker[{}]: no Trash folder found for delete",
                        self.account.id
                    );
                    return;
                }
            };
            let payload = match serde_json::to_string(&MovePayload {
                dest: trash,
                refresh_dest: false,
            }) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(
                        "Worker[{}]: failed to encode delete payload for {}/{}: {e}",
                        self.account.id,
                        folder_path,
                        uid
                    );
                    return;
                }
            };
            self.enqueue_and_process(folder_path, uid, ActionKind::Move, payload)
                .await;
        }
    }

    async fn handle_create_folder(&mut self, name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.create(name).await {
            tracing::error!("Worker[{}]: create folder failed: {}", self.account.id, e);
            return;
        }
        // Refresh the folder list so the sidebar picks up the new folder.
        match imap::list_folders(session).await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache.upsert_folder(
                        self.account.id,
                        &folder.path,
                        0,
                        folder.noselect,
                        folder.kind.as_str(),
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }
    }

    async fn handle_rename_folder(&mut self, old_name: &str, new_name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.rename(old_name, new_name).await {
            tracing::error!("Worker[{}]: rename folder failed: {}", self.account.id, e);
            return;
        }
        // Refresh the folder list.
        match imap::list_folders(session).await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache.upsert_folder(
                        self.account.id,
                        &folder.path,
                        0,
                        folder.noselect,
                        folder.kind.as_str(),
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }
    }

    async fn handle_delete_folder(&mut self, name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.delete(name).await {
            tracing::error!("Worker[{}]: delete folder failed: {}", self.account.id, e);
            return;
        }
        // Remove from cache and refresh list.
        {
            let cache = self.cache.lock().await;
            let _ = cache.delete_folder(self.account.id, name);
        }
        if let Some(s) = self.session.as_mut() {
            match imap::list_folders(s).await {
                Ok(folders) => {
                    let cache = self.cache.lock().await;
                    for folder in folders {
                        let _ = cache.upsert_folder(
                            self.account.id,
                            &folder.path,
                            0,
                            folder.noselect,
                            folder.kind.as_str(),
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
                }
            }
        }
    }
}
