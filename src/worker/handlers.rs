//! Worker request dispatch: user-initiated operations (folders, flags,
//! move/delete, load body) driven by the D-Bus layer.

use super::{Worker, WorkerRequest};
use crate::imap;
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
                let _ = cache.save_body(self.account.id, folder_path, uid, &html);
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
                        let _ = cache.save_body(self.account.id, folder_path, uid, &html);
                        let _ = cache.update_has_attachment(
                            self.account.id,
                            folder_path,
                            uid,
                            has_attachments,
                        );
                        html
                    }
                    Err(e) => format!("<p>Failed to load message: {}</p>", e),
                }
            }
        }
    }

    async fn handle_sync_all(&mut self) {
        // SyncAll is the UI's startup trigger and the "sync now" switch. Make
        // sure the session is live first: `connect()` refreshes the folder list
        // from the server, so a cache holding stale messages but an empty
        // folder table (empty sidebar) self-heals instead of iterating zero
        // folders and doing nothing.
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

    /// Apply read/starred flag changes to a message (STORE + local cache),
    /// then refresh the folder's unread count.
    async fn handle_set_flags(
        &mut self,
        folder_path: &str,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    ) {
        let mut add: Vec<&str> = Vec::new();
        let mut remove: Vec<&str> = Vec::new();
        if let Some(read) = is_read {
            if read {
                add.push("\\Seen");
            } else {
                remove.push("\\Seen");
            }
        }
        if let Some(starred) = is_starred {
            if starred {
                add.push("\\Flagged");
            } else {
                remove.push("\\Flagged");
            }
        }

        if self.ensure_session().await.is_err() {
            return;
        }
        // Try the operation; on auth error, invalidate token and retry once
        let result = self.do_set_flags(folder_path, uid, &add, &remove).await;
        if result.is_err() {
            let e = result.unwrap_err();
            if Self::is_auth_error(&e) && self.account.auth_type == "oauth" {
                tracing::warn!(
                    "Worker[{}]: auth error in set_flags, refreshing token",
                    self.account.id
                );
                self.invalidate_token();
                if self.ensure_session().await.is_err() {
                    return;
                }
                let _ = self.do_set_flags(folder_path, uid, &add, &remove).await;
            }
        }

        {
            let cache = self.cache.lock().await;
            let _ =
                cache.update_message_flags(self.account.id, folder_path, uid, is_read, is_starred);
            let unread = cache
                .count_unread(self.account.id, folder_path)
                .unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }
    }

    /// Internal helper: select folder and store flags (may fail with auth error).
    async fn do_set_flags(
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
        if self.ensure_session().await.is_err() {
            return;
        }

        let dest = match dest_folder {
            Some(d) if !d.is_empty() => d,
            _ => {
                let session = self.session.as_mut();
                match session {
                    Some(s) => {
                        match imap::find_folder_by_kind(s, crate::types::FolderKind::Trash).await {
                            Ok(Some(t)) => t,
                            Ok(None) => {
                                tracing::warn!(
                                    "Worker[{}]: no Trash folder found for move",
                                    self.account.id
                                );
                                return;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Worker[{}]: trash lookup failed: {}",
                                    self.account.id,
                                    e
                                );
                                return;
                            }
                        }
                    }
                    None => return,
                }
            }
        };

        // Try the move; on auth error, invalidate token and retry once
        let move_result = self.do_move(folder_path, uid, &dest).await;
        if let Err(ref e) = move_result {
            if Self::is_auth_error(e) && self.account.auth_type == "oauth" {
                tracing::warn!(
                    "Worker[{}]: auth error in move, refreshing token",
                    self.account.id
                );
                self.invalidate_token();
                if self.ensure_session().await.is_ok() {
                    let _ = self.do_move(folder_path, uid, &dest).await;
                }
            }
        }

        {
            let cache = self.cache.lock().await;
            let _ = cache.move_message(self.account.id, folder_path, uid, &dest);
            let unread = cache
                .count_unread(self.account.id, folder_path)
                .unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }

        // Make sure the destination shows the moved message.
        if let Err(e) = self.sync_folder_first_page(&dest).await {
            tracing::error!(
                "Worker[{}]: refresh {} after move failed: {}",
                self.account.id,
                dest,
                e
            );
        }
        return;
    }

    async fn do_move(&mut self, folder_path: &str, uid: i64, dest: &str) -> Result<(), String> {
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

    /// Delete a message: if already in a Trash folder, permanently remove it
    /// (flag \Deleted + EXPUNGE); otherwise move it to Trash as a soft delete.
    async fn handle_delete_message(&mut self, folder_path: &str, uid: i64) {
        if self.ensure_session().await.is_err() {
            return;
        }

        let in_trash =
            crate::types::FolderKind::from_path(folder_path) == crate::types::FolderKind::Trash;

        if in_trash {
            // Permanently delete: \Deleted + EXPUNGE
            let session = self.session.as_mut();
            if let Some(s) = session {
                let _ = s.select(folder_path).await;
                let _ = imap::store_flags(s, uid, &["\\Deleted"], &[]).await;
                if let Ok(expunge) = s.expunge().await {
                    futures::pin_mut!(expunge);
                    while expunge.next().await.is_some() {}
                }
            }
            let cache = self.cache.lock().await;
            let _ = cache.delete_message(self.account.id, folder_path, uid);
            let unread = cache
                .count_unread(self.account.id, folder_path)
                .unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        } else {
            // Soft delete: move to Trash
            let session = self.session.as_mut();
            let trash = match session {
                Some(s) => {
                    match imap::find_folder_by_kind(s, crate::types::FolderKind::Trash).await {
                        Ok(Some(t)) => t,
                        Ok(None) => {
                            tracing::warn!(
                                "Worker[{}]: no Trash folder found for delete",
                                self.account.id
                            );
                            return;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Worker[{}]: trash lookup failed: {}",
                                self.account.id,
                                e
                            );
                            return;
                        }
                    }
                }
                None => return,
            };
            if self.do_move(folder_path, uid, &trash).await.is_ok() {
                let cache = self.cache.lock().await;
                let _ = cache.move_message(self.account.id, folder_path, uid, &trash);
                let unread = cache
                    .count_unread(self.account.id, folder_path)
                    .unwrap_or(0);
                let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
            }
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
