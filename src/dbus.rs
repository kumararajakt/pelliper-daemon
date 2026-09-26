//! D-Bus interface (`app.pelliper.Daemon`) implementation.

use crate::imap;
use crate::outbox::send_with_auth;
use crate::spawn_worker;
use crate::types::{Account, EmailAttachment};
use crate::worker;
use crate::Daemon;
use zbus::interface;

impl Daemon {
    fn get_worker(&self, account_id: i64) -> Option<worker::WorkerHandle> {
        self.workers.lock().unwrap().get(&account_id).cloned()
    }

    /// Ensure a worker exists for the given account (used for newly-added
    /// accounts at runtime).
    fn ensure_worker(&self, account: &Account) {
        let existing = self.workers.lock().unwrap().contains_key(&account.id);
        if existing {
            return;
        }
        let handle = spawn_worker(
            account,
            self.cache.clone(),
            self.secrets.clone(),
            self.attachments_dir.clone(),
        );
        self.workers.lock().unwrap().insert(account.id, handle);
    }
}

#[interface(name = "app.pelliper.Daemon")]
impl Daemon {
    /// Check if any accounts exist in the database.
    async fn has_accounts(&self) -> bool {
        let cache = self.cache.lock().await;
        match cache.account_count() {
            Ok(count) => count > 0,
            Err(e) => {
                tracing::error!("Failed to check accounts: {}", e);
                false
            }
        }
    }

    /// Add an account, test IMAP connection, and save to database.
    /// Returns true on success, false on failure.
    async fn add_account(
        &self,
        email: &str,
        display_name: &str,
        imap_host: &str,
        imap_port: i32,
        smtp_host: &str,
        smtp_port: i32,
        auth_type: &str,
        auth_token: &str,
        refresh_token: &str,
        provider: &str,
    ) -> bool {
        tracing::info!("add_account: {} ({} {})", email, auth_type, provider);

        // Check if account already exists
        {
            let cache = self.cache.lock().await;
            if let Ok(exists) = cache.account_exists(email) {
                if exists {
                    tracing::warn!("Account already exists: {}", email);
                    return false;
                }
            }
        }

        // Test IMAP connection
        let imap_ok = if auth_type == "oauth" {
            imap::test_connection_oauth(imap_host, imap_port as u16, email, auth_token).await
        } else {
            imap::test_connection_password(imap_host, imap_port as u16, email, auth_token).await
        };

        if !imap_ok {
            tracing::warn!(
                "IMAP connection test failed for {}, saving account anyway",
                email
            );
        }

        // Save account to database
        let account = Account {
            id: 0,
            email: email.to_string(),
            display_name: Some(display_name.to_string()),
            provider: if provider.is_empty() {
                None
            } else {
                Some(provider.to_string())
            },
            auth_type: auth_type.to_string(),
            imap_host: Some(imap_host.to_string()),
            imap_port: Some(imap_port as i64),
            imap_security: Some("tls".to_string()),
            smtp_host: Some(smtp_host.to_string()),
            smtp_port: Some(smtp_port as i64),
            smtp_security: Some("starttls".to_string()),
            username: Some(email.to_string()),
            refresh_token: if refresh_token.is_empty() {
                None
            } else {
                Some(refresh_token.to_string())
            },
            enabled: true,
        };

        let saved_id = {
            let mut cache = self.cache.lock().await;
            match cache.insert_account(&account) {
                Ok(id) => {
                    tracing::info!("Account saved with id {}: {}", id, email);
                    Some(id)
                }
                Err(e) => {
                    tracing::error!("Failed to save account {}: {}", email, e);
                    None
                }
            }
        };

        if let Some(id) = saved_id {
            let saved_account = {
                let cache = self.cache.lock().await;
                cache
                    .load_accounts()
                    .ok()
                    .and_then(|accounts| accounts.into_iter().find(|a| a.id == id))
            };
            if let Some(a) = saved_account {
                // Start a background worker for the new account. It bootstraps
                // (cold cache) and handles sync + IDLE.
                self.ensure_worker(&a);
            }
        }

        saved_id.is_some()
    }

    /// Remove an account and all its data (folders, messages, bodies, attachments).
    /// Returns true on success.
    async fn remove_account(&self, account_id: i64) -> bool {
        tracing::info!("D-Bus: remove_account id={}", account_id);

        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::Shutdown);
        }
        self.workers.lock().unwrap().remove(&account_id);

        let cache = self.cache.lock().await;
        match cache.remove_account(account_id) {
            Ok(()) => {
                tracing::info!("Account {} removed", account_id);
                true
            }
            Err(e) => {
                tracing::error!("Failed to remove account {}: {}", account_id, e);
                false
            }
        }
    }

    /// Trigger a full sync of all enabled accounts (route to each worker).
    async fn sync_all(&self) -> bool {
        tracing::info!("D-Bus: sync_all requested");
        let workers: Vec<worker::WorkerHandle> =
            self.workers.lock().unwrap().values().cloned().collect();
        for handle in workers {
            handle.send(worker::WorkerRequest::SyncAll);
        }
        true
    }

    /// Trigger a sync of a specific account by email.
    async fn sync_account(&self, email: &str) -> bool {
        tracing::info!("D-Bus: sync_account requested for {}", email);
        let account = {
            let cache = self.cache.lock().await;
            match cache.load_accounts() {
                Ok(accounts) => accounts.into_iter().find(|a| a.email == email),
                Err(e) => {
                    tracing::error!("Failed to load accounts: {}", e);
                    return false;
                }
            }
        };

        match account {
            Some(account) => {
                self.ensure_worker(&account);
                if let Some(handle) = self.get_worker(account.id) {
                    handle.send(worker::WorkerRequest::SyncAll);
                    true
                } else {
                    false
                }
            }
            None => {
                tracing::error!("Account not found: {}", email);
                false
            }
        }
    }

    /// Get folders for an account. Returns JSON array.
    async fn get_folders(&self, account_id: i64) -> String {
        let cache = self.cache.lock().await;
        match cache.load_folders(account_id) {
            Ok(folders) => serde_json::to_string(&folders).unwrap_or_else(|_| "[]".to_string()),
            Err(e) => {
                tracing::error!("Failed to load folders: {}", e);
                "[]".to_string()
            }
        }
    }

    /// Get messages for a folder. Returns JSON array.
    async fn get_messages(
        &self,
        account_id: i64,
        folder_path: &str,
        offset: i64,
        limit: i64,
    ) -> String {
        let cache = self.cache.lock().await;
        match cache.load_messages(account_id, folder_path, offset, limit) {
            Ok(messages) => serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string()),
            Err(e) => {
                tracing::error!("Failed to load messages: {}", e);
                "[]".to_string()
            }
        }
    }

    /// Get total message count for a folder.
    async fn count_messages(&self, account_id: i64, folder_path: &str) -> i64 {
        let cache = self.cache.lock().await;
        cache.count_messages(account_id, folder_path).unwrap_or(0)
    }

    /// Tell the account's worker to watch a specific folder for real-time
    /// updates (IDLE) and sync its first page.
    async fn set_idle_folder(&self, account_id: i64, folder_path: &str) {
        tracing::info!(
            "D-Bus: set_idle_folder account={} folder={}",
            account_id,
            folder_path
        );
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::SetIdleFolder {
                folder_path: folder_path.to_string(),
            });
        }
    }

    /// Search the address book for autocomplete. Returns JSON array of
    /// `{email, name}` objects.
    async fn search_addresses(&self, query: &str) -> String {
        let cache = self.cache.lock().await;
        match cache.search_addresses(query, 10) {
            Ok(results) => {
                let items: Vec<serde_json::Value> = results
                    .into_iter()
                    .map(|(email, name)| serde_json::json!({ "email": email, "name": name }))
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            Err(_) => "[]".to_string(),
        }
    }

    /// Check if an email address is a known sender (exists in address book).
    async fn is_known_address(&self, email: &str) -> bool {
        let cache = self.cache.lock().await;
        cache.is_known_address(email).unwrap_or(false)
    }

    /// Block a sender (remote content will be suppressed).
    async fn block_sender(&self, sender_email: &str) {
        let cache = self.cache.lock().await;
        let _ = cache.set_sender_policy(sender_email, true, false);
    }

    /// Unblock a sender.
    async fn unblock_sender(&self, sender_email: &str) {
        let cache = self.cache.lock().await;
        let _ = cache.remove_sender_policy(sender_email);
    }

    /// Check if a sender is blocked.
    async fn is_sender_blocked(&self, sender_email: &str) -> bool {
        let cache = self.cache.lock().await;
        cache.is_sender_blocked(sender_email).unwrap_or(false)
    }

    /// Load all sender block/allow policies. Returns JSON array.
    async fn load_sender_policies(&self) -> String {
        let cache = self.cache.lock().await;
        match cache.load_sender_policies() {
            Ok(policies) => {
                let items: Vec<serde_json::Value> = policies
                    .into_iter()
                    .map(|(email, blocked, allowed)| {
                        serde_json::json!({ "email": email, "blocked": blocked, "allowed": allowed })
                    })
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            Err(_) => "[]".to_string(),
        }
    }

    /// Mark a message read/unread.
    async fn set_message_read(&self, account_id: i64, folder_path: &str, uid: i64, read: bool) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::SetFlags {
                folder_path: folder_path.to_string(),
                uid,
                is_read: Some(read),
                is_starred: None,
            });
        }
    }

    /// Star/unstar a message.
    async fn set_message_starred(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        starred: bool,
    ) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::SetFlags {
                folder_path: folder_path.to_string(),
                uid,
                is_read: None,
                is_starred: Some(starred),
            });
        }
    }

    /// Move a message to a folder, or to Trash when `dest_folder` is empty.
    async fn move_message(&self, account_id: i64, folder_path: &str, uid: i64, dest_folder: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::MoveMessage {
                folder_path: folder_path.to_string(),
                uid,
                dest_folder: if dest_folder.is_empty() {
                    None
                } else {
                    Some(dest_folder.to_string())
                },
            });
        }
    }

    /// Delete a message: move to Trash if not already there, otherwise permanently delete.
    async fn delete_message(&self, account_id: i64, folder_path: &str, uid: i64) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::DeleteMessage {
                folder_path: folder_path.to_string(),
                uid,
            });
        }
    }

    /// Create a new IMAP folder.
    async fn create_folder(&self, account_id: i64, name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::CreateFolder {
                name: name.to_string(),
            });
        }
    }

    /// Rename an IMAP folder.
    async fn rename_folder(&self, account_id: i64, old_name: &str, new_name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::RenameFolder {
                old_name: old_name.to_string(),
                new_name: new_name.to_string(),
            });
        }
    }

    /// Delete an IMAP folder.
    async fn delete_folder(&self, account_id: i64, name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::DeleteFolder {
                name: name.to_string(),
            });
        }
    }

    /// Load the body of a message. Returns HTML.
    /// Serves from cache when available; otherwise the worker fetches + caches
    /// and a second call returns the cached body.
    async fn load_body(&self, account_id: i64, folder_path: &str, uid: i64) -> String {
        // Check cache first
        {
            let cache = self.cache.lock().await;
            if let Ok(Some(html)) = cache.load_body(account_id, folder_path, uid) {
                tracing::debug!("Body cache hit for uid={}", uid);
                return html;
            }
        }

        tracing::info!(
            "Fetching body for account={} folder={} uid={}",
            account_id,
            folder_path,
            uid
        );

        // Delegate to the worker; await its response.
        if let Some(handle) = self.get_worker(account_id) {
            let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
            handle.send(worker::WorkerRequest::LoadBody {
                folder_path: folder_path.to_string(),
                uid,
                reply: reply_tx,
            });
            if let Some(html) = reply_rx.recv().await {
                return html;
            }
        }

        "<p>No worker available for account</p>".to_string()
    }

    /// List downloaded attachments for a message.
    ///
    /// Serves from the attachments table; when nothing is stored yet but the
    /// message was flagged as having attachments, asks the worker to fetch and
    /// parse the body (which persists them) before re-reading the table.
    /// Returns JSON array of `{name, file_path, content_type, size}`.
    async fn list_attachments(&self, account_id: i64, folder_path: &str, uid: i64) -> String {
        let has_attachments = {
            let cache = self.cache.lock().await;
            match cache.load_attachments(account_id, folder_path, uid) {
                Ok(list) if !list.is_empty() => {
                    return serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string());
                }
                Ok(_) => cache
                    .message_has_attachments(account_id, folder_path, uid)
                    .unwrap_or(false),
                Err(e) => {
                    tracing::error!(
                        "Failed to load attachments for account={} folder={} uid={}: {}",
                        account_id,
                        folder_path,
                        uid,
                        e
                    );
                    return "[]".to_string();
                }
            }
        };

        // No cached attachments: if the message claims to have them, the body
        // (and thus the attachment list) hasn't been fetched yet — pull it now.
        if has_attachments {
            if let Some(handle) = self.get_worker(account_id) {
                let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
                handle.send(worker::WorkerRequest::LoadBody {
                    folder_path: folder_path.to_string(),
                    uid,
                    reply: reply_tx,
                });
                let _ = reply_rx.recv().await;
            }
            let cache = self.cache.lock().await;
            match cache.load_attachments(account_id, folder_path, uid) {
                Ok(list) => serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string()),
                Err(_) => "[]".to_string(),
            }
        } else {
            "[]".to_string()
        }
    }

    /// Send an email via the account's SMTP relay. Returns an error string on
    /// failure (empty string on success).
    async fn send_email(
        &self,
        account_id: i64,
        to: &str,
        cc: &str,
        subject: &str,
        body: &str,
        reply_folder_path: &str,
        reply_uid: i64,
        attachment_paths: &str,
    ) -> String {
        let account = {
            let cache = self.cache.lock().await;
            match cache.load_accounts() {
                Ok(accounts) => accounts.into_iter().find(|a| a.id == account_id),
                Err(e) => {
                    let msg = format!("Failed to load accounts: {}", e);
                    tracing::error!("{}", msg);
                    return msg;
                }
            }
        };

        let account = match account {
            Some(a) => a,
            None => return "Account not found".to_string(),
        };

        // Resolve threading headers when replying: In-Reply-To set to the
        // original message-id, References = original chain + this message-id.
        let (mut in_reply_to, mut references) = (None, None);
        if reply_uid > 0 {
            let refs_opt = {
                let cache = self.cache.lock().await;
                cache
                    .message_refs(account_id, reply_folder_path, reply_uid)
                    .ok()
                    .flatten()
            };
            if let Some((Some(mid), existing_refs)) = refs_opt {
                let mut chain = existing_refs.unwrap_or_default();
                if !chain.is_empty() {
                    chain.push(' ');
                }
                chain.push_str(&mid);
                in_reply_to = Some(mid);
                references = Some(chain);
            }
        }
        let in_reply_to = in_reply_to.as_deref();
        let references = references.as_deref();

        // Parse attachment paths from JSON array: ["path1","path2"]
        let attachments: Vec<EmailAttachment> = if !attachment_paths.is_empty() {
            serde_json::from_str(attachment_paths).unwrap_or_default()
        } else {
            Vec::new()
        };

        match send_with_auth(
            &self.secrets,
            &account,
            to,
            cc,
            "",
            "",
            subject,
            body,
            in_reply_to,
            references,
            &attachments,
        )
        .await
        {
            Ok(()) => {
                tracing::info!("Sent email from {} to {}", account.email, to);
                String::new()
            }
            Err(e) => {
                // Persist the message so the background flusher retries it once
                // the connection recovers.
                let msg = format!("SMTP send failed; queued for retry: {}", e);
                tracing::error!("{}", msg);
                {
                    let cache = self.cache.lock().await;
                    if let Err(qe) = cache.enqueue_outbox(account_id, to, cc, "", "", subject, body)
                    {
                        tracing::error!("Failed to queue message in outbox: {}", qe);
                    }
                }
                msg
            }
        }
    }
}
