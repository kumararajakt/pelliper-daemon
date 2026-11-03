use crate::imap;
use crate::outbox::send_with_auth;
use crate::spawn_worker;
use crate::types::{Account, EmailAttachment};
use crate::worker;
use crate::Daemon;
use zbus::interface;
use zbus::object_server::SignalEmitter;

impl Daemon {
    fn get_worker(&self, account_id: i64) -> Option<worker::WorkerHandle> {
        self.workers.lock().unwrap().get(&account_id).cloned()
    }

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
            self.signal_tx.clone(),
        );
        handle.send(worker::WorkerRequest::SyncAll);
        self.workers.lock().unwrap().insert(account.id, handle);
    }
}

#[interface(name = "app.pelliper.Daemon")]
impl Daemon {
    #[zbus(signal, name = "NewMailReceived")]
    pub async fn new_mail_received(
        signal_ctxt: &SignalEmitter<'_>,
        account_id: i64,
        folder_path: &str,
        count: i32,
    ) -> zbus::Result<()>;

    #[zbus(signal, name = "FolderUnreadChanged")]
    pub async fn folder_unread_changed(
        signal_ctxt: &SignalEmitter<'_>,
        account_id: i64,
        folder_path: &str,
        unread_count: i32,
    ) -> zbus::Result<()>;

    #[zbus(signal, name = "MessageFlagsChanged")]
    pub async fn message_flags_changed(
        signal_ctxt: &SignalEmitter<'_>,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        is_read: bool,
        is_starred: bool,
    ) -> zbus::Result<()>;

    #[zbus(signal, name = "OutboxItemSent")]
    pub async fn outbox_item_sent(
        signal_ctxt: &SignalEmitter<'_>,
        account_id: i64,
        item_id: i64,
    ) -> zbus::Result<()>;

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

        {
            let cache = self.cache.lock().await;
            if let Ok(exists) = cache.account_exists(email) {
                if exists {
                    tracing::warn!("Account already exists: {}", email);
                    return false;
                }
            }
        }

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
                self.ensure_worker(&a);
            }
        }

        saved_id.is_some()
    }

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

    async fn sync_all(&self) -> bool {
        tracing::info!("D-Bus: sync_all requested");
        let workers: Vec<worker::WorkerHandle> =
            self.workers.lock().unwrap().values().cloned().collect();
        for handle in workers {
            handle.send(worker::WorkerRequest::SyncAll);
        }
        true
    }

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

    async fn count_messages(&self, account_id: i64, folder_path: &str) -> i64 {
        let cache = self.cache.lock().await;
        cache.count_messages(account_id, folder_path).unwrap_or(0)
    }

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

    async fn search_messages(&self, account_id: i64, query: &str) -> String {
        let cache = self.cache.lock().await;
        match cache.search_messages(account_id, query) {
            Ok(results) => serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string()),
            Err(e) => {
                tracing::error!("Search failed: {e}");
                "[]".to_string()
            }
        }
    }

    async fn is_known_address(&self, email: &str) -> bool {
        let cache = self.cache.lock().await;
        cache.is_known_address(email).unwrap_or(false)
    }

    async fn block_sender(&self, sender_email: &str) {
        let cache = self.cache.lock().await;
        let _ = cache.set_sender_policy(sender_email, true, false);
    }

    async fn unblock_sender(&self, sender_email: &str) {
        let cache = self.cache.lock().await;
        let _ = cache.remove_sender_policy(sender_email);
    }

    async fn is_sender_blocked(&self, sender_email: &str) -> bool {
        let cache = self.cache.lock().await;
        cache.is_sender_blocked(sender_email).unwrap_or(false)
    }

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

    async fn delete_message(&self, account_id: i64, folder_path: &str, uid: i64) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::DeleteMessage {
                folder_path: folder_path.to_string(),
                uid,
            });
        }
    }

    async fn create_folder(&self, account_id: i64, name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::CreateFolder {
                name: name.to_string(),
            });
        }
    }

    async fn rename_folder(&self, account_id: i64, old_name: &str, new_name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::RenameFolder {
                old_name: old_name.to_string(),
                new_name: new_name.to_string(),
            });
        }
    }

    async fn delete_folder(&self, account_id: i64, name: &str) {
        if let Some(handle) = self.get_worker(account_id) {
            handle.send(worker::WorkerRequest::DeleteFolder {
                name: name.to_string(),
            });
        }
    }

    async fn load_body(&self, account_id: i64, folder_path: &str, uid: i64) -> String {
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
