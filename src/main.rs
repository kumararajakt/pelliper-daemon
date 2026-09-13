mod cache;
mod discover;
mod imap;
mod mail;
mod oauth;
mod secrets;
mod smtp;
mod worker;

use cache::{Account, Cache};
use secrets::Secrets;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use zbus::connection::Builder;
use zbus::interface;

/// Spawn a background worker for one account and return its handle.
/// Handles are stored in a map so the D-Bus layer can route requests.
fn spawn_worker(
    account: &Account,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    attachments_dir: PathBuf,
) -> worker::WorkerHandle {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cache_clone = cache.clone();
    let secrets_clone = secrets.clone();
    let account_clone = account.clone();

    tokio::spawn(async move {
        let w = worker::Worker::new(account_clone, cache_clone, secrets_clone, rx, attachments_dir);
        w.run().await;
    });

    worker::WorkerHandle::new(tx)
}

struct Daemon {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    workers: Arc<std::sync::Mutex<HashMap<i64, worker::WorkerHandle>>>,
    attachments_dir: PathBuf,
}

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

/// Send an email via SMTP, resolving the account's auth secret first: a
/// keyring password for password auth, a freshly refreshed XOAUTH2 access
/// token for OAuth accounts.
async fn send_with_auth(
    secrets: &Secrets,
    account: &Account,
    to: &str,
    cc: &str,
    bcc: &str,
    reply_to: &str,
    subject: &str,
    body: &str,
    in_reply_to: Option<&str>,
    references: Option<&str>,
) -> Result<(), String> {
    let auth: String = if account.auth_type == "oauth" {
        let refresh_token = account
            .refresh_token
            .as_deref()
            .ok_or_else(|| "No refresh token stored for account".to_string())?;
        let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID")
            .map_err(|_| "PELLIPER_GOOGLE_CLIENT_ID not set (see ~/.local/share/pelliper/.env)".to_string())?;
        let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();
        let token = crate::oauth::refresh_access_token(
            "https://oauth2.googleapis.com/token",
            &client_id,
            &client_secret,
            refresh_token,
        )
        .await
        .map_err(|e| format!("Failed to refresh access token: {}", e))?;
        token.access_token
    } else {
        let key_name = format!("pelliper:{}", account.email);
        secrets
            .get_password(&key_name)
            .await
            .map_err(|e| format!("Failed to retrieve stored password: {}", e))?
    };

    crate::smtp::send_email(
        account,
        to,
        cc,
        Some(bcc),
        Some(reply_to),
        subject,
        body,
        &auth,
        in_reply_to,
        references,
    )
    .await
    .map_err(|e| e.to_string())
}

/// How often the background flusher polls the outbox for messages that failed
/// to send. This is the "flush on reconnect" mechanism: any message that could
/// not be delivered is retried on this cadence until it goes out or hits the
/// attempt cap.
const OUTBOX_FLUSH_INTERVAL_SECS: u64 = 30;

/// Retry attempts before a stuck queued message is dropped permanently.
const MAX_OUTBOX_ATTEMPTS: i32 = 10;

/// Background task: periodically try to send every queued outbox message.
/// Runs immediately at startup (so messages queued across daemon restarts are
/// flushed on next launch), then on a fixed cadence.
async fn outbox_flusher(cache: Arc<Mutex<Cache>>, secrets: Secrets) {
    loop {
        let items = cache.lock().await.load_pending_outbox().unwrap_or_default();
        for item in items {
            let account = {
                let cache = cache.lock().await;
                cache.load_account(item.account_id).ok().flatten()
            };
            let account = match account {
                Some(a) => a,
                None => {
                    tracing::warn!(
                        "outbox: account {} for item {} no longer exists, dropping",
                        item.account_id,
                        item.id
                    );
                    let _ = cache.lock().await.delete_outbox(item.id);
                    continue;
                }
            };

            match send_with_auth(
                &secrets,
                &account,
                &item.to_addr,
                item.cc_addr.as_deref().unwrap_or(""),
                item.bcc_addr.as_deref().unwrap_or(""),
                item.reply_to.as_deref().unwrap_or(""),
                &item.subject,
                &item.body,
                None,
                None,
            )
            .await
            {
                Ok(()) => {
                    tracing::info!("outbox: flushed item {} to {}", item.id, item.to_addr);
                    let _ = cache.lock().await.delete_outbox(item.id);
                }
                Err(e) => {
                    let attempts = {
                        let cache = cache.lock().await;
                        cache.increment_outbox_attempts(item.id).unwrap_or(0)
                    };
                    if attempts >= MAX_OUTBOX_ATTEMPTS {
                        tracing::error!(
                            "outbox: item {} hit {} attempts, dropping: {}",
                            item.id,
                            MAX_OUTBOX_ATTEMPTS,
                            e
                        );
                        let _ = cache.lock().await.delete_outbox(item.id);
                    } else {
                        tracing::warn!(
                            "outbox: retry {} for item {} failed: {}",
                            attempts,
                            item.id,
                            e
                        );
                    }
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(OUTBOX_FLUSH_INTERVAL_SECS)).await;
    }
}

#[interface(name = "org.kde.pelliper.Daemon")]
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
    ) -> bool {
        tracing::info!("add_account: {} ({})", email, auth_type);

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
            provider: None,
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
    async fn set_message_starred(&self, account_id: i64, folder_path: &str, uid: i64, starred: bool) {
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
        )
        .await
        {
            Ok(()) => {
                tracing::info!(
                    "Sent email from {} to {}",
                    account.email,
                    to
                );
                String::new()
            }
            Err(e) => {
                // Persist the message so the background flusher retries it once
                // the connection recovers.
                let msg = format!("SMTP send failed; queued for retry: {}", e);
                tracing::error!("{}", msg);
                {
                    let cache = self.cache.lock().await;
                    if let Err(qe) =
                        cache.enqueue_outbox(account_id, to, cc, "", "", subject, body)
                    {
                        tracing::error!("Failed to queue message in outbox: {}", qe);
                    }
                }
                msg
            }
        }
    }
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load .env file from data dir or current directory
    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("pelliper");

    let env_file = data_dir.join(".env");
    if env_file.exists() {
        tracing::info!("Loading env from {:?}", env_file);
        dotenvy::from_path(&env_file).ok();
    } else {
        dotenvy::dotenv().ok();
    }

    tracing_subscriber::fmt::init();

    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("pelliper");

    std::fs::create_dir_all(&data_dir).ok();

    let cache = Cache::open(&data_dir).expect("Failed to open cache");
    cache.create_tables().expect("Failed to create tables");
    cache.fts_init().expect("Failed to init search index");
    let cache = Arc::new(Mutex::new(cache));

    let secrets = Secrets::new();

    // Spawn a background worker per enabled account. Each worker owns an IMAP
    // session and runs the priority-ordered sync/backfill/prefetch/IDLE loop.
    let workers: Arc<std::sync::Mutex<HashMap<i64, worker::WorkerHandle>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    {
        let accounts = {
            let cache_guard = cache.lock().await;
            cache_guard.load_accounts().unwrap_or_default()
        };
        for account in &accounts {
            if !account.enabled {
                continue;
            }
            let handle = spawn_worker(
                account,
                cache.clone(),
                secrets.clone(),
                data_dir.join("attachments"),
            );
            workers.lock().unwrap().insert(account.id, handle);
        }
        tracing::info!(
            "Pelliper daemon started with {} worker(s), data dir: {:?}",
            accounts.len(),
            data_dir
        );
    }

    let daemon = Daemon {
        cache: cache.clone(),
        secrets: secrets.clone(),
        workers,
        attachments_dir: data_dir.join("attachments"),
    };

    // Background outbox flusher: retries messages that failed to send until
    // the connection recovers (also flushes anything queued before startup).
    tokio::spawn(outbox_flusher(cache.clone(), secrets));

    // Register on D-Bus session bus
    let _conn = Builder::session()
        .expect("Failed to connect to D-Bus session bus")
        .name("org.kde.pelliper.Daemon")
        .expect("Failed to claim D-Bus name")
        .serve_at("/org/kde/pelliper/Daemon", daemon)
        .expect("Failed to register D-Bus object")
        .build()
        .await
        .expect("Failed to start D-Bus server");

    tracing::info!("D-Bus server running as org.kde.pelliper.Daemon");

    // Keep the daemon running
    std::future::pending::<()>().await;
}
