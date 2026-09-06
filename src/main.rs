mod cache;
mod discover;
mod idle;
mod imap;
mod mail;
mod oauth;
mod secrets;
mod smtp;
mod sync;

use cache::{Account, Cache};
use secrets::Secrets;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use zbus::connection::Builder;
use zbus::interface;

struct Daemon {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    sync_engine: sync::SyncEngine,
    idle_router: std::sync::Arc<idle::IdleRouter>,
    idle_notify_tx: mpsc::Sender<idle::IdleNotification>,
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

        let saved = {
            let mut cache = self.cache.lock().await;
            match cache.insert_account(&account) {
                Ok(id) => {
                    tracing::info!("Account saved with id {}: {}", id, email);
                    true
                }
                Err(e) => {
                    tracing::error!("Failed to save account {}: {}", email, e);
                    false
                }
            }
        };

        // Trigger initial sync in the background
        if saved {
            let sync = self.sync_engine.clone();
            let email_clone = email.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                tracing::info!("Starting initial sync for {}", email_clone);
                if let Err(e) = sync.sync_all().await {
                    tracing::error!("Initial sync failed for {}: {}", email_clone, e);
                }
            });

            // Spawn IDLE worker for the new account
            let cache_guard = self.cache.lock().await;
            if let Ok(accounts) = cache_guard.load_accounts() {
                if let Some(account) = accounts.iter().find(|a| a.email == email) {
                    let account_id = account.id;
                    drop(cache_guard);
                    idle::spawn_idle_worker_for_account(
                        &self.idle_router,
                        self.cache.clone(),
                        self.secrets.clone(),
                        self.idle_notify_tx.clone(),
                        account_id,
                    );
                }
            }
        }

        saved
    }

    /// Remove an account and all its data (folders, messages, bodies, attachments).
    /// Returns true on success.
    async fn remove_account(&self, account_id: i64) -> bool {
        tracing::info!("D-Bus: remove_account id={}", account_id);

        // Clear the IDLE folder for this account — worker will detect account is gone
        self.idle_router.set_folder(account_id, "").await;

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

    /// Trigger a full sync of all enabled accounts.
    async fn sync_all(&self) -> bool {
        tracing::info!("D-Bus: sync_all requested");
        match self.sync_engine.sync_all().await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("sync_all failed: {}", e);
                false
            }
        }
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
            Some(account) => match self.sync_engine.sync_account(&account).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!("sync_account failed for {}: {}", email, e);
                    false
                }
            },
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

    /// Tell the IDLE manager to watch a specific folder for real-time updates.
    /// Called by the UI whenever the user switches folders.
    async fn set_idle_folder(&self, account_id: i64, folder_path: &str) {
        tracing::info!(
            "D-Bus: set_idle_folder account={} folder={}",
            account_id,
            folder_path
        );
        self.idle_router.set_folder(account_id, folder_path).await;
    }

    /// Load the body of a message. Returns HTML.
    /// Fetches from IMAP if not cached, then caches for next time.
    /// Retries once with a fresh connection on failure.
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

        let account = {
            let cache = self.cache.lock().await;
            match cache.load_accounts() {
                Ok(accounts) => accounts.into_iter().find(|a| a.id == account_id),
                Err(e) => {
                    tracing::error!("Failed to load accounts: {}", e);
                    return format!("<p>Error loading account: {}</p>", e);
                }
            }
        };

        let account = match account {
            Some(a) => a,
            None => return "<p>Account not found</p>".to_string(),
        };

        // Attempt 1
        match self.try_load_body(&account, folder_path, uid).await {
            Ok((html, has_attachments)) => {
                self.cache_body(account_id, folder_path, uid, &html, has_attachments)
                    .await;
                return html;
            }
            Err(e) => {
                tracing::warn!("Body fetch attempt 1 failed for uid={}: {}", uid, e);
            }
        }

        // Attempt 2: reconnect and retry once
        tracing::info!("Retrying body fetch for uid={} with fresh connection", uid);
        match self.try_load_body(&account, folder_path, uid).await {
            Ok((html, has_attachments)) => {
                self.cache_body(account_id, folder_path, uid, &html, has_attachments)
                    .await;
                html
            }
            Err(e) => {
                tracing::error!("Body fetch attempt 2 failed for uid={}: {}", uid, e);
                format!("<p>Failed to load message: {}</p>", e)
            }
        }
    }
}

// Private helpers (not D-Bus methods)
impl Daemon {
    /// Single attempt to connect and fetch a message body.
    async fn try_load_body(
        &self,
        account: &Account,
        folder_path: &str,
        uid: i64,
    ) -> Result<(String, bool), Box<dyn std::error::Error + Send + Sync>> {
        let host = account.imap_host.as_deref().unwrap_or("localhost");
        let port = account.imap_port.unwrap_or(993) as u16;

        let mut session = if account.auth_type == "oauth" {
            let refresh_token = account
                .refresh_token
                .as_deref()
                .ok_or("No refresh token configured")?;
            let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID").unwrap_or_else(|_| {
                "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com"
                    .to_string()
            });
            let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();
            let token = oauth::refresh_access_token(
                "https://oauth2.googleapis.com/token",
                &client_id,
                &client_secret,
                refresh_token,
            )
            .await?;
            imap::connect_session_oauth(host, port, &account.email, &token.access_token).await?
        } else {
            let key_name = format!("pelliper:{}", account.email);
            let password = self.secrets.get_password(&key_name).await?;
            imap::connect_session_password_raw(host, port, &account.email, &password).await?
        };

        session.select_folder(folder_path).await?;
        let raw_body = session.fetch_body(uid).await?;
        session.logout().await;

        let parsed = imap::parse_message_body(&raw_body);
        let html = if parsed.html.is_empty() {
            format!("<pre>{}</pre>", parsed.text)
        } else {
            parsed.html
        };

        Ok((html, parsed.has_attachments))
    }

    /// Cache body and update has_attachments flag.
    async fn cache_body(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        html: &str,
        has_attachments: bool,
    ) {
        let cache = self.cache.lock().await;
        if let Err(e) = cache.save_body(account_id, folder_path, uid, html) {
            tracing::error!("Failed to cache body: {}", e);
        }
        if let Err(e) = cache.update_has_attachment(account_id, folder_path, uid, has_attachments) {
            tracing::error!("Failed to update has_attachments: {}", e);
        }
        tracing::info!("Body loaded and cached for uid={}", uid);
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
    let cache = Arc::new(Mutex::new(cache));

    let secrets = Secrets::new();

    let sync_engine = sync::SyncEngine::new(cache.clone(), secrets.clone());

    // Spawn IDLE workers for all enabled accounts
    let (idle_notify_tx, mut idle_notify_rx) = mpsc::channel::<idle::IdleNotification>(16);
    let idle_router = std::sync::Arc::new(
        idle::spawn_idle_workers(cache.clone(), secrets.clone(), idle_notify_tx.clone()),
    );

    tracing::info!("Pelliper daemon started, data dir: {:?}", data_dir);

    let daemon = Daemon {
        cache: cache.clone(),
        secrets,
        sync_engine: sync_engine.clone(),
        idle_router: idle_router.clone(),
        idle_notify_tx: idle_notify_tx.clone(),
    };

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

    // Auto-sync on startup if accounts exist
    {
        let cache_guard = cache.lock().await;
        if let Ok(count) = cache_guard.account_count() {
            if count > 0 {
                tracing::info!("Found {} account(s), starting initial sync", count);
                let sync = sync_engine.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    if let Err(e) = sync.sync_all().await {
                        tracing::error!("Startup sync failed: {}", e);
                    }
                });
            }
        }
    }

    // Handle IDLE notifications (cache already updated by workers)
    tokio::spawn(async move {
        while let Some(notification) = idle_notify_rx.recv().await {
            tracing::info!(
                "IDLE notification: account={} folder={}",
                notification.account_id,
                notification.folder_path
            );
        }
    });

    // Keep the daemon running
    std::future::pending::<()>().await;
}
