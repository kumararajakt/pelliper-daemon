mod cache;
mod discover;
mod imap;
mod idle;
mod mail;
mod smtp;
mod secrets;
mod sync;
mod oauth;

use cache::{Cache, Account};
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
    idle_tx: mpsc::Sender<idle::IdleCommand>,
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
            let mut cache = self.cache.lock().await;
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
            tracing::warn!("IMAP connection test failed for {}, saving account anyway", email);
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
            refresh_token: if refresh_token.is_empty() { None } else { Some(refresh_token.to_string()) },
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
                // Small delay to let the D-Bus response go out first
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                tracing::info!("Starting initial sync for {}", email_clone);
                // Reload account from DB and sync
                // For now, sync_all will pick up the new account
                if let Err(e) = sync.sync_all().await {
                    tracing::error!("Initial sync failed for {}: {}", email_clone, e);
                }
            });
        }

        saved
    }

    /// Remove an account and all its data (folders, messages, bodies, attachments).
    /// Returns true on success.
    async fn remove_account(&self, account_id: i64) -> bool {
        tracing::info!("D-Bus: remove_account id={}", account_id);

        // Notify IDLE manager to stop watching this account
        let _ = self.idle_tx.send(idle::IdleCommand::SetFolder {
            account_id,
            folder_path: String::new(),
        }).await;

        let mut cache = self.cache.lock().await;
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
    async fn get_messages(&self, account_id: i64, folder_path: &str, offset: i64, limit: i64) -> String {
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
        tracing::info!("D-Bus: set_idle_folder account={} folder={}", account_id, folder_path);
        let _ = self.idle_tx.send(idle::IdleCommand::SetFolder {
            account_id,
            folder_path: folder_path.to_string(),
        }).await;
    }

    /// Load the body of a message. Returns HTML.
    /// Fetches from IMAP if not cached, then caches for next time.
    async fn load_body(&self, account_id: i64, folder_path: &str, uid: i64) -> String {
        // Check cache first
        {
            let cache = self.cache.lock().await;
            if let Ok(Some(html)) = cache.load_body(account_id, folder_path, uid) {
                tracing::debug!("Body cache hit for uid={}", uid);
                return html;
            }
        }

        // Fetch from IMAP
        tracing::info!("Fetching body for account={} folder={} uid={}", account_id, folder_path, uid);

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

        let host = account.imap_host.as_deref().unwrap_or("localhost");
        let port = account.imap_port.unwrap_or(993) as u16;

        // Connect
        let connect_result = if account.auth_type == "oauth" {
            let refresh_token = match account.refresh_token.as_deref() {
                Some(rt) if !rt.is_empty() => rt,
                _ => return "<p>No refresh token configured</p>".to_string(),
            };
            let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID")
                .unwrap_or_else(|_| "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com".to_string());
            let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();

            match oauth::refresh_access_token(
                "https://oauth2.googleapis.com/token",
                &client_id,
                &client_secret,
                refresh_token,
            ).await {
                Ok(token) => imap::connect_session_oauth(host, port, &account.email, &token.access_token).await,
                Err(e) => Err(e),
            }
        } else {
            let key_name = format!("pelliper:{}", account.email);
            match self.secrets.get_password(&key_name).await {
                Ok(password) => imap::connect_session_password_raw(host, port, &account.email, &password).await,
                Err(e) => Err(e),
            }
        };

        let mut session = match connect_result {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to connect for body fetch: {}", e);
                return format!("<p>Failed to connect: {}</p>", e);
            }
        };

        // SELECT folder
        if let Err(e) = session.select_folder(folder_path).await {
            tracing::error!("SELECT failed for body fetch: {}", e);
            session.logout().await;
            return format!("<p>Failed to select folder: {}</p>", e);
        }

        // Fetch body
        let raw_body = match session.fetch_body(uid).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("FETCH BODY failed: {}", e);
                session.logout().await;
                return format!("<p>Failed to fetch body: {}</p>", e);
            }
        };

        session.logout().await;

        // Parse MIME
        let parsed = imap::parse_message_body(&raw_body);
        let html = if parsed.html.is_empty() {
            format!("<pre>{}</pre>", parsed.text)
        } else {
            parsed.html
        };

        // Cache
        {
            let cache = self.cache.lock().await;
            if let Err(e) = cache.save_body(account_id, folder_path, uid, &html) {
                tracing::error!("Failed to cache body: {}", e);
            }
        }

        tracing::info!("Body loaded and cached for uid={}", uid);
        html
    }
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");

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

    // Create IDLE manager — one persistent connection per account
    let (idle_notify_tx, mut idle_notify_rx) = mpsc::channel::<idle::IdleNotification>(16);
    let (mut idle_manager, idle_tx) = idle::IdleManager::new(cache.clone(), secrets.clone(), idle_notify_tx);

    tracing::info!("Pelliper daemon started, data dir: {:?}", data_dir);

    let daemon = Daemon {
        cache: cache.clone(),
        secrets,
        sync_engine: sync_engine.clone(),
        idle_tx: idle_tx.clone(),
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

                // Spawn IDLE manager for the first enabled account
                let accounts = cache_guard.load_accounts().unwrap_or_default();
                if let Some(first) = accounts.into_iter().find(|a| a.enabled) {
                    let account_id = first.id;
                    tokio::spawn(async move {
                        // Small delay to let initial sync finish
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        idle_manager.run(account_id).await;
                    });
                }
            }
        }
    }

    // Spawn a task that reloads IDLE on folder changes from the UI
    tokio::spawn(async move {
        while let Some(notification) = idle_notify_rx.recv().await {
            tracing::info!(
                "IDLE notification: account={} folder={}",
                notification.account_id,
                notification.folder_path
            );
            // The cache was already updated by the IdleManager.
            // QFileSystemWatcher on the SQLite file will trigger model reloads in the UI.
            // Nothing more needed here.
        }
    });

    // Keep the daemon running
    std::future::pending::<()>().await;
}
