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
) -> worker::WorkerHandle {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cache_clone = cache.clone();
    let secrets_clone = secrets.clone();
    let account_clone = account.clone();

    tokio::spawn(async move {
        let w = worker::Worker::new(account_clone, cache_clone, secrets_clone, rx);
        w.run().await;
    });

    worker::WorkerHandle::new(tx)
}

struct Daemon {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    workers: Arc<std::sync::Mutex<HashMap<i64, worker::WorkerHandle>>>,
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
        let handle = spawn_worker(account, self.cache.clone(), self.secrets.clone());
        self.workers.lock().unwrap().insert(account.id, handle);
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
            let handle = spawn_worker(account, cache.clone(), secrets.clone());
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
        secrets,
        workers,
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

    // Keep the daemon running
    std::future::pending::<()>().await;
}
