mod cache;
mod discover;
mod imap;
mod mail;
mod smtp;
mod secrets;
mod sync;

use cache::{Cache, Account};
use secrets::Secrets;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use zbus::connection::Builder;
use zbus::interface;
use zbus::object_server::SignalContext;

struct Daemon {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    sync_engine: sync::SyncEngine,
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

        // Store token in keyring
        let key_name = format!("pelliper:{}", email);
        if let Err(e) = self.secrets.store_password(&key_name, auth_token).await {
            tracing::error!("Failed to store credential in keyring: {}", e);
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
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");

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

    tracing::info!("Pelliper daemon started, data dir: {:?}", data_dir);

    let daemon = Daemon { cache: cache.clone(), secrets, sync_engine: sync_engine.clone() };

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

    // Keep the daemon running
    std::future::pending::<()>().await;
}
