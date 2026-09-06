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

struct Daemon {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
}

#[interface(name = "org.kde.pelliper.Daemon")]
impl Daemon {
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

    tracing::info!("Pelliper daemon started, data dir: {:?}", data_dir);

    let daemon = Daemon { cache, secrets };

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
