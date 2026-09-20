mod cache;
mod dbus;
mod imap;
mod oauth;
mod outbox;
mod secrets;
mod smtp;
mod types;
mod worker;

use cache::Cache;
use secrets::Secrets;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use types::Account;
use zbus::connection::Builder;

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
        let w = worker::Worker::new(
            account_clone,
            cache_clone,
            secrets_clone,
            rx,
            attachments_dir,
        );
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
    tokio::spawn(crate::outbox::outbox_flusher(cache.clone(), secrets));

    // Register on D-Bus session bus
    let _conn = Builder::session()
        .expect("Failed to connect to D-Bus session bus")
        .name("app.pelliper.Daemon")
        .expect("Failed to claim D-Bus name")
        .serve_at("/app/pelliper/Daemon", daemon)
        .expect("Failed to register D-Bus object")
        .build()
        .await
        .expect("Failed to start D-Bus server");

    tracing::info!("D-Bus server running as app.pelliper.Daemon");

    // Keep the daemon running
    std::future::pending::<()>().await;
}
