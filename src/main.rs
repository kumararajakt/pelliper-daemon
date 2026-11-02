mod actions;
mod cache;
mod dbus;
mod imap;
mod migrations;
mod oauth;
mod outbox;
mod secrets;
mod smtp;
mod sync_failures;
mod types;
mod worker;

use cache::Cache;
use secrets::Secrets;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use types::{Account, DaemonSignal, SignalSender};
use zbus::connection::Builder;
use zbus::object_server::SignalEmitter;

fn spawn_worker(
    account: &Account,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    attachments_dir: PathBuf,
    signal_tx: Option<SignalSender>,
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
            signal_tx,
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
    signal_tx: Option<SignalSender>,
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

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
    cache.migrate().expect("Failed to migrate schema");
    cache.fts_init().expect("Failed to init search index");
    let cache = Arc::new(Mutex::new(cache));

    let secrets = Secrets::new();

    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::unbounded_channel::<DaemonSignal>();

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
                Some(signal_tx.clone()),
            );
            handle.send(worker::WorkerRequest::SyncAll);
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
        signal_tx: Some(signal_tx.clone()),
    };

    tokio::spawn(crate::outbox::outbox_flusher(
        cache.clone(),
        secrets,
        Some(signal_tx),
    ));

    let conn = Builder::session()
        .expect("Failed to connect to D-Bus session bus")
        .name("app.pelliper.Daemon")
        .expect("Failed to claim D-Bus name")
        .serve_at("/app/pelliper/Daemon", daemon)
        .expect("Failed to register D-Bus object")
        .build()
        .await
        .expect("Failed to start D-Bus server");

    tracing::info!("D-Bus server running as app.pelliper.Daemon");

    let emitter = SignalEmitter::new(&conn, "/app/pelliper/Daemon")
        .expect("Failed to create D-Bus signal emitter");
    tokio::spawn(async move {
        while let Some(sig) = signal_rx.recv().await {
            let res = match sig {
                DaemonSignal::NewMailReceived {
                    account_id,
                    folder_path,
                    count,
                } => Daemon::new_mail_received(&emitter, account_id, &folder_path, count).await,
                DaemonSignal::FolderUnreadChanged {
                    account_id,
                    folder_path,
                    unread_count,
                } => {
                    Daemon::folder_unread_changed(&emitter, account_id, &folder_path, unread_count)
                        .await
                }
                DaemonSignal::MessageFlagsChanged {
                    account_id,
                    folder_path,
                    uid,
                    is_read,
                    is_starred,
                } => {
                    Daemon::message_flags_changed(
                        &emitter,
                        account_id,
                        &folder_path,
                        uid,
                        is_read,
                        is_starred,
                    )
                    .await
                }
                DaemonSignal::OutboxItemSent {
                    account_id,
                    item_id,
                } => Daemon::outbox_item_sent(&emitter, account_id, item_id).await,
            };
            if let Err(e) = res {
                tracing::warn!("Failed to emit D-Bus signal: {e}");
            }
        }
    });

    std::future::pending::<()>().await;
}
