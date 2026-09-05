mod cache;
mod discover;
mod imap;
mod mail;
mod smtp;
mod secrets;
mod sync;

use cache::Cache;
use std::path::PathBuf;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("pelliper");

    std::fs::create_dir_all(&data_dir).ok();

    let cache = Cache::open(&data_dir).expect("Failed to open cache");
    cache.create_tables().expect("Failed to create tables");

    tracing::info!("Pelliper daemon started, data dir: {:?}", data_dir);

    // Start D-Bus service
    let (event_tx, event_rx) = mpsc::channel(100);

    // Spawn D-Bus listener
    let tx = event_tx.clone();
    tokio::spawn(async move {
        run_dbus_server(tx).await;
    });

    // Main event loop
    run_event_loop(cache, event_rx).await;
}

async fn run_dbus_server(_event_tx: mpsc::Sender<DaemonEvent>) {
    // TODO: Implement D-Bus server using zbus
    // Expose org.kde.pelliper.Daemon interface
    // Methods: add_account, sync_folder, send_message, etc.
    tracing::info!("D-Bus server placeholder");
}

async fn run_event_loop(_cache: Cache, mut event_rx: mpsc::Receiver<DaemonEvent>) {
    while let Some(event) = event_rx.recv().await {
        match event {
            DaemonEvent::AddAccount { email, .. } => {
                tracing::info!("Adding account: {}", email);
            }
            DaemonEvent::SyncFolder { folder, .. } => {
                tracing::info!("Syncing folder: {}", folder);
            }
            DaemonEvent::SendMessage { to, .. } => {
                tracing::info!("Sending message to: {}", to);
            }
        }
    }
}

#[derive(Debug)]
pub enum DaemonEvent {
    AddAccount {
        email: String,
        display_name: String,
        imap_host: String,
        imap_port: u16,
        smtp_host: String,
        smtp_port: u16,
    },
    SyncFolder {
        account_id: i64,
        folder: String,
    },
    SendMessage {
        account_id: i64,
        to: String,
        subject: String,
        body: String,
    },
}
