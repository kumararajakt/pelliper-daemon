mod autoconfig;
mod config;
mod db;
mod imap;
mod ipc;
mod mime;
mod search;
mod secrets;
mod smtp;
mod supervisor;

use anyhow::Result;
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::DaemonConfig;
use crate::db::Database;
use crate::ipc::protocol::RpcNotification;
use crate::ipc::server::IpcServer;
use crate::supervisor::AccountSupervisor;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = DaemonConfig::load()?;
    cfg.ensure_dirs()?;

    info!("Starting pelliper-daemon (db: {})", cfg.db_path.display());
    let db = Database::open(&cfg.db_path)?;
    let (events_tx, _) = broadcast::channel::<RpcNotification>(128);
    let supervisor = AccountSupervisor::new(db.clone(), events_tx.clone());
    supervisor.start_all_enabled().await;

    let ipc = IpcServer::new(db.clone(), supervisor.clone(), events_tx);

    tokio::select! {
        res = ipc.run(&cfg.socket_path) => {
            res?;
        }
        _ = shutdown_signal() => {
            info!("Shutdown signal received, cleaning up...");
        }
    }

    supervisor.shutdown_all();
    let _ = db.checkpoint_wal();
    if cfg.socket_path.exists() {
        let _ = std::fs::remove_file(&cfg.socket_path);
    }

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
