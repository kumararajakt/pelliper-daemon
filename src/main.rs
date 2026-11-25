mod config;
mod db;
mod ipc;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::DaemonConfig;
use crate::db::Database;
use crate::ipc::server::IpcServer;

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
    let ipc = IpcServer::new(db.clone());

    tokio::select! {
        res = ipc.run(&cfg.socket_path) => {
            res?;
        }
        _ = shutdown_signal() => {
            info!("Shutdown signal received, cleaning up...");
        }
    }

    let _ = db.checkpoint_wal();
    if cfg.socket_path.exists() {
        let _ = std::fs::remove_file(&cfg.socket_path);
    }

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
