use std::path::Path;

use anyhow::Result;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::db::Database;
use crate::ipc::protocol::{RpcNotification, RpcRequest, RpcResponse};

pub struct IpcServer {
    db: Database,
    events: broadcast::Sender<RpcNotification>,
}

impl IpcServer {
    pub fn new(db: Database) -> Self {
        let (events, _) = broadcast::channel(128);
        Self { db, events }
    }

    pub async fn run(&self, socket_path: &Path) -> Result<()> {
        if socket_path.exists() {
            let _ = std::fs::remove_file(socket_path);
        }

        let listener = UnixListener::bind(socket_path)?;
        info!("IPC listening on {}", socket_path.display());

        loop {
            let (stream, _) = listener.accept().await?;
            let db = self.db.clone();
            let mut rx = self.events.subscribe();

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, db, &mut rx).await {
                    warn!("IPC client disconnected: {e}");
                }
            });
        }
    }
}

async fn handle_client(
    stream: UnixStream,
    db: Database,
    events: &mut broadcast::Receiver<RpcNotification>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else {
                    return Ok(());
                };
                if line.trim().is_empty() {
                    continue;
                }

                let response = match serde_json::from_str::<RpcRequest>(&line) {
                    Ok(req) => dispatch(req, &db),
                    Err(e) => RpcResponse::err(serde_json::Value::Null, -32700, format!("parse error: {e}")),
                };

                let mut encoded = serde_json::to_string(&response)?;
                encoded.push('\n');
                write_half.write_all(encoded.as_bytes()).await?;
            }
            notif = events.recv() => {
                match notif {
                    Ok(n) => {
                        let mut encoded = serde_json::to_string(&n)?;
                        encoded.push('\n');
                        write_half.write_all(encoded.as_bytes()).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        error!("IPC client lagged, skipped {skipped} notifications");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn dispatch(req: RpcRequest, db: &Database) -> RpcResponse {
    match req.method.as_str() {
        "daemon.ping" => RpcResponse::ok(req.id, json!({ "pong": true })),
        "daemon.status" => {
            let accounts = db.account_count().unwrap_or(0);
            RpcResponse::ok(
                req.id,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "accounts": accounts,
                    "state": "ready"
                }),
            )
        }
        other => RpcResponse::err(req.id, -32601, format!("method not found: {other}")),
    }
}
