use std::path::Path;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::autoconfig::lookup_autoconfig;
use crate::db::repo::AccountRow;
use crate::db::Database;
use crate::imap::client::{test_imap_connection, ImapAuth};
use crate::imap::sync_worker::SyncWakeCommand;
use crate::ipc::protocol::{RpcNotification, RpcRequest, RpcResponse};
use crate::search::parse_search_query;
use crate::secrets::{delete_secret, store_secret, AccountSecret};
use crate::smtp::{test_smtp_connection, SmtpAuth};
use crate::supervisor::AccountSupervisor;

pub struct IpcServer {
    db: Database,
    supervisor: AccountSupervisor,
    events: broadcast::Sender<RpcNotification>,
}

impl IpcServer {
    pub fn new(
        db: Database,
        supervisor: AccountSupervisor,
        events: broadcast::Sender<RpcNotification>,
    ) -> Self {
        Self {
            db,
            supervisor,
            events,
        }
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
            let supervisor = self.supervisor.clone();
            let events_tx = self.events.clone();
            let mut rx = self.events.subscribe();

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, db, supervisor, events_tx, &mut rx).await {
                    warn!("IPC client disconnected: {e}");
                }
            });
        }
    }
}

async fn handle_client(
    stream: UnixStream,
    db: Database,
    supervisor: AccountSupervisor,
    events_tx: broadcast::Sender<RpcNotification>,
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
                    Ok(req) => dispatch(req, &db, &supervisor, &events_tx).await,
                    Err(e) => RpcResponse::err(Value::Null, -32700, format!("parse error: {e}")),
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

#[derive(Deserialize)]
struct TestAndAddParams {
    email: String,
    display_name: String,
    imap_host: String,
    imap_port: u16,
    imap_tls: String,
    smtp_host: String,
    smtp_port: u16,
    smtp_tls: String,
    auth_type: String,
    oauth_provider: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

async fn dispatch(
    req: RpcRequest,
    db: &Database,
    supervisor: &AccountSupervisor,
    events: &broadcast::Sender<RpcNotification>,
) -> RpcResponse {
    let id = req.id;
    match req.method.as_str() {
        "daemon.ping" => RpcResponse::ok(id, json!({ "pong": true })),
        "daemon.status" => {
            let accounts = db.account_count().unwrap_or(0);
            RpcResponse::ok(
                id,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "accounts": accounts,
                    "state": "ready"
                }),
            )
        }
        "account.list" => match db.list_accounts() {
            Ok(list) => RpcResponse::ok(id, json!({ "accounts": list })),
            Err(e) => RpcResponse::err(id, -32000, e.to_string()),
        },
        "account.autoconfig" => {
            let email = req.params.get("email").and_then(|v| v.as_str()).unwrap_or("");
            match lookup_autoconfig(email).await {
                Ok(Some(cfg)) => RpcResponse::ok(id, json!({ "found": true, "config": cfg })),
                Ok(None) => RpcResponse::ok(id, json!({ "found": false })),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "account.test_and_add" => {
            let p: TestAndAddParams = match serde_json::from_value(req.params) {
                Ok(v) => v,
                Err(e) => return RpcResponse::err(id, -32602, format!("invalid params: {e}")),
            };

            let (imap_auth, smtp_auth, secret) = if p.auth_type == "oauth2" {
                let access = p.access_token.clone().unwrap_or_default();
                let refresh = p.refresh_token.clone().unwrap_or_default();
                (
                    ImapAuth::XOAuth2 { user: p.email.clone(), access_token: access.clone() },
                    SmtpAuth::XOAuth2 { user: p.email.clone(), access_token: access.clone() },
                    AccountSecret::OAuth2 { access_token: access, refresh_token: refresh, expires_at: 0 },
                )
            } else {
                let pass = p.password.clone().unwrap_or_default();
                (
                    ImapAuth::Password { user: p.email.clone(), password: pass.clone() },
                    SmtpAuth::Password { user: p.email.clone(), password: pass.clone() },
                    AccountSecret::Password { password: pass },
                )
            };

            if let Err(e) = test_imap_connection(&p.imap_host, p.imap_port, &imap_auth).await {
                return RpcResponse::err(id, -32001, format!("IMAP verification failed: {e}"));
            }
            if let Err(e) = test_smtp_connection(&p.smtp_host, p.smtp_port, &p.smtp_tls, &smtp_auth).await {
                return RpcResponse::err(id, -32002, format!("SMTP verification failed: {e}"));
            }

            if let Err(e) = store_secret(&p.email, &secret).await {
                return RpcResponse::err(id, -32003, format!("Keyring error: {e}"));
            }

            let mut row = AccountRow {
                id: 0,
                email: p.email,
                display_name: p.display_name,
                imap_host: p.imap_host,
                imap_port: p.imap_port,
                imap_tls: p.imap_tls,
                smtp_host: p.smtp_host,
                smtp_port: p.smtp_port,
                smtp_tls: p.smtp_tls,
                auth_type: p.auth_type,
                oauth_provider: p.oauth_provider,
                sync_interval_secs: 300,
                enabled: true,
                last_error: None,
            };

            match db.insert_account(&row) {
                Ok(account_id) => {
                    row.id = account_id;
                    supervisor.spawn_account(row.clone()).await;
                    RpcResponse::ok(id, json!({ "account": row }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "account.clear_cache" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            supervisor.stop_account(account_id);
            if let Err(e) = db.clear_account_cache(account_id) {
                return RpcResponse::err(id, -32000, e.to_string());
            }
            if let Ok(Some(acc)) = db.get_account(account_id) {
                if acc.enabled {
                    supervisor.spawn_account(acc).await;
                }
            }
            RpcResponse::ok(id, json!({ "cleared": true }))
        }
        "account.remove" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            supervisor.stop_account(account_id);
            match db.delete_account(account_id) {
                Ok(Some(email)) => {
                    let _ = delete_secret(&email).await;
                    RpcResponse::ok(id, json!({ "removed": true }))
                }
                Ok(None) => RpcResponse::err(id, -32004, "account not found"),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "folder.list" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64());
            match db.list_folders(account_id) {
                Ok(folders) => RpcResponse::ok(id, json!({ "folders": folders })),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "folder.mark_all_read" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64()).unwrap_or(0);
            match db.mark_folder_all_read_optimistic(folder_id) {
                Ok(account_id) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": folder_id, "unread_count": 0 }),
                    ));
                    supervisor.wake_account(account_id, SyncWakeCommand::PendingOps);
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.search" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64());
            let query_str = req.params.get("query").and_then(|v| v.as_str()).unwrap_or("");
            let limit = req.params.get("limit").and_then(|v| v.as_i64()).unwrap_or(100);
            let parsed = parse_search_query(query_str);
            match db.search_messages_local(folder_id, &parsed, limit) {
                Ok(results) => RpcResponse::ok(id, json!({ "messages": results })),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.prefetch_bodies" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let ids: Vec<i64> = req
                .params
                .get("message_ids")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|x| x.as_i64()).collect())
                .unwrap_or_default();
            supervisor.wake_account(account_id, SyncWakeCommand::PrefetchBodies(ids));
            RpcResponse::ok(id, json!({ "queued": true }))
        }
        "message.get" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let message_id = req.params.get("message_id").and_then(|v| v.as_i64()).unwrap_or(0);
            match db.get_message_detail(message_id) {
                Ok(Some(detail)) => {
                    if !detail.get("body_synced").and_then(|v| v.as_bool()).unwrap_or(false) {
                        supervisor.wake_account(account_id, SyncWakeCommand::FetchBodyNow(message_id));
                    }
                    RpcResponse::ok(id, json!({ "message": detail }))
                }
                Ok(None) => {
                    supervisor.wake_account(account_id, SyncWakeCommand::FetchBodyNow(message_id));
                    RpcResponse::ok(id, json!({ "requested": message_id }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.set_flags" => {
            let message_id = req.params.get("message_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let add_mask = req.params.get("add_mask").and_then(|v| v.as_i64()).unwrap_or(0);
            let remove_mask = req.params.get("remove_mask").and_then(|v| v.as_i64()).unwrap_or(0);
            match db.set_flags_optimistic(message_id, add_mask, remove_mask) {
                Ok(Some((account_id, _fid))) => {
                    supervisor.wake_account(account_id, SyncWakeCommand::PendingOps);
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Ok(None) => RpcResponse::err(id, -32004, "message not found"),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.archive" | "message.delete" => {
            let message_id = req.params.get("message_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let target_role = if req.method == "message.archive" { "archive" } else { "trash" };
            match db.delete_or_move_optimistic(message_id, target_role) {
                Ok(Some((account_id, folder_id))) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": folder_id }),
                    ));
                    supervisor.wake_account(account_id, SyncWakeCommand::PendingOps);
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Ok(None) => RpcResponse::err(id, -32004, "message not found"),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.send" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let payload_str = req.params.to_string();
            match db.enqueue_outbox(account_id, &payload_str) {
                Ok(outbox_id) => {
                    let _ = events.send(RpcNotification::new(
                        "outbox.status",
                        json!({ "outbox_id": outbox_id, "status": "queued" }),
                    ));
                    RpcResponse::ok(id, json!({ "outbox_id": outbox_id, "status": "queued" }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        other => RpcResponse::err(id, -32601, format!("method not found: {other}")),
    }
}
