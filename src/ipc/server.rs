use std::path::{Path, PathBuf};

use anyhow::Result;
use futures::TryStreamExt;
use mail_parser::MimeHeaders;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::autoconfig::lookup_autoconfig;
use crate::db::repo::AccountRow;
use crate::db::Database;
use crate::imap::client::{connect_imap_tls, test_imap_connection, ImapAuth};
use crate::imap::priority::classify_folder;
use crate::imap::sync_worker::SyncWakeCommand;
use crate::ipc::protocol::{RpcNotification, RpcRequest, RpcResponse};
use crate::mime::extract_attachment_bytes;
use crate::oauth::{refresh_if_needed, start_pkce_flow};
use crate::search::{parse_search_query, to_imap_search_criteria};
use crate::secrets::{delete_secret, load_secret, store_secret, AccountSecret};
use crate::smtp::{test_smtp_connection, SmtpAuth};
use crate::supervisor::AccountSupervisor;

pub struct IpcServer {
    db: Database,
    supervisor: AccountSupervisor,
    events: broadcast::Sender<RpcNotification>,
    outbox_wake: mpsc::Sender<()>,
    cache_dir: PathBuf,
}

impl IpcServer {
    pub fn new(
        db: Database,
        supervisor: AccountSupervisor,
        events: broadcast::Sender<RpcNotification>,
        outbox_wake: mpsc::Sender<()>,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            db,
            supervisor,
            events,
            outbox_wake,
            cache_dir,
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
            let outbox_wake = self.outbox_wake.clone();
            let cache_dir = self.cache_dir.clone();
            let mut rx = self.events.subscribe();

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, db, supervisor, events_tx, outbox_wake, cache_dir, &mut rx).await {
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
    outbox_wake: mpsc::Sender<()>,
    cache_dir: PathBuf,
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
                    Ok(req) => dispatch(req, &db, &supervisor, &events_tx, &outbox_wake, &cache_dir).await,
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
    expires_at: Option<i64>,
}

async fn dispatch(
    req: RpcRequest,
    db: &Database,
    supervisor: &AccountSupervisor,
    events: &broadcast::Sender<RpcNotification>,
    outbox_wake: &mpsc::Sender<()>,
    cache_dir: &Path,
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
        "account.oauth_start" => {
            let provider = req.params.get("provider").and_then(|v| v.as_str()).unwrap_or("google");
            let email = req.params.get("email").and_then(|v| v.as_str()).unwrap_or("");
            let client_id = req.params.get("client_id").and_then(|v| v.as_str());
            match start_pkce_flow(provider, email, client_id, events.clone()).await {
                Ok((session_id, auth_url)) => {
                    RpcResponse::ok(id, json!({ "session_id": session_id, "auth_url": auth_url }))
                }
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
                let exp = p.expires_at.unwrap_or(0);
                (
                    ImapAuth::XOAuth2 { user: p.email.clone(), access_token: access.clone() },
                    SmtpAuth::XOAuth2 { user: p.email.clone(), access_token: access.clone() },
                    AccountSecret::OAuth2 { access_token: access, refresh_token: refresh, expires_at: exp },
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
                signature: String::new(),
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
        "account.update" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let display_name = req.params.get("display_name").and_then(|v| v.as_str());
            let sync_interval = req.params.get("sync_interval_secs").and_then(|v| v.as_i64());
            let signature = req.params.get("signature").and_then(|v| v.as_str());
            let enabled = req.params.get("enabled").and_then(|v| v.as_bool());

            match db.update_account(account_id, display_name, sync_interval, signature, enabled) {
                Ok(Some(acc)) => {
                    if acc.enabled {
                        supervisor.spawn_account(acc.clone()).await;
                    } else {
                        supervisor.stop_account(acc.id);
                    }
                    RpcResponse::ok(id, json!({ "account": acc }))
                }
                Ok(None) => RpcResponse::err(id, -32004, "account not found"),
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
        "folder.create" => {
            let account_id = req.params.get("account_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let full_path = req.params.get("full_path").and_then(|v| v.as_str()).unwrap_or("");
            match imap_folder_create(account_id, full_path, db).await {
                Ok(fid) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": fid }),
                    ));
                    RpcResponse::ok(id, json!({ "folder_id": fid }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "folder.rename" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let new_path = req.params.get("new_path").and_then(|v| v.as_str()).unwrap_or("");
            match imap_folder_rename(folder_id, new_path, db).await {
                Ok(account_id) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": folder_id }),
                    ));
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "folder.delete" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64()).unwrap_or(0);
            match imap_folder_delete(folder_id, db).await {
                Ok(account_id) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": folder_id }),
                    ));
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "folder.empty" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64()).unwrap_or(0);
            match imap_folder_empty(folder_id, db).await {
                Ok(account_id) => {
                    let _ = events.send(RpcNotification::new(
                        "folder.updated",
                        json!({ "account_id": account_id, "folder_id": folder_id, "unread_count": 0, "total_count": 0 }),
                    ));
                    RpcResponse::ok(id, json!({ "ok": true }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "message.search" => {
            let folder_id = req.params.get("folder_id").and_then(|v| v.as_i64());
            let query_str = req.params.get("query").and_then(|v| v.as_str()).unwrap_or("");
            let limit = req.params.get("limit").and_then(|v| v.as_i64()).unwrap_or(100);
            let server_search = req.params.get("server_search").and_then(|v| v.as_bool()).unwrap_or(false);
            let search_id = req.params.get("search_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let parsed = parse_search_query(query_str);
            match db.search_messages_local(folder_id, &parsed, limit) {
                Ok(results) => {
                    if server_search && !query_str.trim().is_empty() {
                        let db_bg = db.clone();
                        let events_bg = events.clone();
                        let criteria = to_imap_search_criteria(&parsed).join(" ");
                        let seen_ids: std::collections::HashSet<i64> = results.iter().map(|m| m.id).collect();
                        tokio::spawn(async move {
                            run_background_imap_search(db_bg, events_bg, folder_id, criteria, search_id, seen_ids).await;
                        });
                    }
                    RpcResponse::ok(id, json!({ "messages": results, "search_id": search_id }))
                }
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
                    let _ = outbox_wake.try_send(());
                    RpcResponse::ok(id, json!({ "outbox_id": outbox_id, "status": "queued" }))
                }
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        "attachment.get" => {
            let att_id = req.params.get("attachment_id").and_then(|v| v.as_i64()).unwrap_or(0);
            match resolve_attachment(att_id, db, cache_dir).await {
                Ok((filename, mime_type, local_path)) => RpcResponse::ok(
                    id,
                    json!({
                        "attachment_id": att_id,
                        "filename": filename,
                        "mime_type": mime_type,
                        "local_path": local_path,
                    }),
                ),
                Err(e) => RpcResponse::err(id, -32000, e.to_string()),
            }
        }
        other => RpcResponse::err(id, -32601, format!("method not found: {other}")),
    }
}

async fn resolve_attachment(att_id: i64, db: &Database, cache_dir: &Path) -> Result<(String, String, String)> {
    let info = db
        .get_attachment_download_info(att_id)?
        .ok_or_else(|| anyhow::anyhow!("attachment not found"))?;

    if let Some(ref existing) = info.local_path {
        if Path::new(existing).exists() {
            return Ok((info.filename, info.mime_type, existing.clone()));
        }
    }

    let acc = db
        .get_account(info.account_id)?
        .ok_or_else(|| anyhow::anyhow!("account not found"))?;
    let folder_path = db
        .get_folder_path(info.folder_id)?
        .ok_or_else(|| anyhow::anyhow!("folder not found"))?;

    let raw_secret = load_secret(&acc.email)
        .await?
        .ok_or_else(|| anyhow::anyhow!("credentials not found in keyring"))?;
    let secret = if let Some(ref prov) = acc.oauth_provider {
        refresh_if_needed(&acc.email, prov, raw_secret).await?
    } else {
        raw_secret
    };

    let auth = match secret {
        AccountSecret::Password { password } => ImapAuth::Password { user: acc.email.clone(), password },
        AccountSecret::OAuth2 { access_token, .. } => ImapAuth::XOAuth2 { user: acc.email.clone(), access_token },
    };

    let mut session = connect_imap_tls(&acc.imap_host, acc.imap_port, &auth).await?;
    session.select(&folder_path).await?;
    let stream = session.uid_fetch(info.uid.to_string(), "(UID BODY.PEEK[])").await?;
    let fetches: Vec<_> = stream.try_collect().await?;
    let _ = session.logout().await;

    let raw = fetches
        .first()
        .and_then(|f| f.body())
        .ok_or_else(|| anyhow::anyhow!("message body not returned by IMAP"))?;

    let bytes = extract_attachment_bytes(raw, &info.part_id, &info.filename)
        .ok_or_else(|| anyhow::anyhow!("attachment part not found in MIME message"))?;

    let acc_dir = cache_dir.join("attachments").join(info.account_id.to_string());
    std::fs::create_dir_all(&acc_dir)?;
    let safe_name: String = info
        .filename
        .chars()
        .map(|c| if c == '/' || c == '\\' || c == '\0' { '_' } else { c })
        .collect();
    let dest = acc_dir.join(format!("{}_{}", info.message_id, safe_name));
    std::fs::write(&dest, &bytes)?;

    let dest_str = dest.to_string_lossy().into_owned();
    db.set_attachment_local_path(att_id, &dest_str)?;
    Ok((info.filename, info.mime_type, dest_str))
}

async fn connect_account_imap(account_id: i64, db: &Database) -> Result<crate::imap::client::ImapSession> {
    let acc = db
        .get_account(account_id)?
        .ok_or_else(|| anyhow::anyhow!("account not found"))?;
    let raw_secret = load_secret(&acc.email)
        .await?
        .ok_or_else(|| anyhow::anyhow!("credentials not found in keyring"))?;
    let secret = if let Some(ref prov) = acc.oauth_provider {
        refresh_if_needed(&acc.email, prov, raw_secret).await?
    } else {
        raw_secret
    };
    let auth = match secret {
        AccountSecret::Password { password } => ImapAuth::Password { user: acc.email.clone(), password },
        AccountSecret::OAuth2 { access_token, .. } => ImapAuth::XOAuth2 { user: acc.email.clone(), access_token },
    };
    connect_imap_tls(&acc.imap_host, acc.imap_port, &auth).await
}

async fn imap_folder_create(account_id: i64, full_path: &str, db: &Database) -> Result<i64> {
    let mut session = connect_account_imap(account_id, db).await?;
    session.create(full_path).await?;
    let _ = session.logout().await;
    let (role, prio) = classify_folder(full_path, &[]);
    let name = full_path.rsplit('/').next().unwrap_or(full_path);
    db.upsert_folder(account_id, name, full_path, "/", role.as_str(), prio)
}

async fn imap_folder_rename(folder_id: i64, new_path: &str, db: &Database) -> Result<i64> {
    let folder = db.get_folder(folder_id)?.ok_or_else(|| anyhow::anyhow!("folder not found"))?;
    let mut session = connect_account_imap(folder.account_id, db).await?;
    session.rename(&folder.full_path, new_path).await?;
    let _ = session.logout().await;
    let new_name = new_path.rsplit(&folder.delimiter).next().unwrap_or(new_path);
    db.rename_folder_row(folder_id, new_name, new_path)?;
    Ok(folder.account_id)
}

async fn imap_folder_delete(folder_id: i64, db: &Database) -> Result<i64> {
    let folder = db.get_folder(folder_id)?.ok_or_else(|| anyhow::anyhow!("folder not found"))?;
    let mut session = connect_account_imap(folder.account_id, db).await?;
    session.delete(&folder.full_path).await?;
    let _ = session.logout().await;
    db.delete_folder_row(folder_id)?;
    Ok(folder.account_id)
}

async fn imap_folder_empty(folder_id: i64, db: &Database) -> Result<i64> {
    let folder = db.get_folder(folder_id)?.ok_or_else(|| anyhow::anyhow!("folder not found"))?;
    let mut session = connect_account_imap(folder.account_id, db).await?;
    session.select(&folder.full_path).await?;
    if let Ok(s) = session.uid_store("1:*", "+FLAGS.SILENT (\\Deleted)").await {
        let _: Vec<_> = s.try_collect().await.unwrap_or_default();
    }
    let _ = session.expunge().await;
    let _ = session.logout().await;
    db.empty_folder_local(folder_id)?;
    Ok(folder.account_id)
}

async fn run_background_imap_search(
    db: Database,
    events: broadcast::Sender<RpcNotification>,
    folder_id: Option<i64>,
    criteria: String,
    search_id: i64,
    seen_ids: std::collections::HashSet<i64>,
) {
    let mut appended = Vec::new();
    let target_folders = if let Some(fid) = folder_id {
        db.get_folder(fid).ok().flatten().into_iter().collect::<Vec<_>>()
    } else {
        db.list_folders(None)
            .unwrap_or_default()
            .into_iter()
            .filter(|f| f.role == "inbox")
            .collect()
    };

    for folder in target_folders {
        let Ok(mut session) = connect_account_imap(folder.account_id, &db).await else {
            continue;
        };
        if session.select(&folder.full_path).await.is_ok() {
            if let Ok(uids) = session.uid_search(&criteria).await {
                let mut uid_vec: Vec<i64> = uids.into_iter().map(|u| u as i64).collect();
                uid_vec.sort_unstable_by(|a, b| b.cmp(a));
                uid_vec.truncate(50);
                if let Ok(found) = db.list_summaries_by_uids(folder.id, &uid_vec) {
                    for m in found {
                        if !seen_ids.contains(&m.id) {
                            appended.push(m);
                        }
                    }
                }
            }
        }
        let _ = session.logout().await;
    }

    let _ = events.send(RpcNotification::new(
        "search.results_appended",
        json!({
            "search_id": search_id,
            "messages": appended,
            "done": true,
        }),
    ));
}
