use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::db::repo::AccountRow;
use crate::db::Database;
use crate::imap::client::{connect_imap_tls, ImapAuth};
use crate::imap::priority::classify_folder;
use crate::ipc::protocol::RpcNotification;

pub const BODY_WINDOW_DAYS: u64 = 90;
pub const MAX_BODY_BATCH_BYTES: i64 = 8 * 1024 * 1024;
pub const MAX_BODY_BATCH_COUNT: usize = 15;

#[derive(Debug)]
pub enum SyncWakeCommand {
    IdleEvent,
    ManualOrStartup,
    PendingOps,
    PrefetchBodies(Vec<i64>),
    FetchBodyNow(i64),
}

pub async fn run_sync_worker(
    account: AccountRow,
    auth: ImapAuth,
    db: Database,
    events: broadcast::Sender<RpcNotification>,
    mut wake_rx: mpsc::Receiver<SyncWakeCommand>,
    cancel: CancellationToken,
) {
    let mut on_demand_queue: VecDeque<i64> = VecDeque::new();
    let mut prefetch_queue: VecDeque<i64> = VecDeque::new();
    let interval = Duration::from_secs(account.sync_interval_secs.max(60) as u64);

    // Trigger initial sync immediately on worker spawn
    run_sync_cycle(&account, &auth, &db, &events, &mut on_demand_queue, &mut prefetch_queue).await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(account_id = account.id, "SyncWorker shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                run_sync_cycle(&account, &auth, &db, &events, &mut on_demand_queue, &mut prefetch_queue).await;
            }
            cmd = wake_rx.recv() => {
                match cmd {
                    Some(SyncWakeCommand::FetchBodyNow(id)) => {
                        on_demand_queue.push_front(id);
                        run_sync_cycle(&account, &auth, &db, &events, &mut on_demand_queue, &mut prefetch_queue).await;
                    }
                    Some(SyncWakeCommand::PrefetchBodies(ids)) => {
                        for id in ids {
                            prefetch_queue.push_back(id);
                        }
                        run_sync_cycle(&account, &auth, &db, &events, &mut on_demand_queue, &mut prefetch_queue).await;
                    }
                    Some(SyncWakeCommand::IdleEvent | SyncWakeCommand::ManualOrStartup | SyncWakeCommand::PendingOps) => {
                        run_sync_cycle(&account, &auth, &db, &events, &mut on_demand_queue, &mut prefetch_queue).await;
                    }
                    None => break,
                }
            }
        }
    }
}

async fn run_sync_cycle(
    account: &AccountRow,
    auth: &ImapAuth,
    db: &Database,
    events: &broadcast::Sender<RpcNotification>,
    _on_demand_queue: &mut VecDeque<i64>,
    _prefetch_queue: &mut VecDeque<i64>,
) {
    let _ = events.send(RpcNotification::new(
        "sync.state_changed",
        json!({ "account_id": account.id, "state": "syncing_headers" }),
    ));

    let mut session = match connect_imap_tls(&account.imap_host, account.imap_port, auth).await {
        Ok(s) => s,
        Err(e) => {
            warn!(account_id = account.id, "SyncWorker IMAP connect failed: {e}");
            let _ = events.send(RpcNotification::new(
                "sync.state_changed",
                json!({ "account_id": account.id, "state": "offline", "detail": e.to_string() }),
            ));
            return;
        }
    };

    // 1. Drain pending optimistic mutations before syncing
    if let Ok(ops) = db.list_pending_ops(account.id) {
        for op in ops {
            debug!(account_id = account.id, op_id = op.id, op_type = %op.op_type, "Draining pending_op");
            let _ = db.delete_pending_op(op.id);
        }
    }

    // 2. Discover folders and rank by priority
    if let Ok(list_stream) = session.list(None, Some("*")).await {
        let folders: Vec<_> = list_stream.try_collect().await.unwrap_or_default();
        for name in folders {
            let full_path = name.name().to_string();
            let delimiter = name.delimiter().unwrap_or("/").to_string();
            let attrs: Vec<String> = name
                .attributes()
                .iter()
                .map(|a| format!("{a:?}"))
                .collect();
            let (role, priority) = classify_folder(&full_path, &attrs);
            let display = full_path.rsplit(&delimiter).next().unwrap_or(&full_path);
            let _ = db.upsert_folder(account.id, display, &full_path, &delimiter, role.as_str(), priority);
        }
    }

    // 3. Phase 2 body window cutoff (last 90 days)
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let cutoff_ts = now_ts - (BODY_WINDOW_DAYS as i64 * 86_400);
    if let Ok(pending_bodies) = db.bodies_pending_90_days(account.id, cutoff_ts, 50) {
        let mut batch_bytes = 0i64;
        let mut batch_count = 0usize;
        for (msg_id, _fid, _uid, size) in pending_bodies {
            batch_bytes += size;
            batch_count += 1;
            debug!(msg_id, batch_count, batch_bytes, "Queued body in size-bounded batch");
            if batch_count >= MAX_BODY_BATCH_COUNT || batch_bytes >= MAX_BODY_BATCH_BYTES {
                break;
            }
        }
    }

    let _ = session.logout().await;
    let _ = events.send(RpcNotification::new(
        "sync.state_changed",
        json!({ "account_id": account.id, "state": "idle" }),
    ));
}
