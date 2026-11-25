use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_imap::types::Flag;
use futures::TryStreamExt;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::db::repo::{AccountRow, FolderRow, NewHeaderRow};
use crate::db::Database;
use crate::imap::client::{connect_imap_tls, ImapAuth, ImapSession};
use crate::imap::priority::classify_folder;
use crate::ipc::protocol::RpcNotification;
use crate::mime::{compute_thread_id, parse_message_body};

pub const BODY_WINDOW_DAYS: u64 = 90;
pub const HEADER_BATCH_SIZE: usize = 200;
pub const MAX_BODY_BATCH_BYTES: i64 = 8 * 1024 * 1024;
pub const MAX_BODY_BATCH_COUNT: usize = 15;

#[derive(Debug)]
pub enum SyncWakeCommand {
    IdleEvent,
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

    run_sync_cycle(&account, &auth, &db, &events, &mut wake_rx, &mut on_demand_queue, &mut prefetch_queue).await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(account_id = account.id, "SyncWorker shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                run_sync_cycle(&account, &auth, &db, &events, &mut wake_rx, &mut on_demand_queue, &mut prefetch_queue).await;
            }
            cmd = wake_rx.recv() => {
                match cmd {
                    Some(SyncWakeCommand::FetchBodyNow(id)) => {
                        on_demand_queue.push_front(id);
                        run_sync_cycle(&account, &auth, &db, &events, &mut wake_rx, &mut on_demand_queue, &mut prefetch_queue).await;
                    }
                    Some(SyncWakeCommand::PrefetchBodies(ids)) => {
                        for id in ids {
                            if !prefetch_queue.contains(&id) {
                                prefetch_queue.push_back(id);
                            }
                        }
                        run_sync_cycle(&account, &auth, &db, &events, &mut wake_rx, &mut on_demand_queue, &mut prefetch_queue).await;
                    }
                    Some(SyncWakeCommand::IdleEvent | SyncWakeCommand::PendingOps) => {
                        run_sync_cycle(&account, &auth, &db, &events, &mut wake_rx, &mut on_demand_queue, &mut prefetch_queue).await;
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
    wake_rx: &mut mpsc::Receiver<SyncWakeCommand>,
    on_demand_queue: &mut VecDeque<i64>,
    prefetch_queue: &mut VecDeque<i64>,
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
    drain_pending_ops(&mut session, account.id, db).await;

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

    // 3. Phase 1: Header sync in priority order
    if let Ok(folders) = db.list_folders(Some(account.id)) {
        for folder in &folders {
            drain_wake_into_queues(wake_rx, on_demand_queue, prefetch_queue);
            drain_pending_ops(&mut session, account.id, db).await;
            sync_folder_headers(&mut session, account.id, folder, db, events).await;
        }
    }

    // 4. Phase 2: Body sync (on-demand -> scroll prefetch -> 90-day backfill)
    let _ = events.send(RpcNotification::new(
        "sync.state_changed",
        json!({ "account_id": account.id, "state": "syncing_bodies" }),
    ));

    // Process on-demand & scroll-prefetch first
    sync_queued_bodies(&mut session, db, events, on_demand_queue, 1).await;
    sync_queued_bodies(&mut session, db, events, prefetch_queue, MAX_BODY_BATCH_COUNT).await;

    // 90-day background backfill in size-bounded batches
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let cutoff_ts = now_ts - (BODY_WINDOW_DAYS as i64 * 86_400);

    loop {
        drain_wake_into_queues(wake_rx, on_demand_queue, prefetch_queue);
        drain_pending_ops(&mut session, account.id, db).await;
        if !on_demand_queue.is_empty() {
            sync_queued_bodies(&mut session, db, events, on_demand_queue, 1).await;
        }
        if !prefetch_queue.is_empty() {
            sync_queued_bodies(&mut session, db, events, prefetch_queue, MAX_BODY_BATCH_COUNT).await;
        }

        let pending = db.bodies_pending_90_days(account.id, cutoff_ts, 50).unwrap_or_default();
        if pending.is_empty() {
            break;
        }

        // Group by folder_id up to MAX_BODY_BATCH_COUNT / MAX_BODY_BATCH_BYTES
        let target_folder = pending[0].1;
        let mut batch_ids = Vec::new();
        let mut batch_uids = Vec::new();
        let mut batch_bytes = 0i64;
        for (msg_id, fid, uid, size) in pending {
            if fid != target_folder {
                continue;
            }
            if !batch_ids.is_empty()
                && (batch_ids.len() >= MAX_BODY_BATCH_COUNT || batch_bytes + size > MAX_BODY_BATCH_BYTES)
            {
                break;
            }
            batch_bytes += size;
            batch_ids.push((uid, msg_id));
            batch_uids.push(uid.to_string());
        }

        if batch_uids.is_empty() {
            break;
        }
        fetch_and_save_bodies(&mut session, db, events, target_folder, &batch_ids, &batch_uids).await;
    }

    let _ = session.logout().await;
    let _ = events.send(RpcNotification::new(
        "sync.state_changed",
        json!({ "account_id": account.id, "state": "idle" }),
    ));
}

fn drain_wake_into_queues(
    wake_rx: &mut mpsc::Receiver<SyncWakeCommand>,
    on_demand: &mut VecDeque<i64>,
    prefetch: &mut VecDeque<i64>,
) {
    while let Ok(cmd) = wake_rx.try_recv() {
        match cmd {
            SyncWakeCommand::FetchBodyNow(id) => on_demand.push_front(id),
            SyncWakeCommand::PrefetchBodies(ids) => {
                for id in ids {
                    if !prefetch.contains(&id) {
                        prefetch.push_back(id);
                    }
                }
            }
            SyncWakeCommand::IdleEvent | SyncWakeCommand::PendingOps => {}
        }
    }
}

async fn drain_pending_ops(session: &mut ImapSession, account_id: i64, db: &Database) {
    let ops = db.list_pending_ops(account_id).unwrap_or_default();
    for op in ops {
        let Some(fid) = op.folder_id else {
            let _ = db.delete_pending_op(op.id);
            continue;
        };
        let Ok(Some(folder_path)) = db.get_folder_path(fid) else {
            let _ = db.delete_pending_op(op.id);
            continue;
        };
        if session.select(&folder_path).await.is_err() {
            continue;
        }

        let ok = match op.op_type.as_str() {
            "mark_all_read" => {
                if let Ok(s) = session.uid_store("1:*", "+FLAGS.SILENT (\\Seen)").await {
                    let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                    true
                } else {
                    false
                }
            }
            "set_flags" => {
                let uid_str = op.uid.unwrap_or(0).to_string();
                let payload: Value = serde_json::from_str(&op.payload_json).unwrap_or_default();
                let add_mask = payload.get("add_mask").and_then(|v| v.as_i64()).unwrap_or(0);
                let rem_mask = payload.get("remove_mask").and_then(|v| v.as_i64()).unwrap_or(0);
                let mut success = true;
                if let Some(flags) = mask_to_imap_flags(add_mask) {
                    if let Ok(s) = session.uid_store(&uid_str, format!("+FLAGS.SILENT ({flags})")).await {
                        let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                    } else {
                        success = false;
                    }
                }
                if let Some(flags) = mask_to_imap_flags(rem_mask) {
                    if let Ok(s) = session.uid_store(&uid_str, format!("-FLAGS.SILENT ({flags})")).await {
                        let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                    } else {
                        success = false;
                    }
                }
                success
            }
            "move_role" => {
                let uid_str = op.uid.unwrap_or(0).to_string();
                let payload: Value = serde_json::from_str(&op.payload_json).unwrap_or_default();
                let role = payload.get("target_role").and_then(|v| v.as_str()).unwrap_or("trash");
                if let Ok(Some(target_path)) = db.find_folder_path_by_role(account_id, role) {
                    if target_path != folder_path {
                        if session.uid_mv(&uid_str, &target_path).await.is_ok() {
                            true
                        } else if session.uid_copy(&uid_str, &target_path).await.is_ok() {
                            if let Ok(s) = session.uid_store(&uid_str, "+FLAGS.SILENT (\\Deleted)").await {
                                let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                            }
                            if let Ok(s) = session.uid_expunge(&uid_str).await {
                                let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        if let Ok(s) = session.uid_store(&uid_str, "+FLAGS.SILENT (\\Deleted)").await {
                            let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                        }
                        if let Ok(s) = session.uid_expunge(&uid_str).await {
                            let _: Vec<_> = s.try_collect().await.unwrap_or_default();
                        }
                        true
                    }
                } else {
                    true
                }
            }
            _ => true,
        };

        if ok {
            let _ = db.delete_pending_op(op.id);
        }
    }
}

async fn sync_folder_headers(
    session: &mut ImapSession,
    account_id: i64,
    folder: &FolderRow,
    db: &Database,
    events: &broadcast::Sender<RpcNotification>,
) {
    let mailbox = match session.select(&folder.full_path).await {
        Ok(m) => m,
        Err(e) => {
            debug!(folder = %folder.full_path, "SELECT failed: {e}");
            return;
        }
    };

    if let Some(uv) = mailbox.uid_validity {
        let uv_i64 = uv as i64;
        if folder.uid_validity.is_some() && folder.uid_validity != Some(uv_i64) {
            let _ = db.clear_folder_messages(folder.id);
        }
        let _ = db.update_folder_uid_validity(folder.id, uv_i64);
    }

    let search_query = format!("UID {}:*", folder.uid_next.max(1));
    if let Ok(uids) = session.uid_search(&search_query).await {
        let mut sorted: Vec<u32> = uids.into_iter().filter(|&u| u as i64 >= folder.uid_next).collect();
        sorted.sort_unstable();

        for chunk in sorted.chunks(HEADER_BATCH_SIZE) {
            let set = chunk.iter().map(|u| u.to_string()).collect::<Vec<_>>().join(",");
            if let Ok(stream) = session
                .uid_fetch(&set, "(UID FLAGS INTERNALDATE RFC822.SIZE ENVELOPE)")
                .await
            {
                let fetches: Vec<_> = stream.try_collect().await.unwrap_or_default();
                let mut rows = Vec::with_capacity(fetches.len());
                let mut max_uid = 0i64;

                for f in fetches {
                    let Some(uid) = f.uid else { continue };
                    let uid_i64 = uid as i64;
                    max_uid = max_uid.max(uid_i64);
                    let flags_bitmask = flags_to_bitmask(f.flags());
                    let size_bytes = f.size.unwrap_or(0) as i64;
                    let internal_date_ts = f
                        .internal_date()
                        .map(|d| d.timestamp())
                        .unwrap_or(0);

                    let (subject, from_json, to_json, cc_json, msg_id, in_reply_to) =
                        if let Some(env) = f.envelope() {
                            (
                                env.subject
                                    .as_ref()
                                    .map(|b| String::from_utf8_lossy(b).into_owned())
                                    .unwrap_or_default(),
                                encode_addrs(env.from.as_deref()),
                                encode_addrs(env.to.as_deref()),
                                encode_addrs(env.cc.as_deref()),
                                env.message_id
                                    .as_ref()
                                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                                env.in_reply_to
                                    .as_ref()
                                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                            )
                        } else {
                            (String::new(), "[]".into(), "[]".into(), "[]".into(), None, None)
                        };

                    let thread_id = compute_thread_id(
                        msg_id.as_deref(),
                        in_reply_to.as_deref(),
                        None,
                        &subject,
                    );

                    rows.push(NewHeaderRow {
                        account_id,
                        folder_id: folder.id,
                        uid: uid_i64,
                        message_id: msg_id,
                        in_reply_to,
                        references_header: None,
                        thread_id,
                        subject,
                        from_json,
                        to_json,
                        cc_json,
                        date_ts: internal_date_ts,
                        internal_date_ts,
                        size_bytes,
                        flags_bitmask,
                    });
                }

                if let Ok(inserted) = db.upsert_message_headers_batch(&rows, folder.id, max_uid) {
                    if !inserted.is_empty() {
                        let _ = events.send(RpcNotification::new(
                            "message.new",
                            json!({ "account_id": account_id, "folder_id": folder.id, "summaries": inserted }),
                        ));
                    }
                }
            }
        }
    }

    // Reconcile flags for existing messages
    if let Ok(stream) = session.uid_fetch("1:*", "(UID FLAGS)").await {
        let fetches: Vec<_> = stream.try_collect().await.unwrap_or_default();
        let pairs: Vec<(i64, i64)> = fetches
            .into_iter()
            .filter_map(|f| f.uid.map(|u| (u as i64, flags_to_bitmask(f.flags()))))
            .collect();
        let _ = db.reconcile_folder_flags(folder.id, &pairs);
    }

    if let Ok(updated_folders) = db.list_folders(Some(account_id)) {
        if let Some(uf) = updated_folders.into_iter().find(|f| f.id == folder.id) {
            let _ = events.send(RpcNotification::new(
                "folder.updated",
                json!({
                    "account_id": account_id,
                    "folder_id": uf.id,
                    "unread_count": uf.unread_count,
                    "total_count": uf.total_count
                }),
            ));
        }
    }
}

async fn sync_queued_bodies(
    session: &mut ImapSession,
    db: &Database,
    events: &broadcast::Sender<RpcNotification>,
    queue: &mut VecDeque<i64>,
    max_per_call: usize,
) {
    let mut by_folder: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
    let mut taken = 0usize;
    while taken < max_per_call {
        let Some(msg_id) = queue.pop_front() else {
            break;
        };
        if let Ok(Some((fid, uid, _))) = db.get_message_uid_folder(msg_id) {
            by_folder.entry(fid).or_default().push((uid, msg_id));
            taken += 1;
        }
    }

    for (fid, pairs) in by_folder {
        let uids: Vec<String> = pairs.iter().map(|(u, _)| u.to_string()).collect();
        fetch_and_save_bodies(session, db, events, fid, &pairs, &uids).await;
    }
}

async fn fetch_and_save_bodies(
    session: &mut ImapSession,
    db: &Database,
    events: &broadcast::Sender<RpcNotification>,
    folder_id: i64,
    uid_msg_pairs: &[(i64, i64)],
    uids: &[String],
) {
    let Ok(Some(folder_path)) = db.get_folder_path(folder_id) else {
        return;
    };
    if session.select(&folder_path).await.is_err() {
        return;
    }

    let uid_to_msg: HashMap<i64, i64> = uid_msg_pairs.iter().copied().collect();
    let set = uids.join(",");
    let Ok(stream) = session.uid_fetch(&set, "(UID BODY.PEEK[])").await else {
        return;
    };
    let fetches: Vec<_> = stream.try_collect().await.unwrap_or_default();
    let mut parsed_items = Vec::new();

    for f in fetches {
        let Some(uid) = f.uid else { continue };
        let Some(&msg_id) = uid_to_msg.get(&(uid as i64)) else { continue };
        let Some(raw) = f.body() else { continue };
        if let Some(parsed) = parse_message_body(raw) {
            parsed_items.push((msg_id, parsed));
        }
    }

    if let Ok(ready) = db.save_parsed_bodies_batch(&parsed_items) {
        for (msg_id, snippet) in ready {
            let _ = events.send(RpcNotification::new(
                "message.body_ready",
                json!({ "message_id": msg_id, "snippet": snippet }),
            ));
        }
    }
}

fn flags_to_bitmask<'a>(flags: impl Iterator<Item = Flag<'a>>) -> i64 {
    let mut mask = 0i64;
    for flag in flags {
        match flag {
            Flag::Seen => mask |= 1,
            Flag::Flagged => mask |= 2,
            Flag::Answered => mask |= 4,
            Flag::Draft => mask |= 8,
            Flag::Deleted => mask |= 16,
            _ => {}
        }
    }
    mask
}

fn mask_to_imap_flags(mask: i64) -> Option<String> {
    let mut list = Vec::new();
    if mask & 1 != 0 { list.push("\\Seen"); }
    if mask & 2 != 0 { list.push("\\Flagged"); }
    if mask & 4 != 0 { list.push("\\Answered"); }
    if mask & 8 != 0 { list.push("\\Draft"); }
    if mask & 16 != 0 { list.push("\\Deleted"); }
    if list.is_empty() { None } else { Some(list.join(" ")) }
}

fn encode_addrs(addrs: Option<&[async_imap::imap_proto::Address<'_>]>) -> String {
    let Some(list) = addrs else {
        return "[]".to_string();
    };
    let items: Vec<Value> = list
        .iter()
        .map(|a| {
            let name = a
                .name
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            let mailbox = a
                .mailbox
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            let host = a
                .host
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            let address = if host.is_empty() {
                mailbox
            } else {
                format!("{mailbox}@{host}")
            };
            json!({ "name": name, "address": address })
        })
        .collect();
    serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
}
