use crate::cache::Cache;
use crate::imap::{self, ImapSession};
use crate::secrets::Secrets;
use crate::types::{Account, DaemonSignal, SignalSender};
use async_imap::imap_proto::types::{MailboxDatum, Response};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

pub async fn run_inbox_watcher(
    account: Account,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    signal_tx: Option<SignalSender>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let id = account.id;
    tracing::info!(
        "InboxWatcher[{}]: starting dedicated INBOX push connection",
        id
    );

    let mut cached_token: Option<(String, i64)> = None;

    'outer: while !*shutdown_rx.borrow() {
        let mut session = match super::connect_account_session(
            &account,
            &secrets,
            &cache,
            &mut cached_token,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("InboxWatcher[{}]: connect failed: {e}; retrying in 10s", id);
                tokio::select! {
                    _ = shutdown_rx.changed() => break 'outer,
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => continue 'outer,
                }
            }
        };

        let supports_idle = imap::supports_idle(&mut session).await;
        tracing::debug!(
            "InboxWatcher[{}]: connected (supports_idle={})",
            id,
            supports_idle
        );

        if let Err(e) = sync_inbox_once(&account, &mut session, &cache, signal_tx.as_ref()).await {
            tracing::warn!("InboxWatcher[{}]: initial INBOX sync failed: {e}", id);
        }

        if supports_idle {
            if session.select("INBOX").await.is_err() {
                tokio::select! {
                    _ = shutdown_rx.changed() => break 'outer,
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => continue 'outer,
                }
            }

            loop {
                if *shutdown_rx.borrow() {
                    break 'outer;
                }

                let mut handle = session.idle();
                if handle.init().await.is_err() {
                    break;
                }

                let idle_timeout = tokio::time::Duration::from_secs(29 * 60);
                let idle_result = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        let _ = handle.done().await;
                        break 'outer;
                    }
                    res = tokio::time::timeout(idle_timeout, async {
                        loop {
                            match tokio::time::timeout(tokio::time::Duration::from_millis(500), handle.next()).await {
                                Ok(Some(Ok(resp))) => {
                                    if idle_event_from_response(resp.parsed()) {
                                        return true;
                                    }
                                }
                                Ok(Some(Err(_))) | Ok(None) => return false,
                                Err(_) => {}
                            }
                        }
                    }) => res,
                };

                session = match handle.done().await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                match idle_result {
                    Ok(true) => {
                        if let Err(e) =
                            sync_inbox_once(&account, &mut session, &cache, signal_tx.as_ref())
                                .await
                        {
                            tracing::warn!(
                                "InboxWatcher[{}]: post-IDLE INBOX sync failed: {e}",
                                id
                            );
                            break;
                        }
                    }
                    Ok(false) => {
                        break;
                    }
                    Err(_) => {}
                }
            }
        } else {
            loop {
                if *shutdown_rx.borrow() {
                    break 'outer;
                }
                tokio::select! {
                    _ = shutdown_rx.changed() => break 'outer,
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {}
                }
                if let Err(e) =
                    sync_inbox_once(&account, &mut session, &cache, signal_tx.as_ref()).await
                {
                    tracing::warn!("InboxWatcher[{}]: periodic INBOX sync failed: {e}", id);
                    break;
                }
            }
        }

        tokio::select! {
            _ = shutdown_rx.changed() => break 'outer,
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
        }
    }

    tracing::info!("InboxWatcher[{}]: stopped", id);
}

fn idle_event_from_response(resp: &Response<'_>) -> bool {
    matches!(
        resp,
        Response::Expunge(_)
            | Response::Fetch(..)
            | Response::MailboxData(MailboxDatum::Exists(_))
            | Response::MailboxData(MailboxDatum::Flags(_))
    )
}

async fn sync_inbox_once(
    account: &Account,
    session: &mut ImapSession,
    cache: &Arc<Mutex<Cache>>,
    signal_tx: Option<&SignalSender>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (uidvalidity, exists, uid_next) = imap::examine_folder(session, "INBOX").await?;

    if exists == 0 {
        let (to_delete, has_pending) = {
            let c = cache.lock().await;
            let cached = c
                .cached_uids_in_range(account.id, "INBOX", 1, i64::MAX)
                .unwrap_or_default();
            let pending = c
                .pending_action_uids(account.id, "INBOX")
                .unwrap_or_default();
            let to_del: Vec<i64> = cached
                .into_iter()
                .filter(|u| !pending.contains(u))
                .collect();
            (to_del, !pending.is_empty())
        };
        if !to_delete.is_empty() {
            let mut c = cache.lock().await;
            let _ = c.delete_messages(account.id, "INBOX", &to_delete);
            let _ = c.update_folder_unread(account.id, "INBOX", 0);
            if let Some(tx) = signal_tx {
                let _ = tx.send(DaemonSignal::FolderUnreadChanged {
                    account_id: account.id,
                    folder_path: "INBOX".to_string(),
                    unread_count: 0,
                });
            }
        }
        if !has_pending {
            let c = cache.lock().await;
            let _ = c.set_folder_last_seen_uid(account.id, "INBOX", 0);
            let _ = c.set_folder_backfill_cursor(account.id, "INBOX", Some(0));
        }
        return Ok(());
    }

    if uidvalidity != 0 {
        let cached = {
            let c = cache.lock().await;
            c.folder_uidvalidity(account.id, "INBOX").unwrap_or(0)
        };
        if cached != 0 && cached != uidvalidity {
            let c = cache.lock().await;
            let _ = c.clear_folder_messages(account.id, "INBOX");
        }
        if cached != uidvalidity {
            let c = cache.lock().await;
            let _ = c.set_folder_uidvalidity(account.id, "INBOX", uidvalidity);
        }
    }

    let high = imap::mailbox_highest_uid(session, "INBOX", exists, uid_next).await?;
    let (last_seen_uid, _) = {
        let c = cache.lock().await;
        c.folder_sync_state(account.id, "INBOX")
            .unwrap_or((0, None))
    };

    if last_seen_uid == 0 {
        let summaries =
            imap::fetch_recent_summaries(session, "INBOX", super::FIRST_PAGE_SIZE).await?;
        super::write_summaries_to_cache_impl(account.id, "INBOX", &summaries, cache).await;
        {
            let c = cache.lock().await;
            let _ = c.set_folder_last_seen_uid(account.id, "INBOX", high);
            let unread = c.count_unread(account.id, "INBOX").unwrap_or(0);
            let _ = c.update_folder_unread(account.id, "INBOX", unread);
            if let Some(tx) = signal_tx {
                if !summaries.is_empty() {
                    let _ = tx.send(DaemonSignal::NewMailReceived {
                        account_id: account.id,
                        folder_path: "INBOX".to_string(),
                        count: summaries.len() as i32,
                    });
                }
                let _ = tx.send(DaemonSignal::FolderUnreadChanged {
                    account_id: account.id,
                    folder_path: "INBOX".to_string(),
                    unread_count: unread,
                });
            }
        }
    } else {
        let hi = last_seen_uid.max(high);
        let lo = (hi.saturating_sub(super::RECONCILE_WINDOW as i64) + 1).max(1);
        let _ =
            super::reconcile_window_impl(account.id, "INBOX", lo, hi, session, cache, signal_tx)
                .await;

        if high < last_seen_uid {
            let c = cache.lock().await;
            let _ = c.set_folder_last_seen_uid(account.id, "INBOX", high);
        }

        let new_uids = imap::uid_search_after(session, "INBOX", last_seen_uid).await?;
        if !new_uids.is_empty() {
            let mut landed_count = 0;
            let mut all_landed = true;
            for window in new_uids.chunks(super::BACKFILL_CHUNK) {
                let uid_set: Vec<String> = window.iter().map(|u| u.to_string()).collect();
                match imap::fetch_summaries_by_uids(session, "INBOX", &uid_set.join(",")).await {
                    Ok(part) => {
                        landed_count += part.len();
                        super::write_summaries_to_cache_impl(account.id, "INBOX", &part, cache)
                            .await;
                    }
                    Err(e) => {
                        tracing::error!("InboxWatcher[{}]: fetch new mail failed: {e}", account.id);
                        all_landed = false;
                        break;
                    }
                }
            }
            if all_landed {
                let next = new_uids
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0)
                    .max(last_seen_uid);
                let c = cache.lock().await;
                let _ = c.set_folder_last_seen_uid(account.id, "INBOX", next);
            }
            if landed_count > 0 {
                let c = cache.lock().await;
                let unread = c.count_unread(account.id, "INBOX").unwrap_or(0);
                let _ = c.update_folder_unread(account.id, "INBOX", unread);
                if let Some(tx) = signal_tx {
                    let _ = tx.send(DaemonSignal::NewMailReceived {
                        account_id: account.id,
                        folder_path: "INBOX".to_string(),
                        count: landed_count as i32,
                    });
                    let _ = tx.send(DaemonSignal::FolderUnreadChanged {
                        account_id: account.id,
                        folder_path: "INBOX".to_string(),
                        unread_count: unread,
                    });
                }
            }
        }
    }

    Ok(())
}
