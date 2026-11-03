use crate::cache::Cache;
use crate::imap::{self, ImapSession};
use crate::secrets::Secrets;
use crate::types::{Account, DaemonSignal, ImapMessageSummary};

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

mod actions;
mod backfill;
mod handlers;
mod idle;
mod inbox_watcher;
mod prefetch;
mod sync_failures;

const FIRST_PAGE_SIZE: usize = 200;

const REOPEN_PAGE_SIZE: usize = 50;

const BACKFILL_CHUNK: usize = 1_000;

const RECONCILE_WINDOW: usize = 1_000;

const RESYNC_INTERVAL_SECS: u64 = 120;

#[derive(Debug)]
pub enum WorkerRequest {
    LoadBody {
        folder_path: String,
        uid: i64,
        reply: mpsc::UnboundedSender<String>,
    },

    SyncAll,

    SetIdleFolder {
        folder_path: String,
    },

    SetFlags {
        folder_path: String,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    },

    MoveMessage {
        folder_path: String,
        uid: i64,
        dest_folder: Option<String>,
    },

    DeleteMessage {
        folder_path: String,
        uid: i64,
    },

    CreateFolder {
        name: String,
    },

    RenameFolder {
        old_name: String,
        new_name: String,
    },

    DeleteFolder {
        name: String,
    },

    Shutdown,
}

#[derive(Clone)]
pub struct WorkerHandle {
    tx: mpsc::UnboundedSender<WorkerRequest>,
}

impl WorkerHandle {
    pub fn new(tx: mpsc::UnboundedSender<WorkerRequest>) -> Self {
        Self { tx }
    }

    pub fn send(&self, req: WorkerRequest) {
        let _ = self.tx.send(req);
    }
}

struct BackfillJob {
    folder_path: String,

    uidvalidity: i64,

    cursor: i64,
}

pub struct Worker {
    account: Account,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    rx: mpsc::UnboundedReceiver<WorkerRequest>,

    session: Option<ImapSession>,
    current_folder: Option<String>,
    supports_idle: bool,

    body_prefetch: VecDeque<(String, i64)>,
    body_prefetched: HashSet<(String, i64)>,
    attachment_prefetch: VecDeque<(String, i64)>,
    attachment_prefetched: HashSet<(String, i64)>,
    backfill_queue: VecDeque<BackfillJob>,

    pending: Option<WorkerRequest>,

    shutdown: bool,

    cached_token: Option<(String, i64)>,

    attachments_dir: PathBuf,

    signal_tx: Option<mpsc::UnboundedSender<DaemonSignal>>,
}
impl Worker {
    pub fn new(
        account: Account,
        cache: Arc<Mutex<Cache>>,
        secrets: Secrets,
        rx: mpsc::UnboundedReceiver<WorkerRequest>,
        attachments_dir: PathBuf,
        signal_tx: Option<mpsc::UnboundedSender<DaemonSignal>>,
    ) -> Self {
        Self {
            account,
            cache,
            secrets,
            rx,
            session: None,
            current_folder: None,
            supports_idle: false,
            body_prefetch: VecDeque::new(),
            body_prefetched: HashSet::new(),
            attachment_prefetch: VecDeque::new(),
            attachment_prefetched: HashSet::new(),
            backfill_queue: VecDeque::new(),
            pending: None,
            shutdown: false,
            cached_token: None,
            attachments_dir,
            signal_tx,
        }
    }

    pub(super) fn emit_signal(&self, signal: DaemonSignal) {
        if let Some(ref tx) = self.signal_tx {
            let _ = tx.send(signal);
        }
    }

    pub async fn run(mut self) {
        let id = self.account.id;
        tracing::info!("Worker[{}]: starting ({})", id, self.account.email);

        if let Err(e) = self.connect().await {
            tracing::error!("Worker[{}]: bootstrap connect failed: {}", id, e);
        }

        let (inbox_shutdown_tx, inbox_shutdown_rx) = tokio::sync::watch::channel(false);
        let watcher_account = self.account.clone();
        let watcher_cache = self.cache.clone();
        let watcher_secrets = self.secrets.clone();
        let watcher_signal_tx = self.signal_tx.clone();

        tokio::spawn(inbox_watcher::run_inbox_watcher(
            watcher_account,
            watcher_cache,
            watcher_secrets,
            watcher_signal_tx,
            inbox_shutdown_rx,
        ));

        {
            let cache = self.cache.lock().await;
            match cache.reset_in_flight_actions(self.account.id) {
                Ok(n) if n > 0 => tracing::info!(
                    "Worker[{}]: recovered {} in-flight action(s) from a previous run",
                    id,
                    n
                ),
                _ => {}
            }
            match cache.count_sync_failures(self.account.id) {
                Ok(n) if n > 0 => tracing::info!(
                    "Worker[{}]: {} failed header/body fetch(es) queued for retry",
                    id,
                    n
                ),
                _ => {}
            }
        }

        'outer: loop {
            if self.shutdown {
                break;
            }

            if let Some(req) = self.pending.take() {
                self.handle_request(req).await;
            }
            loop {
                match self.rx.try_recv() {
                    Ok(req) => {
                        self.handle_request(req).await;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        break 'outer;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                }
            }

            if self.drain_one_action().await {
                continue;
            }

            if self.retry_one_sync_failure().await {
                continue;
            }

            if let Some((path, uid)) = self.body_prefetch.pop_front() {
                self.run_one_body_prefetch(&path, uid).await;
                continue;
            }

            if let Some((path, uid)) = self.attachment_prefetch.pop_front() {
                self.run_one_attachment_prefetch(&path, uid).await;
                continue;
            }

            if let Some((path, uidvalidity, cursor)) = self
                .backfill_queue
                .front()
                .map(|job| (job.folder_path.clone(), job.uidvalidity, job.cursor))
            {
                if cursor <= 0 {
                    self.backfill_queue.pop_front();
                    self.maybe_queue_next_backfill_folder().await;
                    continue;
                }

                if !self.backfill_uidvalidity_ok(&path, uidvalidity).await {
                    tracing::warn!(
                        "Worker[{}]: dropping stale backfill job for {} (UIDVALIDITY changed)",
                        self.account.id,
                        path
                    );
                    self.backfill_queue.pop_front();
                    self.maybe_queue_next_backfill_folder().await;
                    continue;
                }
                let hi = cursor;
                let lo = (hi - BACKFILL_CHUNK as i64 + 1).max(1);
                if self.run_backfill_chunk(&path, lo, hi).await {
                    let next = lo - 1;
                    if next <= 0 {
                        self.backfill_queue.pop_front();
                        self.maybe_queue_next_backfill_folder().await;
                    } else if let Some(job) = self.backfill_queue.front_mut() {
                        job.cursor = next;
                    }
                }
                continue;
            }

            if self.backfill_queue.is_empty() {
                self.maybe_queue_next_backfill_folder().await;
                if !self.backfill_queue.is_empty() {
                    continue;
                }
            }

            if self.session.is_some() && self.supports_idle {
                if let Some(folder) = self.current_folder.clone() {
                    match self.idle_wait(&folder).await {
                        IdleResult::MailboxChange => {
                            self.on_mailbox_change(&folder).await;
                            continue;
                        }
                        IdleResult::FolderSwitch(new_folder) => {
                            self.on_folder_switch(new_folder).await;
                            continue;
                        }
                        IdleResult::Disconnected => {
                            self.session = None;
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }

                        IdleResult::Request => {
                            continue;
                        }
                    }
                }
            }

            match tokio::time::timeout(
                tokio::time::Duration::from_secs(RESYNC_INTERVAL_SECS),
                self.rx.recv(),
            )
            .await
            {
                Ok(Some(req)) => self.pending = Some(req),
                Ok(None) => break,
                Err(_elapsed) => {
                    self.do_periodic_resync().await;
                }
            }
        }

        let _ = inbox_shutdown_tx.send(true);
        tracing::info!("Worker[{}]: shut down", id);
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = self.connect_session().await?;

        match imap::list_folders(&mut session).await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache.upsert_folder(
                        self.account.id,
                        &folder.path,
                        0,
                        folder.noselect,
                        folder.kind.as_str(),
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }

        self.supports_idle = imap::supports_idle(&mut session).await;
        self.session = Some(session);
        Ok(())
    }

    async fn connect_session(
        &mut self,
    ) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
        connect_account_session(
            &self.account,
            &self.secrets,
            &self.cache,
            &mut self.cached_token,
        )
        .await
    }

    async fn ensure_session(&mut self) -> Result<(), String> {
        if self.session.is_some() {
            return Ok(());
        }
        self.connect().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    fn is_auth_error(e: &str) -> bool {
        let lower = e.to_lowercase();
        lower.contains("auth")
            && (lower.contains("fail")
                || lower.contains("invalid")
                || lower.contains("expired")
                || lower.contains("rejected"))
            || lower.contains("no longer valid")
            || lower.contains("invalid credentials")
    }

    fn invalidate_token(&mut self) {
        self.cached_token = None;
        self.session = None;
    }

    async fn sync_folder_first_page(
        &mut self,
        folder_path: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        let (uidvalidity, exists, uid_next) = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::examine_folder(session, folder_path).await?
        };

        if exists == 0 {
            let (to_delete, has_pending) = {
                let cache = self.cache.lock().await;
                let cached = cache
                    .cached_uids_in_range(self.account.id, folder_path, 1, i64::MAX)
                    .unwrap_or_default();
                let pending = cache
                    .pending_action_uids(self.account.id, folder_path)
                    .unwrap_or_default();
                let to_delete: Vec<i64> = cached
                    .into_iter()
                    .filter(|u| !pending.contains(u))
                    .collect();
                (to_delete, !pending.is_empty())
            };
            if !to_delete.is_empty() {
                let mut cache = self.cache.lock().await;
                let _ = cache.delete_messages(self.account.id, folder_path, &to_delete);
                let _ = cache.update_folder_unread(self.account.id, folder_path, 0);
                self.emit_signal(DaemonSignal::FolderUnreadChanged {
                    account_id: self.account.id,
                    folder_path: folder_path.to_string(),
                    unread_count: 0,
                });
                tracing::info!(
                    "Worker[{}]: folder {} is empty on server; expunged {} cached message(s)",
                    self.account.id,
                    folder_path,
                    to_delete.len()
                );
            }
            if !has_pending {
                let cache = self.cache.lock().await;
                let _ = cache.set_folder_last_seen_uid(self.account.id, folder_path, 0);
                let _ = cache.set_folder_backfill_cursor(self.account.id, folder_path, Some(0));
            }
            return Ok(Vec::new());
        }

        if uidvalidity != 0 {
            let cached = {
                let cache = self.cache.lock().await;
                cache
                    .folder_uidvalidity(self.account.id, folder_path)
                    .unwrap_or(0)
            };
            if cached != 0 && cached != uidvalidity {
                self.handle_uidvalidity_change(folder_path, uidvalidity)
                    .await;
            } else if cached != uidvalidity {
                let cache = self.cache.lock().await;
                let _ = cache.set_folder_uidvalidity(self.account.id, folder_path, uidvalidity);
            }
        }

        let high = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::mailbox_highest_uid(session, folder_path, exists, uid_next).await?
        };

        let (last_seen_uid, _backfill_cursor) = {
            let cache = self.cache.lock().await;
            cache
                .folder_sync_state(self.account.id, folder_path)
                .unwrap_or((0, None))
        };

        let summaries = if last_seen_uid == 0 {
            let cached_count = {
                let cache = self.cache.lock().await;
                cache
                    .count_messages(self.account.id, folder_path)
                    .unwrap_or(0)
            };
            let page_size = if cached_count == 0 {
                FIRST_PAGE_SIZE
            } else {
                REOPEN_PAGE_SIZE
            };
            let summaries = self.fetch_recent_page(folder_path, page_size).await?;
            let failures = self.write_summaries_to_cache(folder_path, &summaries).await;
            self.note_header_write_failures(folder_path, failures).await;
            let cache = self.cache.lock().await;
            let _ = cache.set_folder_last_seen_uid(
                self.account.id,
                folder_path,
                last_seen_uid.max(high),
            );
            if !summaries.is_empty() {
                self.emit_signal(DaemonSignal::NewMailReceived {
                    account_id: self.account.id,
                    folder_path: folder_path.to_string(),
                    count: summaries.len() as i32,
                });
            }
            summaries
        } else {
            let hi = last_seen_uid.max(high);
            let lo = (hi.saturating_sub(RECONCILE_WINDOW as i64) + 1).max(1);
            if let Err(e) = self.reconcile_window(folder_path, lo, hi).await {
                tracing::warn!(
                    "Worker[{}]: active window reconciliation failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
            }
            if high < last_seen_uid {
                let cache = self.cache.lock().await;
                let _ = cache.set_folder_last_seen_uid(self.account.id, folder_path, high);
            }

            let new_uids = {
                let session = self.session.as_mut().ok_or("no session")?;
                imap::uid_search_after(session, folder_path, last_seen_uid).await?
            };
            if new_uids.is_empty() {
                let summaries = self
                    .fetch_recent_page(folder_path, REOPEN_PAGE_SIZE)
                    .await?;
                let failures = self.write_summaries_to_cache(folder_path, &summaries).await;
                self.note_header_write_failures(folder_path, failures).await;
                summaries
            } else {
                let mut summaries = Vec::new();
                let mut all_landed = true;
                for window in new_uids.chunks(BACKFILL_CHUNK) {
                    let uid_set: Vec<String> = window.iter().map(|u| u.to_string()).collect();
                    match self.fetch_uid_set(folder_path, &uid_set.join(",")).await {
                        Ok(part) => {
                            let failures = self.write_summaries_to_cache(folder_path, &part).await;
                            self.note_header_write_failures(folder_path, failures).await;
                            summaries.extend(part);
                        }
                        Err(e) => {
                            tracing::error!(
                                "Worker[{}]: new-mail fetch for {} failed mid-window: {}",
                                self.account.id,
                                folder_path,
                                e
                            );
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
                    let cache = self.cache.lock().await;
                    let _ = cache.set_folder_last_seen_uid(self.account.id, folder_path, next);
                    if !summaries.is_empty() {
                        self.emit_signal(DaemonSignal::NewMailReceived {
                            account_id: self.account.id,
                            folder_path: folder_path.to_string(),
                            count: summaries.len() as i32,
                        });
                    }
                }
                summaries
            }
        };

        let mut newest_first = summaries;
        newest_first.sort_by_key(|a| std::cmp::Reverse(a.headers.date_ts));
        self.queue_body_prefetch(folder_path, &newest_first).await;
        self.queue_attachment_prefetch(folder_path).await;
        self.queue_backfill_for_folder(folder_path).await;

        tracing::info!(
            "Worker[{}]: synced {} recent messages for {}",
            self.account.id,
            newest_first.len(),
            folder_path
        );

        Ok(newest_first)
    }

    async fn fetch_recent_page(
        &mut self,
        folder_path: &str,
        page_size: usize,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let first = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::fetch_recent_summaries(session, folder_path, page_size).await
        };
        match first {
            Ok(s) => Ok(s),
            Err(e) if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" => {
                tracing::warn!(
                    "Worker[{}]: auth error fetching {} page, refreshing token",
                    self.account.id,
                    folder_path
                );
                self.invalidate_token();
                self.connect().await?;
                let session = self.session.as_mut().ok_or("no session after reconnect")?;
                imap::fetch_recent_summaries(session, folder_path, page_size).await
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_uid_set(
        &mut self,
        folder_path: &str,
        uid_set: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let first = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::fetch_summaries_by_uids(session, folder_path, uid_set).await
        };
        match first {
            Ok(s) => Ok(s),
            Err(e) if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" => {
                tracing::warn!(
                    "Worker[{}]: auth error fetching {} UID set, refreshing token",
                    self.account.id,
                    folder_path
                );
                self.invalidate_token();
                self.connect().await?;
                let session = self.session.as_mut().ok_or("no session after reconnect")?;
                imap::fetch_summaries_by_uids(session, folder_path, uid_set).await
            }
            Err(e) => Err(e),
        }
    }

    pub(super) async fn reconcile_window(
        &mut self,
        folder_path: &str,
        lo: i64,
        hi: i64,
    ) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;
        let (to_delete, deleted, flags_len) = reconcile_window_impl(
            self.account.id,
            folder_path,
            lo,
            hi,
            session,
            &self.cache,
            self.signal_tx.as_ref(),
        )
        .await?;

        if !to_delete.is_empty() {
            let del_set: HashSet<i64> = to_delete.into_iter().collect();
            self.body_prefetch
                .retain(|(p, u)| !(p == folder_path && del_set.contains(u)));
            self.body_prefetched
                .retain(|(p, u)| !(p == folder_path && del_set.contains(u)));
            self.attachment_prefetch
                .retain(|(p, u)| !(p == folder_path && del_set.contains(u)));
            self.attachment_prefetched
                .retain(|(p, u)| !(p == folder_path && del_set.contains(u)));
        }

        Ok((deleted, flags_len))
    }

    async fn write_summaries_to_cache(
        &self,
        folder_path: &str,
        summaries: &[ImapMessageSummary],
    ) -> Vec<(i64, String)> {
        let failures =
            write_summaries_to_cache_impl(self.account.id, folder_path, summaries, &self.cache)
                .await;
        let cache = self.cache.lock().await;
        let unread = cache
            .count_unread(self.account.id, folder_path)
            .unwrap_or(0);
        let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        self.emit_signal(DaemonSignal::FolderUnreadChanged {
            account_id: self.account.id,
            folder_path: folder_path.to_string(),
            unread_count: unread,
        });
        failures
    }

    async fn do_periodic_resync(&mut self) {
        let id = self.account.id;
        if self.session.is_none() {
            if let Err(e) = self.ensure_session().await {
                tracing::warn!("Worker[{}]: periodic reconnect failed: {}", id, e);
                return;
            }
        }
        let folder = self
            .current_folder
            .clone()
            .unwrap_or_else(|| "INBOX".to_string());
        match self.sync_folder_first_page(&folder).await {
            Ok(_summaries) => {}
            Err(e) => {
                tracing::warn!(
                    "Worker[{}]: periodic resync of {} failed: {}",
                    id,
                    folder,
                    e
                );
                self.session = None;
            }
        }
        self.poll_background_folders_status().await;
    }

    async fn poll_background_folders_status(&mut self) {
        let folders = {
            let cache = self.cache.lock().await;
            cache.load_folders(self.account.id).unwrap_or_default()
        };

        let current = self.current_folder.clone();
        for folder in folders {
            if folder.noselect {
                continue;
            }
            if folder.path.eq_ignore_ascii_case("INBOX") {
                continue;
            }
            if current.as_deref() == Some(&folder.path) {
                continue;
            }

            let session = match self.session.as_mut() {
                Some(s) => s,
                None => break,
            };

            match imap::folder_status(session, &folder.path).await {
                Ok((uidvalidity, exists, uid_next, unseen)) => {
                    let (cached_last_seen, _) = {
                        let cache = self.cache.lock().await;
                        cache
                            .folder_sync_state(self.account.id, &folder.path)
                            .unwrap_or((0, None))
                    };
                    let cached_uv = {
                        let cache = self.cache.lock().await;
                        cache
                            .folder_uidvalidity(self.account.id, &folder.path)
                            .unwrap_or(0)
                    };

                    if uidvalidity != 0 && cached_uv != 0 && uidvalidity != cached_uv {
                        self.handle_uidvalidity_change(&folder.path, uidvalidity)
                            .await;
                    }

                    let should_sync = match uid_next {
                        Some(next) => i64::from(next) > cached_last_seen + 1,
                        None => exists > 0 && cached_last_seen == 0,
                    };

                    if should_sync {
                        tracing::info!(
                            "Worker[{}]: background folder {} advanced; syncing",
                            self.account.id,
                            folder.path
                        );
                        let _ = self.sync_folder_first_page(&folder.path).await;
                    } else if let Some(unseen_count) = unseen {
                        let unread_i32 = unseen_count as i32;
                        if folder.unread_count != unread_i32 {
                            let cache = self.cache.lock().await;
                            let _ = cache.update_folder_unread(
                                self.account.id,
                                &folder.path,
                                unread_i32,
                            );
                            self.emit_signal(DaemonSignal::FolderUnreadChanged {
                                account_id: self.account.id,
                                folder_path: folder.path.clone(),
                                unread_count: unread_i32,
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "Worker[{}]: STATUS on {} failed: {}",
                        self.account.id,
                        folder.path,
                        e
                    );
                }
            }
        }
    }

    async fn handle_uidvalidity_change(&mut self, folder_path: &str, uidvalidity: i64) {
        let cleared = {
            let cache = self.cache.lock().await;
            cache
                .clear_folder_messages(self.account.id, folder_path)
                .unwrap_or(0)
        };
        tracing::warn!(
            "Worker[{}]: UIDVALIDITY changed for {} to {}; cleared {} cached message(s)",
            self.account.id,
            folder_path,
            uidvalidity,
            cleared
        );

        self.body_prefetch.retain(|(p, _)| p != folder_path);
        self.body_prefetched.retain(|(p, _)| p != folder_path);
        self.attachment_prefetch.retain(|(p, _)| p != folder_path);
        self.attachment_prefetched.retain(|(p, _)| p != folder_path);
        self.backfill_queue
            .retain(|job| job.folder_path != folder_path);

        let cache = self.cache.lock().await;
        let _ = cache.set_folder_uidvalidity(self.account.id, folder_path, uidvalidity);
    }

    async fn resolve_role_folder(&self, kind: crate::types::FolderKind) -> Option<String> {
        let cache = self.cache.lock().await;
        let folders = cache.load_folders(self.account.id).ok()?;
        folders
            .into_iter()
            .find(|f| !f.noselect && f.kind == kind.as_str())
            .map(|f| f.path)
    }

    async fn is_role_folder(&self, path: &str, kind: crate::types::FolderKind) -> bool {
        let cache = self.cache.lock().await;
        cache
            .load_folders(self.account.id)
            .ok()
            .map(|folders| {
                folders
                    .iter()
                    .any(|f| f.path == path && f.kind == kind.as_str())
            })
            .unwrap_or(false)
    }
}

enum IdleResult {
    MailboxChange,
    FolderSwitch(String),
    Disconnected,

    Request,
}

pub(crate) async fn get_valid_access_token_impl(
    account: &Account,
    cache: &Arc<Mutex<Cache>>,
    cached_token: &mut Option<(String, i64)>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().timestamp();
    let buffer = 300;

    if let Some((ref token, expires_at)) = cached_token {
        if *expires_at > 0 && now < *expires_at - buffer {
            return Ok(token.clone());
        }
    }

    let refresh_token = {
        let c = cache.lock().await;
        c.load_account(account.id)?
            .and_then(|a| a.refresh_token)
            .or_else(|| account.refresh_token.clone())
    }
    .ok_or("No refresh token")?;

    let provider = crate::oauth::provider_for(account)?;
    let (client_id, client_secret) = crate::oauth::client_credentials(provider)?;
    let token = crate::oauth::refresh_access_token(
        provider.token_endpoint,
        &client_id,
        &client_secret,
        &refresh_token,
    )
    .await?;

    if let Some(ref new_rt) = token.refresh_token {
        if new_rt != &refresh_token {
            tracing::info!(
                "Worker[{}]: {} issued new refresh token, saving",
                account.id,
                provider.id
            );
            let c = cache.lock().await;
            if let Err(e) = c.update_refresh_token(account.id, new_rt) {
                tracing::error!(
                    "Worker[{}]: failed to save new refresh token: {}",
                    account.id,
                    e
                );
            }
        }
    }

    let access_token = token.access_token.clone();
    *cached_token = Some((access_token.clone(), token.expires_at()));

    tracing::debug!(
        "Worker[{}]: refreshed access token (expires in {}s)",
        account.id,
        token.expires_at() - now
    );

    Ok(access_token)
}

pub(crate) async fn connect_account_session(
    account: &Account,
    secrets: &Secrets,
    cache: &Arc<Mutex<Cache>>,
    cached_token: &mut Option<(String, i64)>,
) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let host = account
        .imap_host
        .clone()
        .unwrap_or_else(|| "localhost".to_string());
    let port = account.imap_port.unwrap_or(993) as u16;
    let email = account.email.clone();

    if account.auth_type == "oauth" {
        let access_token = get_valid_access_token_impl(account, cache, cached_token).await?;
        imap::connect_session_oauth(&host, port, &email, &access_token).await
    } else {
        let key_name = format!("pelliper:{}", email);
        let password = secrets.get_password(&key_name).await?;
        imap::connect_session_password(&host, port, &email, &password).await
    }
}

pub(crate) async fn write_summaries_to_cache_impl(
    account_id: i64,
    folder_path: &str,
    summaries: &[ImapMessageSummary],
    cache: &Arc<Mutex<Cache>>,
) -> Vec<(i64, String)> {
    let mut failures = Vec::new();
    let cache = cache.lock().await;
    for msg in summaries {
        if let Some((email, name)) = imap::first_address(&msg.from) {
            let _ = cache.upsert_address(&email, &name);
        }
        if let Err(e) = cache.upsert_message(
            account_id,
            folder_path,
            msg.uid,
            msg.is_read,
            msg.is_starred,
            msg.has_attachments,
            &msg.preview,
        ) {
            tracing::error!(
                "Worker[{}]: upsert_message {}/{} failed: {e}",
                account_id,
                folder_path,
                msg.uid
            );
            failures.push((msg.uid, e.to_string()));
            continue;
        }

        if let Err(e) = cache.save_message_headers(account_id, folder_path, msg.uid, &msg.headers) {
            tracing::error!(
                "Worker[{}]: save_message_headers {}/{} failed: {e}",
                account_id,
                folder_path,
                msg.uid
            );
            failures.push((msg.uid, e.to_string()));
        }
    }
    failures
}

pub(crate) async fn reconcile_window_impl(
    account_id: i64,
    folder_path: &str,
    lo: i64,
    hi: i64,
    session: &mut ImapSession,
    cache: &Arc<Mutex<Cache>>,
    signal_tx: Option<&mpsc::UnboundedSender<DaemonSignal>>,
) -> Result<(Vec<i64>, usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    if lo > hi || hi <= 0 {
        return Ok((Vec::new(), 0, 0));
    }
    let uid_set = format!("{lo}:{hi}");
    let server_flags = imap::fetch_uid_flags(session, folder_path, &uid_set).await?;
    let server_uids: HashSet<i64> = server_flags.iter().map(|&(u, _, _)| u).collect();

    let (to_delete, flags_to_update) = {
        let cache = cache.lock().await;
        let cached = cache.cached_uids_in_range(account_id, folder_path, lo, hi)?;
        let pending = cache.pending_action_uids(account_id, folder_path)?;

        let to_delete: Vec<i64> = cached
            .into_iter()
            .filter(|u| !server_uids.contains(u) && !pending.contains(u))
            .collect();

        let flags_to_update: Vec<(i64, bool, bool)> = server_flags
            .into_iter()
            .filter(|(u, _, _)| !pending.contains(u))
            .collect();

        (to_delete, flags_to_update)
    };

    let mut deleted = 0;
    let mut changed_flags = Vec::new();

    if !to_delete.is_empty() || !flags_to_update.is_empty() {
        let mut cache = cache.lock().await;
        if !to_delete.is_empty() {
            deleted = cache.delete_messages(account_id, folder_path, &to_delete)?;
            if deleted > 0 {
                tracing::info!(
                    "Worker[{}]: expunged {} message(s) in {} (window {uid_set})",
                    account_id,
                    deleted,
                    folder_path
                );
            }
        }
        if !flags_to_update.is_empty() {
            changed_flags =
                cache.update_message_flags_bulk(account_id, folder_path, &flags_to_update)?;
        }
        if deleted > 0 || !changed_flags.is_empty() {
            let unread = cache.count_unread(account_id, folder_path).unwrap_or(0);
            let _ = cache.update_folder_unread(account_id, folder_path, unread);
            if let Some(tx) = signal_tx {
                let _ = tx.send(DaemonSignal::FolderUnreadChanged {
                    account_id,
                    folder_path: folder_path.to_string(),
                    unread_count: unread,
                });
            }
        }
    }

    if let Some(tx) = signal_tx {
        for &(uid, is_read, is_starred) in &changed_flags {
            let _ = tx.send(DaemonSignal::MessageFlagsChanged {
                account_id,
                folder_path: folder_path.to_string(),
                uid,
                is_read,
                is_starred,
            });
        }
    }

    Ok((to_delete, deleted, changed_flags.len()))
}
