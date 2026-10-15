//! Per-account background worker with a priority-ordered event loop.
//!
//! IMAP sessions are stateful and async; the worker owns one session per
//! account and always processes the most important work first:
//!
//! 1. **User requests** — LoadMessages / LoadBody are served immediately.
//! 2. **Queued actions** — durable flags/move/delete retries (the action ledger).
//! 3. **Sync-failure retry** — durable header/body fetch retries.
//! 4. **Body prefetch** — recent message bodies cached so opening is instant.
//! 5. **Attachment prefetch** — attachments for older messages cached on disk.
//! 6. **Backfill** — remaining folder messages indexed in chunks.
//! 7. **IDLE** — blocks for server push; re-syncs on mailbox change.
//!
//! This replaces the separate SyncEngine + IdleRouter architecture with a
//! single task per account that never lets background work starve the user.

use crate::cache::Cache;
use crate::imap::{self, ImapSession};
use crate::secrets::Secrets;
use crate::types::{Account, ImapMessageSummary};

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

mod actions;
mod backfill;
mod handlers;
mod idle;
mod prefetch;
mod sync_failures;

/// First page of messages fetched instantly when a folder is opened for the
/// first time (cold cache) — seeds the UI's message list.
const FIRST_PAGE_SIZE: usize = 200;
/// Page size for re-opens: the cache already has most messages, so a smaller
/// fetch of the newest messages (plus merge via upsert) is enough.
const REOPEN_PAGE_SIZE: usize = 50;
/// Width (in UIDs) of each history-backfill window: the walk fetches
/// `UID FETCH lo:hi` bands descending from the persisted cursor. Bigger =
/// fewer round-trips; smaller = more responsive to interleaved user requests.
/// The same band size bounds incremental new-mail catch-up fetches.
const BACKFILL_CHUNK: usize = 1_000;
/// Fallback re-sync interval used when the worker is NOT actively IDLE-watching
/// a folder (no folder selected yet, no session, or after a disconnect). IMAP
/// IDLE pushes mailbox changes, but servers are not reliable about it — poll
/// every couple of minutes so new mail is still picked up.
const RESYNC_INTERVAL_SECS: u64 = 120;
// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// A request from the D-Bus layer (or internal scheduler) to the worker.
#[derive(Debug)]
pub enum WorkerRequest {
    /// User opened a message: fetch its body (also returns from cache).
    LoadBody {
        folder_path: String,
        uid: i64,
        reply: mpsc::UnboundedSender<String>,
    },
    /// Trigger a sync of every folder.
    SyncAll,
    /// Tell the worker to watch a specific folder: sync its first page and
    /// IDLE on it for push notifications.
    SetIdleFolder { folder_path: String },
    /// Change read/starred flags on one message (None leaves a flag unchanged).
    SetFlags {
        folder_path: String,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    },
    /// Move a message to another folder (None = move to the account's Trash).
    MoveMessage {
        folder_path: String,
        uid: i64,
        dest_folder: Option<String>,
    },
    /// Delete a message: move to Trash if not already there, otherwise permanently delete.
    DeleteMessage { folder_path: String, uid: i64 },
    /// Create a new IMAP folder.
    CreateFolder { name: String },
    /// Rename an IMAP folder.
    RenameFolder { old_name: String, new_name: String },
    /// Delete an IMAP folder.
    DeleteFolder { name: String },
    /// Graceful shutdown.
    Shutdown,
}

/// Handle used by the D-Bus layer to talk to a worker.
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

// ---------------------------------------------------------------------------
// Background jobs
// ---------------------------------------------------------------------------

/// Remaining UIDs to backfill for a folder, persisted as a cursor.
struct BackfillJob {
    folder_path: String,
    /// The folder's UIDVALIDITY when the job was planned. If the server reports
    /// a different value later the queued UIDs name different messages, so the
    /// job is dropped and re-seeded by the next sync.
    uidvalidity: i64,
    /// Top (highest UID) of the next window to fetch. `0` = complete.
    /// The window walks **downward** from here in `BACKFILL_CHUNK`-wide bands,
    /// so an interrupted run resumes without re-running `UID SEARCH ALL`.
    cursor: i64,
}
// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// Per-account background worker.
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
    /// A request deferred while we were inside an IDLE block, to be processed
    /// at the top of the priority loop.
    pending: Option<WorkerRequest>,
    /// Set by Shutdown; the event loop exits once set.
    shutdown: bool,
    /// Cached OAuth access token (in-memory only, never persisted).
    cached_access_token: Option<String>,
    /// Unix timestamp when cached_access_token expires (0 = unknown).
    token_expires_at: i64,
    /// Directory where downloaded attachment files are stored (one folder per
    /// message, files prefixed with the part index).
    attachments_dir: PathBuf,
}
impl Worker {
    pub fn new(
        account: Account,
        cache: Arc<Mutex<Cache>>,
        secrets: Secrets,
        rx: mpsc::UnboundedReceiver<WorkerRequest>,
        attachments_dir: PathBuf,
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
            cached_access_token: None,
            token_expires_at: 0,
            attachments_dir,
        }
    }

    // -----------------------------------------------------------------------
    // Priority-ordered event loop
    // -----------------------------------------------------------------------

    pub async fn run(mut self) {
        let id = self.account.id;
        tracing::info!("Worker[{}]: starting ({})", id, self.account.email);

        if let Err(e) = self.connect().await {
            tracing::error!("Worker[{}]: bootstrap connect failed: {}", id, e);
        }

        // Recover actions left in-flight by a previous run (crash/restart) so
        // they are retried rather than stranded. New actions are only enqueued
        // after this point, so nothing live is disturbed. Also report how many
        // header/body fetch retries are still queued.
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

            // ---- Step 0: drain every pending user request (non-blocking)
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

            // ---- Step 0.5: drain one due queued action (durable retry)
            // Runs before background work but after user requests, so a
            // reconnect's backlog of flags/moves/deletes clears promptly
            // without evicting interactive work.
            if self.drain_one_action().await {
                continue;
            }

            // ---- Step 0.6: retry one due header/body fetch (sync-failure
            // ledger). Also after user requests, so a transient fetch failure
            // heals in the background instead of waiting for an unrelated
            // re-sync to happen to include the message.
            if self.retry_one_sync_failure().await {
                continue;
            }

            // ---- Step 1: body prefetch (one per iteration)
            if let Some((path, uid)) = self.body_prefetch.pop_front() {
                self.run_one_body_prefetch(&path, uid).await;
                continue;
            }

            // ---- Step 2: attachment prefetch (one per iteration)
            if let Some((path, uid)) = self.attachment_prefetch.pop_front() {
                self.run_one_attachment_prefetch(&path, uid).await;
                continue;
            }

            // ---- Step 3: backfill window (one chunk per iteration)
            if let Some((path, uidvalidity, cursor)) = self
                .backfill_queue
                .front()
                .map(|job| (job.folder_path.clone(), job.uidvalidity, job.cursor))
            {
                if cursor <= 0 {
                    self.backfill_queue.pop_front();
                    continue;
                }
                // Drop a job planned under an older UIDVALIDITY: its UIDs now
                // name different messages. The next sync re-seeds the folder.
                if !self.backfill_uidvalidity_ok(&path, uidvalidity).await {
                    tracing::warn!(
                        "Worker[{}]: dropping stale backfill job for {} (UIDVALIDITY changed)",
                        self.account.id,
                        path
                    );
                    self.backfill_queue.pop_front();
                    continue;
                }
                let hi = cursor;
                let lo = (hi - BACKFILL_CHUNK as i64 + 1).max(1);
                if self.run_backfill_chunk(&path, lo, hi).await {
                    // The chunk wrote and persisted the cursor; advance the
                    // in-memory job so the next iteration picks up below it.
                    let next = lo - 1;
                    if next <= 0 {
                        self.backfill_queue.pop_front();
                    } else if let Some(job) = self.backfill_queue.front_mut() {
                        job.cursor = next;
                    }
                }
                continue;
            }

            // ---- Step 4: IDLE (blocks until push notification)
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
                        // A request arrived during IDLE; it lives on self.pending
                        // and the top of the loop processes it at priority 0.
                        IdleResult::Request => {
                            continue;
                        }
                    }
                }
            }

            // ---- Nothing higher-priority: block for the next request, but
            //      wake up periodically to re-sync. IDLE push is the happy
            //      path; servers can miss notifications, and when we cannot
            //      IDLE at all (no folder selected / no session) this timer
            //      is the only thing that keeps the mailbox fresh.
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(RESYNC_INTERVAL_SECS),
                self.rx.recv(),
            )
            .await
            {
                Ok(Some(req)) => self.pending = Some(req),
                Ok(None) => break, // channel closed — shut down
                Err(_elapsed) => {
                    self.do_periodic_resync().await;
                }
            }
        }

        tracing::info!("Worker[{}]: shut down", id);
    }
    // -----------------------------------------------------------------------
    // Network operations
    // -----------------------------------------------------------------------

    /// Connect with a fresh IMAP session and refresh the cached folder list.
    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = self.connect_session().await?;

        // Refresh the folder list so the UI's sidebar is never empty.
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

    /// Get a valid OAuth access token, refreshing if expired or about to expire.
    /// Refreshes 5 minutes before expiry to avoid race conditions.
    async fn get_valid_access_token(
        &mut self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let now = chrono::Utc::now().timestamp();
        // Refresh 5 minutes (300s) before expiry
        let buffer = 300;

        if let Some(ref token) = self.cached_access_token {
            if self.token_expires_at > 0 && now < self.token_expires_at - buffer {
                return Ok(token.clone());
            }
        }

        // Token is missing or about to expire — refresh it
        let refresh_token = self
            .account
            .refresh_token
            .as_deref()
            .ok_or("No refresh token")?;
        let provider = crate::oauth::provider_for(&self.account)?;
        let (client_id, client_secret) = crate::oauth::client_credentials(provider)?;
        let token = crate::oauth::refresh_access_token(
            provider.token_endpoint,
            &client_id,
            &client_secret,
            refresh_token,
        )
        .await?;

        // Save the new refresh token if the provider issued one (Google rotates them)
        if let Some(ref new_rt) = token.refresh_token {
            if new_rt != refresh_token {
                tracing::info!(
                    "Worker[{}]: {} issued new refresh token, saving",
                    self.account.id,
                    provider.id
                );
                let cache = self.cache.lock().await;
                if let Err(e) = cache.update_refresh_token(self.account.id, new_rt) {
                    tracing::error!(
                        "Worker[{}]: failed to save new refresh token: {}",
                        self.account.id,
                        e
                    );
                } else {
                    // Update the in-memory account so future refreshes use the new token
                    self.account.refresh_token = Some(new_rt.clone());
                }
            }
        }

        let access_token = token.access_token.clone();
        self.cached_access_token = Some(access_token.clone());
        self.token_expires_at = token.expires_at();

        tracing::debug!(
            "Worker[{}]: refreshed access token (expires in {}s)",
            self.account.id,
            self.token_expires_at - now
        );

        Ok(access_token)
    }

    async fn connect_session(
        &mut self,
    ) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
        let host = self
            .account
            .imap_host
            .clone()
            .unwrap_or_else(|| "localhost".to_string());
        let port = self.account.imap_port.unwrap_or(993) as u16;
        let email = self.account.email.clone();

        if self.account.auth_type == "oauth" {
            let access_token = self.get_valid_access_token().await?;
            imap::connect_session_oauth(&host, port, &email, &access_token).await
        } else {
            let key_name = format!("pelliper:{}", email);
            let password = self.secrets.get_password(&key_name).await?;
            imap::connect_session_password(&host, port, &email, &password).await
        }
    }

    /// Ensure we have a live session, reconnecting if needed.
    async fn ensure_session(&mut self) -> Result<(), String> {
        if self.session.is_some() {
            return Ok(());
        }
        self.connect().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Check if an error string indicates an authentication failure.
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

    /// Invalidate the cached token and drop the session so the next
    /// operation triggers a fresh connect with a new token.
    fn invalidate_token(&mut self) {
        self.cached_access_token = None;
        self.token_expires_at = 0;
        self.session = None;
    }

    /// Sync the top of a folder's UID space and queue background work for the
    /// rest. Drives the persisted per-folder cursors: a cold folder (no
    /// `last_seen_uid`) seeds with a sequence-range page and snapshots the
    /// server high-water; a warm folder asks the server only for UIDs above
    /// the high-water and fetches exactly those, in bounded chunks; with no
    /// new mail it falls back to a small newest-page refresh so flag changes
    /// still land. All writes are upserts, so overlaps merge harmlessly.
    /// Returns the summaries fetched.
    async fn sync_folder_first_page(
        &mut self,
        folder_path: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        // EXAMINE once for the whole plan: UIDVALIDITY (reset guard), EXISTS
        // and UIDNEXT (high-water rule).
        let (uidvalidity, exists, uid_next) = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::examine_folder(session, folder_path).await?
        };

        // UIDVALIDITY guard. Everything below is keyed on `(folder, uid)`, but
        // IMAP only guarantees those UIDs stay meaningful while the folder's
        // UIDVALIDITY is unchanged. When the server reports a new value it has
        // renumbered every message, so the cached rows now point at different
        // mail: drop them (and any queued work keyed on the old UIDs) before
        // syncing afresh. A 0 means the server did not report one — unknown,
        // not a change.
        if uidvalidity != 0 {
            let cached = {
                let cache = self.cache.lock().await;
                cache
                    .folder_uidvalidity(self.account.id, folder_path)
                    .unwrap_or(0)
            };
            if cached != 0 && cached != uidvalidity {
                let cleared = {
                    let cache = self.cache.lock().await;
                    cache
                        .clear_folder_messages(self.account.id, folder_path)
                        .unwrap_or(0)
                };
                tracing::warn!(
                    "Worker[{}]: UIDVALIDITY changed for {} ({} -> {}); cleared {} cached message(s)",
                    self.account.id,
                    folder_path,
                    cached,
                    uidvalidity,
                    cleared
                );
                // Forget background work planned against the old UIDs. The
                // re-sync below queues fresh work for the new numbering.
                self.body_prefetch.retain(|(p, _)| p != folder_path);
                self.body_prefetched.retain(|(p, _)| p != folder_path);
                self.attachment_prefetch.retain(|(p, _)| p != folder_path);
                self.attachment_prefetched.retain(|(p, _)| p != folder_path);
                self.backfill_queue
                    .retain(|job| job.folder_path != folder_path);
            }
            if cached != uidvalidity {
                let cache = self.cache.lock().await;
                if let Err(e) =
                    cache.set_folder_uidvalidity(self.account.id, folder_path, uidvalidity)
                {
                    tracing::error!(
                        "Worker[{}]: failed to store UIDVALIDITY for {}: {e}",
                        self.account.id,
                        folder_path
                    );
                }
            }
        }

        // High-water UID: UIDNEXT-1 when consistent with EXISTS, else SEARCH.
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

        // Front-plane fetch. Three cases:
        //  - never synced (cold/legacy): newest-page seed, then snapshot the
        //    high-water so later cycles only ask the server for UIDs above it;
        //  - warm with new mail: fetch exactly the UIDs above the high-water
        //    in bounded chunks, advancing the watermark only when all land
        //    (a post-offline catch-up cannot stall the loop forever, and a
        //    partial failure is re-checked next cycle instead of losing mail);
        //  - warm without new mail: cheap newest-page refresh for flag
        //    changes, watermark untouched.
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
            let cache = self.cache.lock().await;
            let _ = cache.set_folder_last_seen_uid(
                self.account.id,
                folder_path,
                last_seen_uid.max(high),
            );
            summaries
        } else {
            let new_uids = {
                let session = self.session.as_mut().ok_or("no session")?;
                imap::uid_search_after(session, folder_path, last_seen_uid).await?
            };
            if new_uids.is_empty() {
                self.fetch_recent_page(folder_path, REOPEN_PAGE_SIZE)
                    .await?
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
                }
                summaries
            }
        };

        // Prefetch newest-first so the newest mail wins the body window.
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

    /// Fetch a sequence-range page of summaries, reconnecting and retrying
    /// once when an OAuth token turns out to be invalid mid-session.
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

    /// Fetch summaries for an explicit UID set, retrying once on an OAuth auth
    /// error. Shared by incremental new-mail sync and history backfill windows.
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

    /// Upsert a batch of summaries (message row, autocomplete address, header
    /// block) and refresh the folder's unread count from the whole cache.
    ///
    /// Returns the `(uid, error)` of any message whose header block could not be
    /// persisted, so the caller can record it in the sync-failure ledger. The
    /// writes are best-effort and do not abort the batch: a single bad row must
    /// not discard the rest of the page.
    async fn write_summaries_to_cache(
        &self,
        folder_path: &str,
        summaries: &[ImapMessageSummary],
    ) -> Vec<(i64, String)> {
        let mut failures = Vec::new();
        let cache = self.cache.lock().await;
        for msg in summaries {
            // Record sender address for autocomplete. The From list is
            // JSON, so take its first entry.
            if let Some((email, name)) = imap::first_address(&msg.from) {
                let _ = cache.upsert_address(&email, &name);
            }
            if let Err(e) = cache.upsert_message(
                self.account.id,
                folder_path,
                msg.uid,
                msg.is_read,
                msg.is_starred,
                msg.has_attachments,
                &msg.preview,
            ) {
                tracing::error!(
                    "Worker[{}]: upsert_message {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    msg.uid
                );
                failures.push((msg.uid, e.to_string()));
                continue;
            }
            // Full header block + ENVELOPE. Must follow upsert_message:
            // the row is foreign-keyed to it, so it cannot be written
            // first. This is also what indexes the message for search.
            if let Err(e) =
                cache.save_message_headers(self.account.id, folder_path, msg.uid, &msg.headers)
            {
                tracing::error!(
                    "Worker[{}]: save_message_headers {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    msg.uid
                );
                failures.push((msg.uid, e.to_string()));
            }
        }
        // Use the whole-cache unread count, not just this batch, so re-opens
        // and IDLE re-syncs don't undercount older unread mail.
        let unread = cache
            .count_unread(self.account.id, folder_path)
            .unwrap_or(0);
        let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        failures
    }
    /// Periodic poll fired when the worker is not actively IDLE-watching a
    /// folder (no folder selected yet, or no live session). Tries to
    /// (re)establish the session and refresh the active folder — falling back
    /// to INBOX so per-account new mail is still picked up without IDLE push.
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
    }

    /// Resolve a semantic role (e.g. Trash) to a concrete folder path, using the
    /// folder list already classified and cached by `connect()`.
    ///
    /// Reading the cache rather than re-issuing `LIST` is deliberate: the cached
    /// rows have already had name-matched impostors demoted, so this returns the
    /// server's authoritative special-use folder. A fresh `LIST` would re-apply
    /// the name heuristic and pick whichever candidate the server happened to
    /// list first — so "delete" could file mail into a user's own `Trash` label
    /// instead of the real trash, where it would never leave the mailbox.
    async fn resolve_role_folder(&self, kind: crate::types::FolderKind) -> Option<String> {
        let cache = self.cache.lock().await;
        let folders = cache.load_folders(self.account.id).ok()?;
        folders
            .into_iter()
            .find(|f| !f.noselect && f.kind == kind.as_str())
            .map(|f| f.path)
    }

    /// Whether the daemon's classified folder list says this path is `kind`.
    ///
    /// Prefers the cached classification over `FolderKind::from_path` because
    /// the cached rows have had name-matched impostors demoted. Returns `false`
    /// when the folder is unknown, so callers on a destructive path default to
    /// the recoverable behaviour instead of the irreversible one.
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

// ---------------------------------------------------------------------------
// IDLE event parsing (reused from the old idle.rs)
// ---------------------------------------------------------------------------

enum IdleResult {
    MailboxChange,
    FolderSwitch(String),
    Disconnected,
    /// A request arrived during IDLE; it was stored on `self.pending`.
    Request,
}
