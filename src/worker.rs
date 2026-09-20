//! Per-account background worker with a priority-ordered event loop.
//!
//! IMAP sessions are stateful and async; the worker owns one session per
//! account and always processes the most important work first:
//!
//! 1. **User requests** — LoadMessages / LoadBody are served immediately.
//! 2. **Body prefetch** — recent message bodies cached so opening is instant.
//! 3. **Attachment prefetch** — attachments for older messages cached on disk.
//! 4. **Backfill** — remaining folder messages indexed in chunks.
//! 5. **IDLE** — blocks for server push; re-syncs on mailbox change.
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

mod backfill;
mod handlers;
mod idle;
mod prefetch;

/// First page of messages fetched instantly when a folder is opened for the
/// first time (cold cache) — seeds the UI's message list.
const FIRST_PAGE_SIZE: usize = 200;
/// Page size for re-opens: the cache already has most messages, so a smaller
/// fetch of the newest messages (plus merge via upsert) is enough.
const REOPEN_PAGE_SIZE: usize = 50;
/// How many messages to fetch per backfill round-trip. Bigger = fewer
/// round-trips; smaller = more responsive to interleaved user requests.
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

/// Remaining UIDs to backfill for a folder.
struct BackfillJob {
    folder_path: String,
    remaining: Vec<i64>,
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

        // If the cache is cold we must bootstrap the folder list now;
        // otherwise the UI renders from cache while we connect lazily. Also
        // bootstrap when messages survive but the folder table is empty: the
        // sidebar would render empty and SyncAll would iterate zero folders.
        if !self.has_cached_messages().await || self.cached_folders_missing().await {
            if let Err(e) = self.connect().await {
                tracing::error!("Worker[{}]: bootstrap connect failed: {}", id, e);
            }
        }

        'outer: loop {
            if self.shutdown {
                break;
            }

            // ---- Priority 0: drain every pending user request (non-blocking)
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

            // ---- Priority 1: body prefetch (one per iteration)
            if let Some((path, uid)) = self.body_prefetch.pop_front() {
                self.run_one_body_prefetch(&path, uid).await;
                continue;
            }

            // ---- Priority 2: attachment prefetch (one per iteration)
            if let Some((path, uid)) = self.attachment_prefetch.pop_front() {
                self.run_one_attachment_prefetch(&path, uid).await;
                continue;
            }

            // ---- Priority 3: backfill chunk (one chunk per iteration)
            if let Some(job) = self.backfill_queue.front_mut() {
                let n = BACKFILL_CHUNK.min(job.remaining.len());
                if n > 0 {
                    let chunk: Vec<i64> = job.remaining.drain(..n).collect();
                    let path = job.folder_path.clone();
                    self.run_backfill_chunk(&path, &chunk).await;
                    continue;
                } else {
                    self.backfill_queue.pop_front();
                    continue;
                }
            }

            // ---- Priority 4: IDLE (blocks until push notification)
            if self.session.is_some() {
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
        let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID")
            .map_err(|_| "PELLIPER_GOOGLE_CLIENT_ID not set (see ~/.local/share/pelliper/.env)")?;
        let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();
        let token = crate::oauth::refresh_access_token(
            "https://oauth2.googleapis.com/token",
            &client_id,
            &client_secret,
            refresh_token,
        )
        .await?;

        // Save new refresh token if Google issued one (rotation)
        if let Some(ref new_rt) = token.refresh_token {
            if new_rt != refresh_token {
                tracing::info!(
                    "Worker[{}]: Google issued new refresh token, saving",
                    self.account.id
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

    /// Fetch the most recent page of a folder's messages, cache the results,
    /// and queue background work for the remainder.  The page size adapts to
    /// the cache: `FIRST_PAGE_SIZE` on first open (cold folder), a smaller
    /// `REOPEN_PAGE_SIZE` when messages are already cached (refresh).  Because
    /// messages are upserted, re-fetching a page merges new/updated mail while
    /// leaving older cached messages intact.  Returns the summaries.
    async fn sync_folder_first_page(
        &mut self,
        folder_path: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;

        // First open (no cached messages in this folder) gets a bigger page;
        // re-opens get a lighter refresh.
        let cached = {
            let cache = self.cache.lock().await;
            cache
                .count_messages(self.account.id, folder_path)
                .unwrap_or(0)
        };
        let page_size = if cached == 0 {
            FIRST_PAGE_SIZE
        } else {
            REOPEN_PAGE_SIZE
        };

        let summaries = imap::fetch_recent_summaries(session, folder_path, page_size).await;

        let summaries = match summaries {
            Ok(s) => s,
            Err(e) if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" => {
                tracing::warn!(
                    "Worker[{}]: auth error in sync_folder_first_page, refreshing token",
                    self.account.id
                );
                self.invalidate_token();
                self.connect()
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
                let session = self.session.as_mut().ok_or("no session after reconnect")?;
                imap::fetch_recent_summaries(session, folder_path, page_size).await?
            }
            Err(e) => return Err(e),
        };

        {
            let cache = self.cache.lock().await;
            for msg in &summaries {
                let _ = cache.upsert_message(
                    self.account.id,
                    folder_path,
                    msg.uid,
                    &msg.subject,
                    &msg.sender,
                    msg.date,
                    msg.is_read,
                    msg.is_starred,
                    msg.has_attachments,
                    &msg.preview,
                    &msg.message_id,
                    &msg.references,
                );
                // Record sender address for autocomplete.
                if let Some((email, name)) = parse_address(&msg.sender) {
                    let _ = cache.upsert_address(&email, &name);
                }
            }
            // Use the whole-cache unread count, not just this page, so re-opens
            // and IDLE re-syncs don't undercount older unread mail.
            let unread = cache
                .count_unread(self.account.id, folder_path)
                .unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }

        // Queue background work for this folder.
        self.queue_body_prefetch(folder_path, &summaries).await;
        self.queue_attachment_prefetch(folder_path).await;
        self.queue_backfill_for_folder(folder_path).await;

        tracing::info!(
            "Worker[{}]: synced {} recent messages for {} (page size {})",
            self.account.id,
            summaries.len(),
            folder_path,
            page_size
        );

        Ok(summaries)
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
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn has_cached_messages(&self) -> bool {
        let cache = self.cache.lock().await;
        cache.has_cached_messages(self.account.id).unwrap_or(false)
    }

    /// True when the cache holds messages for this account but no folder rows —
    /// a broken state (empty sidebar, SyncAll iterates nothing) that a
    /// bootstrap `connect()` repairs by re-listing folders from the server.
    async fn cached_folders_missing(&self) -> bool {
        let cache = self.cache.lock().await;
        match cache.load_folders(self.account.id) {
            Ok(folders) => folders.is_empty(),
            Err(_) => true,
        }
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

/// Parse an IMAP sender string like "John Doe <john@example.com>" into
/// (email, display_name). Returns None if the string is empty or unparseable.
fn parse_address(sender: &str) -> Option<(String, String)> {
    let s = sender.trim();
    if s.is_empty() {
        return None;
    }
    if let Some(start) = s.find('<') {
        if let Some(end) = s.find('>') {
            let email = s[start + 1..end].trim().to_string();
            let name = s[..start].trim().trim_matches('"').to_string();
            if !email.is_empty() {
                return Some((email, name));
            }
        }
    }
    // Bare email address (no angle brackets).
    if s.contains('@') && !s.contains(' ') {
        return Some((s.to_string(), String::new()));
    }
    None
}
