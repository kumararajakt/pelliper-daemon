//! Per-account background worker with a priority-ordered event loop.
//!
//! IMAP sessions are stateful and async; the worker owns one session per
//! account and always processes the most important work first:
//!
//! 1. **User requests** — LoadMessages / LoadBody are served immediately.
//! 2. **Body prefetch** — recent message bodies cached so opening is instant.
//! 3. **Backfill** — remaining folder messages indexed in chunks.
//! 4. **IDLE** — blocks for server push; re-syncs on mailbox change.
//!
//! This replaces the separate SyncEngine + IdleRouter architecture with a
//! single task per account that never lets background work starve the user.

use crate::cache::{Account, Cache};
use crate::imap::{self, ImapMessageSummary, RawImapSession};
use crate::secrets::Secrets;

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// First page of messages fetched instantly when a folder is opened for the
/// first time (cold cache) — seeds the UI's message list.
const FIRST_PAGE_SIZE: usize = 200;

/// Page size for re-opens: the cache already has most messages, so a smaller
/// fetch of the newest messages (plus merge via upsert) is enough.
const REOPEN_PAGE_SIZE: usize = 50;

/// How many messages to fetch per backfill round-trip. Bigger = fewer
/// round-trips; smaller = more responsive to interleaved user requests.
const BACKFILL_CHUNK: usize = 1_000;

/// Pre-download bodies for this many of the most recent messages so opening
/// them is instant.  Older messages load on demand.
const PREFETCH_BODY_LIMIT: usize = 50;

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
    SetIdleFolder {
        folder_path: String,
    },
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

    session: Option<RawImapSession>,
    current_folder: Option<String>,

    body_prefetch: VecDeque<(String, i64)>,
    body_prefetched: HashSet<(String, i64)>,
    backfill_queue: VecDeque<BackfillJob>,
    /// A request deferred while we were inside an IDLE block, to be processed
    /// at the top of the priority loop.
    pending: Option<WorkerRequest>,
    /// Set by Shutdown; the event loop exits once set.
    shutdown: bool,
}

impl Worker {
    pub fn new(
        account: Account,
        cache: Arc<Mutex<Cache>>,
        secrets: Secrets,
        rx: mpsc::UnboundedReceiver<WorkerRequest>,
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
            backfill_queue: VecDeque::new(),
            pending: None,
            shutdown: false,
        }
    }

    // -----------------------------------------------------------------------
    // Priority-ordered event loop
    // -----------------------------------------------------------------------

    pub async fn run(mut self) {
        let id = self.account.id;
        tracing::info!(
            "Worker[{}]: starting ({})",
            id,
            self.account.email
        );

        // If the cache is cold we must bootstrap the folder list now;
        // otherwise the UI renders from cache while we connect lazily.
        if !self.has_cached_messages().await {
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

            // ---- Priority 2: backfill chunk (one chunk per iteration)
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

            // ---- Priority 3: IDLE (blocks until push notification)
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

            // ---- Nothing higher-priority: block for the next request.
            match self.rx.recv().await {
                Some(req) => self.pending = Some(req),
                None => break, // channel closed — shut down
            }
        }

        tracing::info!("Worker[{}]: shut down", id);
    }

    // -----------------------------------------------------------------------
    // Request handling
    // -----------------------------------------------------------------------

    async fn handle_request(&mut self, req: WorkerRequest) {
        match req {
            WorkerRequest::LoadBody {
                folder_path,
                uid,
                reply,
            } => {
                let html = self.handle_load_body(&folder_path, uid).await;
                let _ = reply.send(html);
            }
            WorkerRequest::SyncAll => {
                self.handle_sync_all().await;
            }
            WorkerRequest::SetIdleFolder { folder_path } => {
                self.on_folder_switch(folder_path).await;
            }
            WorkerRequest::Shutdown => {
                self.shutdown = true;
                if let Some(mut s) = self.session.take() {
                    s.logout().await;
                }
            }
        }
    }

    async fn handle_load_body(&mut self, folder_path: &str, uid: i64) -> String {
        // Check cache first.
        {
            let cache = self.cache.lock().await;
            if let Ok(Some(html)) = cache.load_body(self.account.id, folder_path, uid) {
                return html;
            }
        }

        // Fetch from IMAP (retry once with a fresh connection).
        match self.fetch_single_body(folder_path, uid).await {
            Ok((html, has_attachments)) => {
                let cache = self.cache.lock().await;
                let _ = cache.save_body(self.account.id, folder_path, uid, &html);
                let _ =
                    cache.update_has_attachment(self.account.id, folder_path, uid, has_attachments);
                html
            }
            Err(_e) => {
                // Retry once with a fresh connection.
                self.session = None;
                match self.fetch_single_body(folder_path, uid).await {
                    Ok((html, has_attachments)) => {
                        let cache = self.cache.lock().await;
                        let _ = cache.save_body(self.account.id, folder_path, uid, &html);
                        let _ = cache.update_has_attachment(
                            self.account.id, folder_path, uid, has_attachments,
                        );
                        html
                    }
                    Err(e) => format!("<p>Failed to load message: {}</p>", e),
                }
            }
        }
    }

    async fn handle_sync_all(&mut self) {
        let folders = {
            let cache = self.cache.lock().await;
            cache.load_folders(self.account.id).unwrap_or_default()
        };

        for folder in &folders {
            if folder.noselect {
                continue;
            }
            match self.sync_folder_first_page(&folder.path).await {
                Ok(_summaries) => {}
                Err(e) => {
                    tracing::error!(
                        "Worker[{}]: sync folder {} failed: {}",
                        self.account.id,
                        folder.path,
                        e
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Network operations
    // -----------------------------------------------------------------------

    /// Connect with a fresh IMAP session and refresh the cached folder list.
    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = self.connect_session().await?;

        // Refresh the folder list so the UI's sidebar is never empty.
        match session.list_folders().await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache
                        .upsert_folder(self.account.id, &folder.path, 0, folder.noselect);
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }

        self.session = Some(session);
        Ok(())
    }

    async fn connect_session(
        &self,
    ) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
        let host = self.account.imap_host.as_deref().unwrap_or("localhost");
        let port = self.account.imap_port.unwrap_or(993) as u16;
        let email = &self.account.email;

        if self.account.auth_type == "oauth" {
            let refresh_token = self
                .account
                .refresh_token
                .as_deref()
                .ok_or("No refresh token")?;
            let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID").unwrap_or_else(|_| {
                "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com"
                    .to_string()
            });
            let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();
            let token = crate::oauth::refresh_access_token(
                "https://oauth2.googleapis.com/token",
                &client_id,
                &client_secret,
                refresh_token,
            )
            .await?;
            imap::connect_session_oauth(host, port, email, &token.access_token).await
        } else {
            let key_name = format!("pelliper:{}", email);
            let password = self.secrets.get_password(&key_name).await?;
            imap::connect_session_password_raw(host, port, email, &password).await
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
        self.ensure_session().await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;

        // First open (no cached messages in this folder) gets a bigger page;
        // re-opens get a lighter refresh.
        let cached = {
            let cache = self.cache.lock().await;
            cache.count_messages(self.account.id, folder_path).unwrap_or(0)
        };
        let page_size = if cached == 0 { FIRST_PAGE_SIZE } else { REOPEN_PAGE_SIZE };

        let summaries = session
            .fetch_recent_summaries(folder_path, page_size)
            .await?;

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
            }
            // Use the whole-cache unread count, not just this page, so re-opens
            // and IDLE re-syncs don't undercount older unread mail.
            let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }

        // Queue background work for this folder.
        self.queue_body_prefetch(folder_path, &summaries).await;
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

    /// Handle an IDLE mailbox-change: fast re-sync the active folder.
    async fn on_mailbox_change(&mut self, folder_path: &str) {
        match self.sync_folder_first_page(folder_path).await {
            Ok(_summaries) => {
                tracing::info!(
                    "Worker[{}]: re-synced {} after IDLE",
                    self.account.id,
                    folder_path
                );
            }
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: IDLE re-sync failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                self.session = None;
            }
        }
    }

    /// Switch to a new IDLE folder, discarding stale background queues, and
    /// sync its first page so the UI's message list populates immediately.
    async fn on_folder_switch(&mut self, folder_path: String) {
        tracing::info!(
            "Worker[{}]: switching to folder {}",
            self.account.id,
            folder_path
        );
        self.current_folder = Some(folder_path.clone());
        self.body_prefetch.clear();
        self.backfill_queue.clear();

        match self.sync_folder_first_page(&folder_path).await {
            Ok(_summaries) => {
                tracing::info!(
                    "Worker[{}]: first page synced for {}",
                    self.account.id,
                    folder_path
                );
            }
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: failed to sync {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                self.session = None;
            }
        }
    }

    /// Fetch a single message body from IMAP.
    async fn fetch_single_body(
        &mut self,
        folder_path: &str,
        uid: i64,
    ) -> Result<(String, bool), Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session().await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;

        session.select_folder(folder_path).await?;
        let raw = session.fetch_body(uid).await?;

        let parsed = imap::parse_message_body(&raw);
        let html = if parsed.html.is_empty() {
            format!("<pre>{}</pre>", parsed.text)
        } else {
            parsed.html
        };

        Ok((html, parsed.has_attachments))
    }

    // -----------------------------------------------------------------------
    // Background work: body prefetch
    // -----------------------------------------------------------------------

    async fn queue_body_prefetch(
        &mut self,
        folder_path: &str,
        summaries: &[ImapMessageSummary],
    ) {
        let cache = self.cache.lock().await;
        for msg in summaries.iter().take(PREFETCH_BODY_LIMIT) {
            let key = (folder_path.to_string(), msg.uid);
            if self.body_prefetched.contains(&key) {
                continue;
            }
            match cache.has_body(self.account.id, folder_path, msg.uid) {
                Ok(true) => {
                    self.body_prefetched.insert(key);
                }
                _ => {
                    self.body_prefetch.push_back(key.clone());
                    self.body_prefetched.insert(key);
                }
            }
        }
    }

    async fn run_one_body_prefetch(&mut self, folder_path: &str, uid: i64) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };

        if session.select_folder(folder_path).await.is_err() {
            self.session = None;
            return;
        }

        match session.fetch_body(uid).await {
            Ok(raw) => {
                let parsed = imap::parse_message_body(&raw);
                let html = if parsed.html.is_empty() {
                    format!("<pre>{}</pre>", parsed.text)
                } else {
                    parsed.html
                };
                let cache = self.cache.lock().await;
                let _ = cache.save_body(self.account.id, folder_path, uid, &html);
                let _ = cache.update_has_attachment(
                    self.account.id,
                    folder_path,
                    uid,
                    parsed.has_attachments,
                );
                tracing::debug!(
                    "Worker[{}]: prefetched body for {} uid={}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Worker[{}]: body prefetch failed for uid={}: {}",
                    self.account.id,
                    uid,
                    e
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Background work: backfill
    // -----------------------------------------------------------------------

    async fn queue_backfill_for_folder(&mut self, folder_path: &str) {
        // Don't double-queue.
        if self.backfill_queue.iter().any(|j| j.folder_path == folder_path) {
            return;
        }

        let all_uids = match self.fetch_all_uids(folder_path).await {
            Ok(uids) => uids,
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: backfill UID fetch failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
                return;
            }
        };

        let cached_uids = {
            let cache = self.cache.lock().await;
            cache
                .load_cached_uids(self.account.id, folder_path)
                .unwrap_or_default()
        };
        let cached_set: HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids
            .into_iter()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            return;
        }

        tracing::info!(
            "Worker[{}]: queued {} messages for backfill in {}",
            self.account.id,
            missing.len(),
            folder_path
        );

        self.backfill_queue.push_back(BackfillJob {
            folder_path: folder_path.to_string(),
            remaining: missing,
        });
    }

    async fn fetch_all_uids(
        &mut self,
        folder_path: &str,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        self.ensure_session().await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        let session = self.session.as_mut().ok_or("no session")?;
        session.fetch_all_uids(folder_path).await
    }

    async fn run_backfill_chunk(&mut self, folder_path: &str, chunk: &[i64]) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };

        let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
        let uid_set_str = uid_set.join(",");

        match session.fetch_summaries_by_uids(&uid_set_str).await {
            Ok(summaries) => {
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
                }
                tracing::debug!(
                    "Worker[{}]: backfilled {} messages in {}",
                    self.account.id,
                    summaries.len(),
                    folder_path
                );
            }
            Err(e) => {
                tracing::error!(
                    "Worker[{}]: backfill chunk failed for {}: {}",
                    self.account.id,
                    folder_path,
                    e
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // IDLE
    // -----------------------------------------------------------------------

    async fn idle_wait(&mut self, folder_path: &str) -> IdleResult {
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return IdleResult::Disconnected,
        };

        if session.select_folder(folder_path).await.is_err() {
            return IdleResult::Disconnected;
        }

        if session.idle_start().await.is_err() {
            return IdleResult::Disconnected;
        }

        // current_tag() is read AFTER idle_start(), which increments the tag.
        let tag_num = session.current_tag();

        tracing::debug!(
            "Worker[{}]: IDLE on {} (tag T{})",
            self.account.id,
            folder_path,
            tag_num
        );

        let idle_timeout = tokio::time::Duration::from_secs(29 * 60);
        let idle_tag = format!("T{}", tag_num);

        let result = tokio::time::timeout(idle_timeout, async {
            loop {
                // Check for commands (non-blocking) during IDLE.
                while let Ok(cmd) = self.rx.try_recv() {
                    match cmd {
                        WorkerRequest::SetIdleFolder { folder_path } => {
                            return IdleResult::FolderSwitch(folder_path);
                        }
                        WorkerRequest::Shutdown => {
                            self.shutdown = true;
                            return IdleResult::Request;
                        }
                        // Other requests: leave IDLE and let the top of the
                        // loop process them at priority 0.
                        other => {
                            self.pending = Some(other);
                            return IdleResult::Request;
                        }
                    }
                }

                match session.idle_read_line().await {
                    Ok(Some(line)) => {
                        if let Some(event) = parse_idle_event(&line) {
                            return event;
                        }
                    }
                    Ok(None) => return IdleResult::Disconnected,
                    Err(_) => return IdleResult::Disconnected,
                }
            }
        })
        .await;

        let _ = session.idle_done(&idle_tag).await;

        match result {
            Ok(IdleResult::MailboxChange) => IdleResult::MailboxChange,
            Ok(IdleResult::FolderSwitch(f)) => IdleResult::FolderSwitch(f),
            Ok(IdleResult::Disconnected) => IdleResult::Disconnected,
            Ok(IdleResult::Request) => IdleResult::Request,
            Err(_) => IdleResult::Disconnected, // timeout — reconnect
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn has_cached_messages(&self) -> bool {
        let cache = self.cache.lock().await;
        cache
            .has_cached_messages(self.account.id)
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

fn parse_idle_event(line: &str) -> Option<IdleResult> {
    let rest = line.strip_prefix("* ")?;
    if rest.contains("EXISTS")
        || rest.contains("EXPUNGE")
        || rest.starts_with("FLAGS")
        || (rest.contains("FETCH") && rest.contains("FLAGS"))
    {
        Some(IdleResult::MailboxChange)
    } else {
        None
    }
}
