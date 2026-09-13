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

use crate::cache::{Account, Cache};
use crate::imap::{self, ImapMessageSummary, RawImapSession};
use crate::secrets::Secrets;

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
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

/// Beyond the body-prefetch window, still download attachments for this many
/// of the most recent messages flagged as having attachments, so viewing or
/// saving them is instant. Older ones load on demand via LoadBody.
const PREFETCH_ATTACHMENT_LIMIT: usize = 100;

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
    SetIdleFolder {
        folder_path: String,
    },
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
    DeleteMessage {
        folder_path: String,
        uid: i64,
    },
    /// Create a new IMAP folder.
    CreateFolder {
        name: String,
    },
    /// Rename an IMAP folder.
    RenameFolder {
        old_name: String,
        new_name: String,
    },
    /// Delete an IMAP folder.
    DeleteFolder {
        name: String,
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
            WorkerRequest::SetFlags {
                folder_path,
                uid,
                is_read,
                is_starred,
            } => {
                self.handle_set_flags(&folder_path, uid, is_read, is_starred).await;
            }
            WorkerRequest::MoveMessage {
                folder_path,
                uid,
                dest_folder,
            } => {
                self.handle_move_message(&folder_path, uid, dest_folder).await;
            }
            WorkerRequest::DeleteMessage { folder_path, uid } => {
                self.handle_delete_message(&folder_path, uid).await;
            }
            WorkerRequest::CreateFolder { name } => {
                self.handle_create_folder(&name).await;
            }
            WorkerRequest::RenameFolder { old_name, new_name } => {
                self.handle_rename_folder(&old_name, &new_name).await;
            }
            WorkerRequest::DeleteFolder { name } => {
                self.handle_delete_folder(&name).await;
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
            Err(e) => {
                // On auth error, invalidate token so reconnect gets a fresh one
                if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" {
                    tracing::warn!("Worker[{}]: auth error in load_body, refreshing token", self.account.id);
                    self.invalidate_token();
                } else {
                    self.session = None;
                }
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

    /// Apply read/starred flag changes to a message (STORE + local cache),
    /// then refresh the folder's unread count.
    async fn handle_set_flags(
        &mut self,
        folder_path: &str,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    ) {
        let mut add: Vec<&str> = Vec::new();
        let mut remove: Vec<&str> = Vec::new();
        if let Some(read) = is_read {
            if read { add.push("\\Seen"); } else { remove.push("\\Seen"); }
        }
        if let Some(starred) = is_starred {
            if starred { add.push("\\Flagged"); } else { remove.push("\\Flagged"); }
        }

        if self.ensure_session().await.is_err() {
            return;
        }
        // Try the operation; on auth error, invalidate token and retry once
        let result = self.do_set_flags(folder_path, uid, &add, &remove).await;
        if result.is_err() {
            let e = result.unwrap_err();
            if Self::is_auth_error(&e) && self.account.auth_type == "oauth" {
                tracing::warn!("Worker[{}]: auth error in set_flags, refreshing token", self.account.id);
                self.invalidate_token();
                if self.ensure_session().await.is_err() {
                    return;
                }
                let _ = self.do_set_flags(folder_path, uid, &add, &remove).await;
            }
        }

        {
            let cache = self.cache.lock().await;
            let _ = cache.update_message_flags(self.account.id, folder_path, uid, is_read, is_starred);
            let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }
    }

    /// Internal helper: select folder and store flags (may fail with auth error).
    async fn do_set_flags(
        &mut self,
        folder_path: &str,
        uid: i64,
        add: &[&str],
        remove: &[&str],
    ) -> Result<(), String> {
        let session = self.session.as_mut().ok_or_else(|| "no session".to_string())?;
        session.select_folder(folder_path).await.map_err(|e| e.to_string())?;
        session.store_flags(uid, add, remove).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Move a message to a destination folder (or Trash when None). Uses
    /// UID MOVE where available, falling back to COPY + \Deleted + EXPUNGE.
    async fn handle_move_message(
        &mut self,
        folder_path: &str,
        uid: i64,
        dest_folder: Option<String>,
    ) {
        if self.ensure_session().await.is_err() {
            return;
        }

        let dest = match dest_folder {
            Some(d) if !d.is_empty() => d,
            _ => {
                let session = self.session.as_mut();
                match session {
                    Some(s) => match s.find_folder_by_kind(crate::imap::FolderKind::Trash).await {
                        Ok(Some(t)) => t,
                        Ok(None) => {
                            tracing::warn!(
                                "Worker[{}]: no Trash folder found for move",
                                self.account.id
                            );
                            return;
                        }
                        Err(e) => {
                            tracing::error!("Worker[{}]: trash lookup failed: {}", self.account.id, e);
                            return;
                        }
                    },
                    None => return,
                }
            }
        };

        // Try the move; on auth error, invalidate token and retry once
        let move_result = self.do_move(folder_path, uid, &dest).await;
        if let Err(ref e) = move_result {
            if Self::is_auth_error(e) && self.account.auth_type == "oauth" {
                tracing::warn!("Worker[{}]: auth error in move, refreshing token", self.account.id);
                self.invalidate_token();
                if self.ensure_session().await.is_ok() {
                    let _ = self.do_move(folder_path, uid, &dest).await;
                }
            }
        }

        {
            let cache = self.cache.lock().await;
            let _ = cache.move_message(self.account.id, folder_path, uid, &dest);
            let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        }

        // Make sure the destination shows the moved message.
        if let Err(e) = self.sync_folder_first_page(&dest).await {
            tracing::error!(
                "Worker[{}]: refresh {} after move failed: {}",
                self.account.id, dest, e
            );
        }
        return;
    }

    async fn do_move(&mut self, folder_path: &str, uid: i64, dest: &str) -> Result<(), String> {
        let session = self.session.as_mut().ok_or_else(|| "no session".to_string())?;
        session.select_folder(folder_path).await.map_err(|e| e.to_string())?;
        match session.uid_move(uid, dest).await {
            Ok(true) => Ok(()),
            Ok(false) => {
                tracing::info!(
                    "Worker[{}]: UID MOVE unsupported, using COPY + EXPUNGE",
                    self.account.id
                );
                session.uid_copy(uid, dest).await.map_err(|e| e.to_string())?;
                session.store_flags(uid, &["\\Deleted"], &[]).await.map_err(|e| e.to_string())?;
                session.expunge().await.map_err(|e| e.to_string())?;
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// Delete a message: if already in a Trash folder, permanently remove it
    /// (flag \Deleted + EXPUNGE); otherwise move it to Trash as a soft delete.
    async fn handle_delete_message(&mut self, folder_path: &str, uid: i64) {
        if self.ensure_session().await.is_err() {
            return;
        }

        let in_trash =
            crate::imap::FolderKind::from_path(folder_path) == crate::imap::FolderKind::Trash;

        if in_trash {
            // Permanently delete: \Deleted + EXPUNGE
            let session = self.session.as_mut();
            if let Some(s) = session {
                let _ = s.select_folder(folder_path).await;
                let _ = s.store_flags(uid, &["\\Deleted"], &[]).await;
                let _ = s.expunge().await;
            }
            let cache = self.cache.lock().await;
            let _ = cache.delete_message(self.account.id, folder_path, uid);
            let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
            let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
        } else {
            // Soft delete: move to Trash
            let session = self.session.as_mut();
            let trash = match session {
                Some(s) => match s.find_folder_by_kind(crate::imap::FolderKind::Trash).await {
                    Ok(Some(t)) => t,
                    Ok(None) => {
                        tracing::warn!(
                            "Worker[{}]: no Trash folder found for delete",
                            self.account.id
                        );
                        return;
                    }
                    Err(e) => {
                        tracing::error!("Worker[{}]: trash lookup failed: {}", self.account.id, e);
                        return;
                    }
                },
                None => return,
            };
            if self.do_move(folder_path, uid, &trash).await.is_ok() {
                let cache = self.cache.lock().await;
                let _ = cache.move_message(self.account.id, folder_path, uid, &trash);
                let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
                let _ = cache.update_folder_unread(self.account.id, folder_path, unread);
            }
        }
    }

    async fn handle_create_folder(&mut self, name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.create_folder(name).await {
            tracing::error!("Worker[{}]: create folder failed: {}", self.account.id, e);
            return;
        }
        // Refresh the folder list so the sidebar picks up the new folder.
        match session.list_folders().await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache.upsert_folder(self.account.id, &folder.path, 0, folder.noselect);
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }
    }

    async fn handle_rename_folder(&mut self, old_name: &str, new_name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.rename_folder(old_name, new_name).await {
            tracing::error!("Worker[{}]: rename folder failed: {}", self.account.id, e);
            return;
        }
        // Refresh the folder list.
        match session.list_folders().await {
            Ok(folders) => {
                let cache = self.cache.lock().await;
                for folder in folders {
                    let _ = cache.upsert_folder(self.account.id, &folder.path, 0, folder.noselect);
                }
            }
            Err(e) => {
                tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
            }
        }
    }

    async fn handle_delete_folder(&mut self, name: &str) {
        if self.ensure_session().await.is_err() {
            return;
        }
        let session = match self.session.as_mut() {
            Some(s) => s,
            None => return,
        };
        if let Err(e) = session.delete_folder(name).await {
            tracing::error!("Worker[{}]: delete folder failed: {}", self.account.id, e);
            return;
        }
        // Remove from cache and refresh list.
        {
            let cache = self.cache.lock().await;
            let _ = cache.delete_folder(self.account.id, name);
        }
        if let Some(s) = self.session.as_mut() {
            match s.list_folders().await {
                Ok(folders) => {
                    let cache = self.cache.lock().await;
                    for folder in folders {
                        let _ =
                            cache.upsert_folder(self.account.id, &folder.path, 0, folder.noselect);
                    }
                }
                Err(e) => {
                    tracing::warn!("Worker[{}]: list_folders failed: {}", self.account.id, e);
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

    /// Get a valid OAuth access token, refreshing if expired or about to expire.
    /// Refreshes 5 minutes before expiry to avoid race conditions.
    async fn get_valid_access_token(&mut self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
                tracing::info!("Worker[{}]: Google issued new refresh token, saving", self.account.id);
                let cache = self.cache.lock().await;
                if let Err(e) = cache.update_refresh_token(self.account.id, new_rt) {
                    tracing::error!("Worker[{}]: failed to save new refresh token: {}", self.account.id, e);
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
    ) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
        let host = self.account.imap_host.clone().unwrap_or_else(|| "localhost".to_string());
        let port = self.account.imap_port.unwrap_or(993) as u16;
        let email = self.account.email.clone();

        if self.account.auth_type == "oauth" {
            let access_token = self.get_valid_access_token().await?;
            imap::connect_session_oauth(&host, port, &email, &access_token).await
        } else {
            let key_name = format!("pelliper:{}", email);
            let password = self.secrets.get_password(&key_name).await?;
            imap::connect_session_password_raw(&host, port, &email, &password).await
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
            && (lower.contains("fail") || lower.contains("invalid") || lower.contains("expired") || lower.contains("rejected"))
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
            .await;

        let summaries = match summaries {
            Ok(s) => s,
            Err(e) if Self::is_auth_error(&e.to_string()) && self.account.auth_type == "oauth" => {
                tracing::warn!("Worker[{}]: auth error in sync_folder_first_page, refreshing token", self.account.id);
                self.invalidate_token();
                self.connect().await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
                let session = self.session.as_mut().ok_or("no session after reconnect")?;
                session.fetch_recent_summaries(folder_path, page_size).await?
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
            let unread = cache.count_unread(self.account.id, folder_path).unwrap_or(0);
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
                tracing::warn!("Worker[{}]: periodic resync of {} failed: {}", id, folder, e);
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
        self.attachment_prefetch.clear();
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
            parsed.html.clone()
        };

        // Persist body + attachments so both are cached for offline/viewing.
        self.persist_parsed(folder_path, uid, &html, &parsed).await;

        Ok((html, parsed.has_attachments))
    }

    /// Save the parsed body HTML and any attachment files for a message, then
    /// refresh its `has_attachments` flag. Files land under
    /// `attachments/<account>/<sanitized-folder>/<uid>/<index>_<name>`.
    async fn persist_parsed(
        &mut self,
        folder_path: &str,
        uid: i64,
        html: &str,
        parsed: &imap::ParsedBody,
    ) {
        {
            let cache = self.cache.lock().await;
            let _ = cache.save_body(self.account.id, folder_path, uid, html);
            let _ = cache.update_has_attachment(
                self.account.id,
                folder_path,
                uid,
                parsed.has_attachments,
            );
        }

        if parsed.attachments.is_empty() {
            return;
        }

        let dir = self
            .attachments_dir
            .join(self.account.id.to_string())
            .join(sanitize_path_component(folder_path))
            .join(uid.to_string());
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!(
                "Worker[{}]: failed to create attachment dir {:?}: {}",
                self.account.id,
                dir,
                e
            );
            return;
        }

        let mut records = Vec::with_capacity(parsed.attachments.len());
        for (i, att) in parsed.attachments.iter().enumerate() {
            let fallback_name = attachment_fallback_name(&att.content_type, i);
            let safe_name = sanitize_filename(&att.name, &fallback_name);
            let full = dir.join(format!("{}_{}", i, safe_name));
            if let Err(e) = std::fs::write(&full, &att.bytes) {
                tracing::warn!(
                    "Worker[{}]: failed to write attachment {:?}: {}",
                    self.account.id,
                    full,
                    e
                );
                continue;
            }
            records.push(crate::cache::AttachmentRecord {
                name: if att.name.is_empty() {
                    fallback_name.clone()
                } else {
                    att.name.clone()
                },
                file_path: full.to_string_lossy().to_string(),
                content_type: att.content_type.clone(),
                size: att.bytes.len() as i64,
            });
        }

        if !records.is_empty() {
            let cache = self.cache.lock().await;
            let _ = cache.replace_attachments(self.account.id, folder_path, uid, &records);
            tracing::debug!(
                "Worker[{}]: cached {} attachment(s) for {} uid={}",
                self.account.id,
                records.len(),
                folder_path,
                uid
            );
        }
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
                    parsed.html.clone()
                };
                self.persist_parsed(folder_path, uid, &html, &parsed).await;
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
    // Background work: attachment prefetch
    // -----------------------------------------------------------------------

    /// Find messages flagged as having attachments that aren't stored on disk
    /// yet and enqueue them for background download. The fetch reuses the body
    /// fetch path (persist_parsed also writes attachment files), so messages
    /// already queued for body prefetch are skipped to avoid double fetches.
    async fn queue_attachment_prefetch(&mut self, folder_path: &str) {
        let uids = {
            let cache = self.cache.lock().await;
            cache
                .messages_missing_attachments(
                    self.account.id,
                    folder_path,
                    PREFETCH_ATTACHMENT_LIMIT,
                )
                .unwrap_or_default()
        };

        for uid in uids {
            let key = (folder_path.to_string(), uid);
            if self.attachment_prefetched.contains(&key) {
                continue;
            }
            if self.body_prefetch.iter().any(|k| k == &key) {
                self.attachment_prefetched.insert(key);
                continue;
            }
            self.attachment_prefetch.push_back(key.clone());
            self.attachment_prefetched.insert(key);
        }
    }

    /// Download one message's body + attachments. Same code path as body
    /// prefetch; persist_parsed writes any attachment files found.
    async fn run_one_attachment_prefetch(&mut self, folder_path: &str, uid: i64) {
        self.run_one_body_prefetch(folder_path, uid).await;
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

/// Map a folder path to a single safe directory component (e.g.
/// `INBOX/Custom` -> `INBOX_Custom`), avoiding nested/absolute paths.
fn sanitize_path_component(path: &str) -> String {
    let clean: String = path
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if clean.is_empty() {
        "folder".to_string()
    } else {
        clean
    }
}

/// Map an attachment's claimed name to a safe filename; falls back to a
/// generated `attachment_<n>.ext` name when nothing usable is provided.
fn sanitize_filename(name: &str, fallback: &str) -> String {
    let clean: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ' ' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();
    if clean.is_empty() || clean == "." {
        fallback.to_string()
    } else {
        clean
    }
}

/// Derive a default filename from a MIME type when a part has no name.
fn attachment_fallback_name(content_type: &str, index: usize) -> String {
    let lower = content_type.to_lowercase();
    let ext = match lower.split('/').nth(1) {
        Some("pdf") => "pdf",
        Some("zip") => "zip",
        Some("html") => "html",
        Some("plain") => "txt",
        Some("jpeg") => "jpg",
        Some("png") => "png",
        Some("gif") => "gif",
        Some("octet-stream") | None => "bin",
        Some(other) => {
            if other.is_empty() {
                "bin"
            } else {
                other
            }
        }
    };
    format!("attachment_{}.{}", index, ext)
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
