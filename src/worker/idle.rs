//! IMAP IDLE: block for server push, handle folder switches, and classify
//! IDLE responses into mailbox events.

use super::{IdleResult, Worker, WorkerRequest};
use async_imap::imap_proto::types::{MailboxDatum, Response};
use futures::StreamExt;

impl Worker {
    /// Handle an IDLE mailbox-change: fast re-sync the active folder.
    pub(super) async fn on_mailbox_change(&mut self, folder_path: &str) {
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
    pub(super) async fn on_folder_switch(&mut self, folder_path: String) {
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
    // -----------------------------------------------------------------------
    // IDLE
    // -----------------------------------------------------------------------

    pub(super) async fn idle_wait(&mut self, folder_path: &str) -> IdleResult {
        let mut session = match self.session.take() {
            Some(s) => s,
            None => return IdleResult::Disconnected,
        };

        if session.select(folder_path).await.is_err() {
            self.session = Some(session);
            return IdleResult::Disconnected;
        }

        let mut handle = session.idle();
        if handle.init().await.is_err() {
            return IdleResult::Disconnected;
        }

        tracing::debug!("Worker[{}]: IDLE on {}", self.account.id, folder_path);

        let idle_timeout = tokio::time::Duration::from_secs(29 * 60);

        let result = tokio::time::timeout(idle_timeout, async {
            loop {
                // Check for commands (non-blocking) during IDLE. Every arm
                // below returns, so this handles at most one command — an
                // `if let`, not a `while let`.
                if let Ok(cmd) = self.rx.try_recv() {
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

                // Poll the IDLE stream so requests stay responsive even when
                // the server is quiet; IDLE responses drive mailbox changes.
                match tokio::time::timeout(tokio::time::Duration::from_millis(500), handle.next())
                    .await
                {
                    Ok(Some(Ok(resp))) => {
                        if let Some(event) = idle_event_from_response(resp.parsed()) {
                            return event;
                        }
                    }
                    Ok(Some(Err(_))) => return IdleResult::Disconnected,
                    Ok(None) => return IdleResult::Disconnected,
                    Err(_) => {} // no push within 500ms — check requests again
                }
            }
        })
        .await;

        // Leave IDLE cleanly and take the session back.
        self.session = handle.done().await.ok();

        match result {
            Ok(IdleResult::MailboxChange) => IdleResult::MailboxChange,
            Ok(IdleResult::FolderSwitch(f)) => IdleResult::FolderSwitch(f),
            Ok(IdleResult::Disconnected) => IdleResult::Disconnected,
            Ok(IdleResult::Request) => IdleResult::Request,
            Err(_) => IdleResult::Disconnected, // timeout — reconnect
        }
    }
}
fn idle_event_from_response(resp: &Response<'_>) -> Option<IdleResult> {
    match resp {
        Response::Expunge(_) => Some(IdleResult::MailboxChange),
        Response::Fetch(..) => Some(IdleResult::MailboxChange),
        Response::MailboxData(MailboxDatum::Exists(_)) => Some(IdleResult::MailboxChange),
        Response::MailboxData(MailboxDatum::Flags(_)) => Some(IdleResult::MailboxChange),
        _ => None,
    }
}
