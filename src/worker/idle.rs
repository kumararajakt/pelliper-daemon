use super::{IdleResult, Worker, WorkerRequest};
use async_imap::imap_proto::types::{MailboxDatum, Response};
use futures::StreamExt;

impl Worker {
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
                if let Ok(cmd) = self.rx.try_recv() {
                    match cmd {
                        WorkerRequest::SetIdleFolder { folder_path } => {
                            return IdleResult::FolderSwitch(folder_path);
                        }
                        WorkerRequest::Shutdown => {
                            self.shutdown = true;
                            return IdleResult::Request;
                        }

                        other => {
                            self.pending = Some(other);
                            return IdleResult::Request;
                        }
                    }
                }

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
                    Err(_) => {}
                }
            }
        })
        .await;

        self.session = handle.done().await.ok();

        match result {
            Ok(IdleResult::MailboxChange) => IdleResult::MailboxChange,
            Ok(IdleResult::FolderSwitch(f)) => IdleResult::FolderSwitch(f),
            Ok(IdleResult::Disconnected) => IdleResult::Disconnected,
            Ok(IdleResult::Request) => IdleResult::Request,
            Err(_) => IdleResult::Disconnected,
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
