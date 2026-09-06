use crate::cache::Cache;
use crate::imap::{self, RawImapSession};
use crate::oauth;
use crate::secrets::Secrets;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Notification sent when IDLE detects folder changes.
#[derive(Debug, Clone)]
pub struct IdleNotification {
    pub account_id: i64,
    pub folder_path: String,
}

/// Commands sent to the IDLE manager.
#[derive(Debug)]
pub enum IdleCommand {
    /// Switch the IDLE connection to a different folder.
    SetFolder {
        account_id: i64,
        folder_path: String,
    },
    /// Stop IDLE and shut down.
    Stop,
}

/// Manages a long-lived IMAP IDLE connection for real-time push updates.
///
/// One IdleManager runs per account. It holds a dedicated IMAP connection
/// that stays in IDLE on the currently viewed folder. When the server
/// sends EXISTS/EXPUNGE/FLAGS notifications, it re-fetches summaries and
/// updates the cache. The UI models auto-refresh via QFileSystemWatcher.
pub struct IdleManager {
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    cmd_rx: mpsc::Receiver<IdleCommand>,
    cmd_tx: mpsc::Sender<IdleCommand>,
    notify_tx: mpsc::Sender<IdleNotification>,
}

impl IdleManager {
    pub fn new(
        cache: Arc<Mutex<Cache>>,
        secrets: Secrets,
        notify_tx: mpsc::Sender<IdleNotification>,
    ) -> (Self, mpsc::Sender<IdleCommand>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let manager = Self {
            cache,
            secrets,
            cmd_rx,
            cmd_tx: cmd_tx.clone(),
            notify_tx,
        };
        (manager, cmd_tx)
    }

    /// Run the IDLE loop for an account. Connects, authenticates, and
    /// stays in IDLE on the most recently viewed folder.
    pub async fn run(&mut self, account_id: i64) {
        let account = match self.load_account(account_id).await {
            Some(a) => a,
            None => {
                tracing::error!("IdleManager: account {} not found", account_id);
                return;
            }
        };

        let initial_folder = self.get_initial_folder(account_id).await;
        tracing::info!(
            "IdleManager: starting for {} (initial folder: {:?})",
            account.email,
            initial_folder
        );

        let mut current_folder: Option<String> = initial_folder;
        let mut session: Option<RawImapSession> = None;

        loop {
            // Check for commands (non-blocking)
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                match cmd {
                    IdleCommand::SetFolder { account_id: aid, folder_path } if aid == account_id => {
                        tracing::info!("IdleManager: switching to folder {}", folder_path);
                        // Drop current session to force reconnect with new folder
                        session = None;
                        current_folder = Some(folder_path);
                    }
                    IdleCommand::SetFolder { .. } => {}
                    IdleCommand::Stop => {
                        tracing::info!("IdleManager: stopping");
                        if let Some(mut s) = session {
                            s.logout().await;
                        }
                        return;
                    }
                }
            }

            // Connect if needed
            if session.is_none() {
                match self.connect(&account).await {
                    Ok(s) => session = Some(s),
                    Err(e) => {
                        tracing::error!("IdleManager: connect failed for {}: {}", account.email, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                }
            }

            // Select folder and start IDLE
            let folder = match &current_folder {
                Some(f) => f.clone(),
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let sess = session.as_mut().unwrap();
            if let Err(e) = sess.select_folder(&folder).await {
                tracing::error!("IdleManager: SELECT {} failed: {}", folder, e);
                session = None;
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }

            let tag_num = sess.current_tag();
            if let Err(e) = sess.idle_start().await {
                tracing::error!("IdleManager: IDLE start failed: {}", e);
                session = None;
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }

            tracing::info!("IdleManager: IDLE on {} (tag T{})", folder, tag_num);

            // Read IDLE notifications with a timeout (29 minutes per RFC)
            let idle_timeout = tokio::time::Duration::from_secs(29 * 60);
            let idle_tag = format!("T{}", tag_num);

            let result = tokio::time::timeout(idle_timeout, async {
                loop {
                    // Check for commands between reads
                    while let Ok(cmd) = self.cmd_rx.try_recv() {
                        match cmd {
                            IdleCommand::SetFolder { account_id: aid, folder_path } if aid == account_id => {
                                return IdleEvent::FolderSwitch(folder_path);
                            }
                            IdleCommand::SetFolder { .. } => {}
                            IdleCommand::Stop => return IdleEvent::Stop,
                        }
                    }

                    match sess.idle_read_line().await {
                        Ok(Some(line)) => {
                            if line.is_empty() { continue; }
                            // Parse IDLE notifications
                            if let Some(event) = parse_idle_event(&line) {
                                return event;
                            }
                            // Non-notification lines (e.g., untagged FETCH) — keep reading
                        }
                        Ok(None) => {
                            // Server ended IDLE (BYE or timeout)
                            return IdleEvent::Disconnected;
                        }
                        Err(e) => {
                            tracing::error!("IdleManager: read error: {}", e);
                            return IdleEvent::Disconnected;
                        }
                    }
                }
            }).await;

            // Send DONE to exit IDLE
            let _ = sess.idle_done(&idle_tag).await;

            match result {
                Ok(IdleEvent::FolderSwitch(folder)) => {
                    tracing::info!("IdleManager: switching to {}", folder);
                    current_folder = Some(folder);
                    // Reconnect to force clean SELECT
                    if let Some(mut s) = session.take() {
                        s.logout().await;
                    }
                }
                Ok(IdleEvent::MailboxChange) => {
                    tracing::info!("IdleManager: mailbox changed on {}, re-syncing", folder);
                    // Re-fetch summaries and update cache
                    if let Err(e) = self.sync_folder(account_id, &folder, &account.email).await {
                        tracing::error!("IdleManager: sync failed: {}", e);
                    }
                    // Notify UI to refresh
                    let _ = self.notify_tx.send(IdleNotification {
                        account_id,
                        folder_path: folder,
                    }).await;
                }
                Ok(IdleEvent::Stop) => {
                    tracing::info!("IdleManager: stopping");
                    if let Some(mut s) = session {
                        s.logout().await;
                    }
                    return;
                }
                Ok(IdleEvent::Disconnected) | Err(_) => {
                    tracing::warn!("IdleManager: disconnected or timed out, reconnecting");
                    session = None;
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn load_account(&self, account_id: i64) -> Option<crate::cache::Account> {
        let cache = self.cache.lock().await;
        cache.load_accounts().ok()?.into_iter().find(|a| a.id == account_id)
    }

    async fn get_initial_folder(&self, account_id: i64) -> Option<String> {
        let cache = self.cache.lock().await;
        cache.load_folders(account_id).ok()?.into_iter()
            .find(|f| f.path == "INBOX" || f.path == "Inbox")
            .map(|f| f.path)
    }

    async fn connect(
        &self,
        account: &crate::cache::Account,
    ) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
        let host = account.imap_host.as_deref().unwrap_or("localhost");
        let port = account.imap_port.unwrap_or(993) as u16;
        let email = &account.email;

        if account.auth_type == "oauth" {
            let refresh_token = account.refresh_token.as_deref().ok_or("No refresh token")?;
            let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID")
                .unwrap_or_else(|_| "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com".to_string());
            let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();

            let token = oauth::refresh_access_token(
                "https://oauth2.googleapis.com/token",
                &client_id,
                &client_secret,
                refresh_token,
            ).await?;

            imap::connect_session_oauth(host, port, email, &token.access_token).await
        } else {
            let key_name = format!("pelliper:{}", email);
            let password = self.secrets.get_password(&key_name).await?;
            imap::connect_session_password_raw(host, port, email, &password).await
        }
    }

    async fn sync_folder(
        &self,
        account_id: i64,
        folder_path: &str,
        _email: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Re-connect, fetch, update cache
        let account = self.load_account(account_id).await.ok_or("Account not found")?;
        let mut session = self.connect(&account).await?;
        let summaries = session.fetch_message_summaries(folder_path).await?;
        session.logout().await;

        let cache = self.cache.lock().await;
        let mut valid_uids = Vec::new();
        let mut unread_count = 0;

        for msg in &summaries {
            valid_uids.push(msg.uid);
            if !msg.is_read { unread_count += 1; }
            let _ = cache.upsert_message(
                account_id, folder_path, msg.uid,
                &msg.subject, &msg.sender, msg.date,
                msg.is_read, msg.is_starred, msg.has_attachments, &msg.preview,
            );
        }

        let _ = cache.delete_stale_messages(account_id, folder_path, &valid_uids);
        let _ = cache.update_folder_unread(account_id, folder_path, unread_count);

        tracing::info!("IdleManager: synced {} messages for {}", summaries.len(), folder_path);
        Ok(())
    }
}

enum IdleEvent {
    MailboxChange,
    FolderSwitch(String),
    Stop,
    Disconnected,
}

/// Parse an IDLE untagged line into an event.
fn parse_idle_event(line: &str) -> Option<IdleEvent> {
    let rest = line.strip_prefix("* ")?;
    // * <n> EXISTS — new messages arrived
    if rest.contains("EXISTS") {
        return Some(IdleEvent::MailboxChange);
    }
    // * <n> EXPUNGE — messages deleted
    if rest.contains("EXPUNGE") {
        return Some(IdleEvent::MailboxChange);
    }
    // * FLAGS (...) — flag changes (read/unread)
    if rest.starts_with("FLAGS") {
        return Some(IdleEvent::MailboxChange);
    }
    // * <n> FETCH (FLAGS ...) — individual flag change
    if rest.contains("FETCH") && rest.contains("FLAGS") {
        return Some(IdleEvent::MailboxChange);
    }
    None
}
