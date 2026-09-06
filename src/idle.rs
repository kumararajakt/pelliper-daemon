use crate::cache::Cache;
use crate::imap::{self, RawImapSession};
use crate::oauth;
use crate::secrets::Secrets;
use std::collections::HashMap;
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
}

/// Routes IdleCommands to per-account IDLE tasks.
pub struct IdleRouter {
    senders: std::sync::Mutex<HashMap<i64, mpsc::Sender<IdleCommand>>>,
}

impl IdleRouter {
    pub fn new() -> Self {
        Self {
            senders: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Register a new account's command sender.
    pub fn register(&self, account_id: i64, sender: mpsc::Sender<IdleCommand>) {
        self.senders.lock().unwrap().insert(account_id, sender);
    }

    /// Remove an account's sender (e.g., when account is deleted).
    pub fn unregister(&self, account_id: i64) {
        self.senders.lock().unwrap().remove(&account_id);
    }

    /// Route a SetFolder command to the correct account.
    pub async fn set_folder(&self, account_id: i64, folder_path: &str) {
        let sender = self.senders.lock().unwrap().get(&account_id).cloned();
        if let Some(sender) = sender {
            let _ = sender
                .send(IdleCommand::SetFolder {
                    account_id,
                    folder_path: folder_path.to_string(),
                })
                .await;
        } else {
            tracing::warn!(
                "IdleRouter: no sender registered for account {}",
                account_id
            );
        }
    }
}

/// Manages a long-lived IMAP IDLE connection for one account.
///
/// Runs as a spawned tokio task. Listens for IdleCommands on its channel
/// and stays in IDLE on the most recently viewed folder.
struct IdleWorker {
    account_id: i64,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    cmd_rx: mpsc::Receiver<IdleCommand>,
    notify_tx: mpsc::Sender<IdleNotification>,
}

impl IdleWorker {
    async fn run(mut self) {
        let account = match self.load_account().await {
            Some(a) => a,
            None => {
                tracing::error!("IdleWorker: account {} not found", self.account_id);
                return;
            }
        };

        let initial_folder = self.get_initial_folder().await;
        tracing::info!(
            "IdleWorker[{}]: starting for {} (initial folder: {:?})",
            self.account_id,
            account.email,
            initial_folder
        );

        let mut current_folder: Option<String> = initial_folder;
        let mut session: Option<RawImapSession> = None;

        loop {
            // Check for commands (non-blocking)
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                match cmd {
                    IdleCommand::SetFolder {
                        account_id: aid,
                        folder_path,
                    } if aid == self.account_id => {
                        tracing::info!(
                            "IdleWorker[{}]: switching to folder {}",
                            self.account_id,
                            folder_path
                        );
                        session = None;
                        current_folder = Some(folder_path);
                    }
                    _ => {}
                }
            }

            // Connect if needed
            if session.is_none() {
                match self.connect(&account).await {
                    Ok(s) => session = Some(s),
                    Err(e) => {
                        tracing::error!(
                            "IdleWorker[{}]: connect failed for {}: {}",
                            self.account_id,
                            account.email,
                            e
                        );
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
                tracing::error!(
                    "IdleWorker[{}]: SELECT {} failed: {}",
                    self.account_id,
                    folder,
                    e
                );
                session = None;
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }

            let tag_num = sess.current_tag();
            if let Err(e) = sess.idle_start().await {
                tracing::error!(
                    "IdleWorker[{}]: IDLE start failed: {}",
                    self.account_id,
                    e
                );
                session = None;
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }

            tracing::info!(
                "IdleWorker[{}]: IDLE on {} (tag T{})",
                self.account_id,
                folder,
                tag_num
            );

            // Read IDLE notifications with 29-minute timeout (RFC)
            let idle_timeout = tokio::time::Duration::from_secs(29 * 60);
            let idle_tag = format!("T{}", tag_num);

            let result = tokio::time::timeout(idle_timeout, async {
                loop {
                    while let Ok(cmd) = self.cmd_rx.try_recv() {
                        match cmd {
                            IdleCommand::SetFolder {
                                account_id: aid,
                                folder_path,
                            } if aid == self.account_id => {
                                return IdleEvent::FolderSwitch(folder_path);
                            }
                            _ => {}
                        }
                    }

                    match sess.idle_read_line().await {
                        Ok(Some(line)) => {
                            if line.is_empty() {
                                continue;
                            }
                            if let Some(event) = parse_idle_event(&line) {
                                return event;
                            }
                        }
                        Ok(None) => return IdleEvent::Disconnected,
                        Err(e) => {
                            tracing::error!(
                                "IdleWorker[{}]: read error: {}",
                                self.account_id,
                                e
                            );
                            return IdleEvent::Disconnected;
                        }
                    }
                }
            })
            .await;

            let _ = sess.idle_done(&idle_tag).await;

            match result {
                Ok(IdleEvent::FolderSwitch(folder)) => {
                    tracing::info!(
                        "IdleWorker[{}]: switching to {}",
                        self.account_id,
                        folder
                    );
                    current_folder = Some(folder);
                    if let Some(mut s) = session.take() {
                        s.logout().await;
                    }
                }
                Ok(IdleEvent::MailboxChange) => {
                    tracing::info!(
                        "IdleWorker[{}]: mailbox changed on {}, re-syncing",
                        self.account_id,
                        folder
                    );
                    if let Err(e) = self.sync_folder(&folder, &account.email).await {
                        tracing::error!(
                            "IdleWorker[{}]: sync failed: {}",
                            self.account_id,
                            e
                        );
                    }
                    let _ = self
                        .notify_tx
                        .send(IdleNotification {
                            account_id: self.account_id,
                            folder_path: folder,
                        })
                        .await;
                }
                Ok(IdleEvent::Disconnected) | Err(_) => {
                    tracing::warn!(
                        "IdleWorker[{}]: disconnected or timed out, reconnecting",
                        self.account_id
                    );
                    session = None;
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn load_account(&self) -> Option<crate::cache::Account> {
        let cache = self.cache.lock().await;
        cache
            .load_accounts()
            .ok()?
            .into_iter()
            .find(|a| a.id == self.account_id)
    }

    async fn get_initial_folder(&self) -> Option<String> {
        let cache = self.cache.lock().await;
        cache
            .load_folders(self.account_id)
            .ok()?
            .into_iter()
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
            let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID").unwrap_or_else(|_| {
                "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com"
                    .to_string()
            });
            let client_secret =
                std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();

            let token = oauth::refresh_access_token(
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

    async fn sync_folder(
        &self,
        folder_path: &str,
        _email: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let account = self.load_account().await.ok_or("Account not found")?;
        let mut session = self.connect(&account).await?;
        let summaries = session.fetch_message_summaries(folder_path).await?;
        session.logout().await;

        let cache = self.cache.lock().await;
        let mut valid_uids = Vec::new();
        let mut unread_count = 0;

        for msg in &summaries {
            valid_uids.push(msg.uid);
            if !msg.is_read {
                unread_count += 1;
            }
            let _ = cache.upsert_message(
                self.account_id,
                folder_path,
                msg.uid,
                &msg.subject,
                &msg.sender,
                msg.date,
                msg.is_read,
                msg.is_starred,
                msg.has_attachments,
                &msg.preview,
            );
        }

        let _ = cache.delete_stale_messages(self.account_id, folder_path, &valid_uids);
        let _ = cache.update_folder_unread(self.account_id, folder_path, unread_count);

        tracing::info!(
            "IdleWorker[{}]: synced {} messages for {}",
            self.account_id,
            summaries.len(),
            folder_path
        );
        Ok(())
    }
}

enum IdleEvent {
    MailboxChange,
    FolderSwitch(String),
    Disconnected,
}

fn parse_idle_event(line: &str) -> Option<IdleEvent> {
    let rest = line.strip_prefix("* ")?;
    if rest.contains("EXISTS") {
        return Some(IdleEvent::MailboxChange);
    }
    if rest.contains("EXPUNGE") {
        return Some(IdleEvent::MailboxChange);
    }
    if rest.starts_with("FLAGS") {
        return Some(IdleEvent::MailboxChange);
    }
    if rest.contains("FETCH") && rest.contains("FLAGS") {
        return Some(IdleEvent::MailboxChange);
    }
    None
}

/// Spawn IDLE workers for all enabled accounts.
/// Returns a router that can send commands to individual workers.
pub fn spawn_idle_workers(
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    notify_tx: mpsc::Sender<IdleNotification>,
) -> IdleRouter {
    let router = IdleRouter::new();

    // Load accounts synchronously (cache is behind a Mutex but we can try_lock)
    let accounts = {
        let cache_clone = cache.clone();
        // We need to block briefly to get accounts — this is called once at startup
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let cache = cache_clone.lock().await;
                cache.load_accounts().unwrap_or_default()
            })
        })
    };

    for account in &accounts {
        if !account.enabled {
            continue;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        router.register(account.id, cmd_tx);

        let cache = cache.clone();
        let secrets = secrets.clone();
        let notify_tx = notify_tx.clone();
        let account_id = account.id;

        tokio::spawn(async move {
            // Delay to let initial sync finish
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let worker = IdleWorker {
                account_id,
                cache,
                secrets,
                cmd_rx,
                notify_tx,
            };
            worker.run().await;
        });
    }

    router
}

/// Spawn an IDLE worker for a single account (called when account is added at runtime).
pub fn spawn_idle_worker_for_account(
    router: &IdleRouter,
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    notify_tx: mpsc::Sender<IdleNotification>,
    account_id: i64,
) {
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    router.register(account_id, cmd_tx);

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let worker = IdleWorker {
            account_id,
            cache,
            secrets,
            cmd_rx,
            notify_tx,
        };
        worker.run().await;
    });
}
