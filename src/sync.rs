use crate::cache::{Cache, Account};
use crate::secrets::Secrets;

#[derive(Clone)]
pub struct SyncEngine {
    cache: std::sync::Arc<tokio::sync::Mutex<Cache>>,
    secrets: Secrets,
}

impl SyncEngine {
    pub fn new(cache: std::sync::Arc<tokio::sync::Mutex<Cache>>, secrets: Secrets) -> Self {
        Self { cache, secrets }
    }

    /// Sync all enabled accounts: list folders, fetch newest messages per folder.
    pub async fn sync_all(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let accounts = {
            let cache = self.cache.lock().await;
            cache.load_accounts()?
        };

        for account in &accounts {
            if account.enabled {
                if let Err(e) = self.sync_account(account).await {
                    tracing::error!("Failed to sync account {}: {}", account.email, e);
                }
            }
        }
        Ok(())
    }

    /// Sync a single account: list folders, then sync each folder.
    pub async fn sync_account(&self, account: &Account) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting sync for {} ({})", account.email, account.auth_type);

        let host = account.imap_host.as_deref().unwrap_or("localhost");
        let port = account.imap_port.unwrap_or(993) as u16;
        let email = &account.email;

        // Get credentials from keyring
        let key_name = format!("pelliper:{}", email);
        let credential = match self.secrets.get_password(&key_name).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to get credential for {}: {}", email, e);
                return Err(e);
            }
        };

        if account.auth_type == "oauth" {
            self.sync_account_oauth(account, host, port, email, &credential).await
        } else {
            self.sync_account_password(account, host, port, email, &credential).await
        }
    }

    /// Sync using OAuth (XOAUTH2) via RawImapSession.
    async fn sync_account_oauth(
        &self,
        account: &Account,
        host: &str,
        port: u16,
        email: &str,
        access_token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = crate::imap::connect_session_oauth(host, port, email, access_token).await?;

        // List folders
        let folders = session.list_folders().await?;
        tracing::info!("Found {} folders for {} (oauth)", folders.len(), email);

        {
            let mut cache = self.cache.lock().await;
            for folder in &folders {
                if let Err(e) = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect) {
                    tracing::error!("Failed to upsert folder {}: {}", folder.path, e);
                }
            }
        }

        // Fetch messages for each folder (skip noselect virtual folders)
        for folder in &folders {
            if folder.noselect {
                tracing::debug!("Skipping noselect folder: {}", folder.path);
                continue;
            }
            if let Err(e) = self.sync_folder_messages_raw(account, &folder.path, &mut session).await {
                tracing::error!("Failed to sync folder {} for {}: {}", folder.path, email, e);
            }
        }

        session.logout().await;
        tracing::info!("Sync completed for {} (oauth)", email);
        Ok(())
    }

    /// Sync using password via async-imap Session.
    async fn sync_account_password(
        &self,
        account: &Account,
        host: &str,
        port: u16,
        email: &str,
        password: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = crate::imap::connect_session_password(host, port, email, password).await?;

        // List folders
        let folders = crate::imap::list_folders(&mut session).await?;
        tracing::info!("Found {} folders for {} (password)", folders.len(), email);

        {
            let mut cache = self.cache.lock().await;
            for folder in &folders {
                if let Err(e) = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect) {
                    tracing::error!("Failed to upsert folder {}: {}", folder.path, e);
                }
            }
        }

        // Fetch messages for each folder (skip noselect virtual folders)
        for folder in &folders {
            if folder.noselect {
                tracing::debug!("Skipping noselect folder: {}", folder.path);
                continue;
            }
            if let Err(e) = self.sync_folder_messages_imap(account, &folder.path, &mut session).await {
                tracing::error!("Failed to sync folder {} for {}: {}", folder.path, email, e);
            }
        }

        crate::imap::logout_session(&mut session).await;
        tracing::info!("Sync completed for {} (password)", email);
        Ok(())
    }

    /// Sync messages for a folder using the raw OAuth session.
    async fn sync_folder_messages_raw(
        &self,
        account: &Account,
        folder_path: &str,
        session: &mut crate::imap::RawImapSession,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const FETCH_LIMIT: usize = 50;

        let summaries = session.fetch_message_summaries(folder_path, FETCH_LIMIT).await?;
        self.process_summaries(account, folder_path, &summaries).await
    }

    /// Sync messages for a folder using async-imap Session.
    async fn sync_folder_messages_imap(
        &self,
        account: &Account,
        folder_path: &str,
        session: &mut async_imap::Session<crate::imap::CompatTls>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const FETCH_LIMIT: usize = 50;

        let summaries = crate::imap::fetch_message_summaries(session, folder_path, FETCH_LIMIT).await?;
        self.process_summaries(account, folder_path, &summaries).await
    }

    /// Process message summaries: upsert into cache, delete stale, update unread.
    async fn process_summaries(
        &self,
        account: &Account,
        folder_path: &str,
        summaries: &[crate::imap::ImapMessageSummary],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut cache = self.cache.lock().await;
        let mut valid_uids = Vec::new();
        let mut unread_count = 0;

        for msg in summaries {
            valid_uids.push(msg.uid);
            if !msg.is_read {
                unread_count += 1;
            }

            if let Err(e) = cache.upsert_message(
                account.id,
                folder_path,
                msg.uid,
                &msg.subject,
                &msg.sender,
                msg.date,
                msg.is_read,
                msg.is_starred,
                msg.has_attachments,
                &msg.preview,
            ) {
                tracing::error!("Failed to upsert message uid={}: {}", msg.uid, e);
            }
        }

        if let Err(e) = cache.delete_stale_messages(account.id, folder_path, &valid_uids) {
            tracing::error!("Failed to delete stale messages: {}", e);
        }

        if let Err(e) = cache.update_folder_unread(account.id, folder_path, unread_count) {
            tracing::error!("Failed to update unread count: {}", e);
        }

        tracing::info!(
            "Folder {}: {} messages, {} unread",
            folder_path,
            summaries.len(),
            unread_count
        );

        Ok(())
    }
}
