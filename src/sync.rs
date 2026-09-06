use crate::cache::{Cache, Account};
use crate::secrets::Secrets;
use crate::oauth;

const FIRST_PAGE_SIZE: usize = 200;
const BACKFILL_CHUNK: usize = 1000;

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

    async fn sync_account_oauth(
        &self,
        account: &Account,
        host: &str,
        port: u16,
        email: &str,
        _access_token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let refresh_token = match &account.refresh_token {
            Some(rt) if !rt.is_empty() => rt.clone(),
            _ => {
                tracing::error!("No refresh token stored for {}", email);
                return Err("No refresh token configured. Re-add the account.".into());
            }
        };

        let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID")
            .unwrap_or_else(|_| "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com".to_string());
        let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET")
            .unwrap_or_default();

        let token = oauth::refresh_access_token(
            "https://oauth2.googleapis.com/token",
            &client_id,
            &client_secret,
            &refresh_token,
        ).await?;

        tracing::info!("Refreshed access token for {}", email);
        self.sync_with_access_token(account, host, port, email, &token.access_token).await
    }

    async fn sync_with_access_token(
        &self,
        account: &Account,
        host: &str,
        port: u16,
        email: &str,
        access_token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = crate::imap::connect_session_oauth(host, port, email, access_token).await?;

        let folders = session.list_folders().await?;
        tracing::info!("Found {} folders for {} (oauth)", folders.len(), email);

        {
            let cache = self.cache.lock().await;
            for folder in &folders {
                if let Err(e) = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect) {
                    tracing::error!("Failed to upsert folder {}: {}", folder.path, e);
                }
            }
        }

        for folder in &folders {
            if folder.noselect {
                tracing::debug!("Skipping noselect folder: {}", folder.path);
                continue;
            }
            if let Err(e) = self.sync_folder_windowed_raw(account, &folder.path, &mut session).await {
                tracing::error!("Failed to sync folder {} for {}: {}", folder.path, email, e);
            }
        }

        session.logout().await;
        tracing::info!("Sync completed for {} (oauth)", email);
        Ok(())
    }

    async fn sync_account_password(
        &self,
        account: &Account,
        host: &str,
        port: u16,
        email: &str,
        password: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut session = crate::imap::connect_session_password(host, port, email, password).await?;

        let folders = crate::imap::list_folders(&mut session).await?;
        tracing::info!("Found {} folders for {} (password)", folders.len(), email);

        {
            let cache = self.cache.lock().await;
            for folder in &folders {
                if let Err(e) = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect) {
                    tracing::error!("Failed to upsert folder {}: {}", folder.path, e);
                }
            }
        }

        for folder in &folders {
            if folder.noselect {
                tracing::debug!("Skipping noselect folder: {}", folder.path);
                continue;
            }
            if let Err(e) = self.sync_folder_windowed_imap(account, &folder.path, &mut session).await {
                tracing::error!("Failed to sync folder {} for {}: {}", folder.path, email, e);
            }
        }

        crate::imap::logout_session(&mut session).await;
        tracing::info!("Sync completed for {} (password)", email);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Two-phase windowed sync (raw IMAP session)
    // -----------------------------------------------------------------------

    async fn sync_folder_windowed_raw(
        &self,
        account: &Account,
        folder_path: &str,
        session: &mut crate::imap::RawImapSession,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1: Fetch recent messages for instant display
        let recent = session.fetch_recent_summaries(folder_path, FIRST_PAGE_SIZE).await?;

        {
            let cache = self.cache.lock().await;
            for msg in &recent {
                let _ = cache.upsert_message(
                    account.id, folder_path, msg.uid,
                    &msg.subject, &msg.sender, msg.date,
                    msg.is_read, msg.is_starred, msg.has_attachments, &msg.preview,
                );
            }
            let unread = recent.iter().filter(|m| !m.is_read).count() as i32;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!(
            "Folder {}: {} recent messages cached (phase 1)",
            folder_path, recent.len()
        );

        // Phase 2: Backfill remaining messages in chunks
        let all_uids = session.fetch_all_uids(folder_path).await?;
        let cached_uids: Vec<i64> = {
            let cache = self.cache.lock().await;
            cache.load_cached_uids(account.id, folder_path)?
        };
        let cached_set: std::collections::HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids.iter().copied()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            tracing::info!("Folder {}: already fully cached", folder_path);
            return Ok(());
        }

        tracing::info!(
            "Folder {}: {} total, {} cached, {} missing — backfilling",
            folder_path, all_uids.len(), cached_set.len(), missing.len()
        );

        for chunk in missing.chunks(BACKFILL_CHUNK) {
            let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
            let uid_set_str = uid_set.join(",");

            match session.fetch_summaries_by_uids(&uid_set_str).await {
                Ok(summaries) => {
                    let cache = self.cache.lock().await;
                    for msg in &summaries {
                        let _ = cache.upsert_message(
                            account.id, folder_path, msg.uid,
                            &msg.subject, &msg.sender, msg.date,
                            msg.is_read, msg.is_starred, msg.has_attachments, &msg.preview,
                        );
                    }
                    tracing::debug!(
                        "Folder {}: backfilled chunk of {} messages",
                        folder_path, summaries.len()
                    );
                }
                Err(e) => {
                    tracing::error!("Folder {}: backfill chunk failed: {}", folder_path, e);
                }
            }
        }

        // Reconcile: delete stale messages no longer on server
        {
            let cache = self.cache.lock().await;
            let _ = cache.delete_stale_messages(account.id, folder_path, &all_uids);

            // Recount unread after full sync
            let unread = cache.count_unread(account.id, folder_path)?;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!("Folder {}: backfill complete", folder_path);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Two-phase windowed sync (async-imap session)
    // -----------------------------------------------------------------------

    async fn sync_folder_windowed_imap(
        &self,
        account: &Account,
        folder_path: &str,
        session: &mut async_imap::Session<crate::imap::CompatTls>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1: Fetch recent messages for instant display
        let recent = crate::imap::fetch_recent_summaries(session, folder_path, FIRST_PAGE_SIZE).await?;

        {
            let cache = self.cache.lock().await;
            for msg in &recent {
                let _ = cache.upsert_message(
                    account.id, folder_path, msg.uid,
                    &msg.subject, &msg.sender, msg.date,
                    msg.is_read, msg.is_starred, msg.has_attachments, &msg.preview,
                );
            }
            let unread = recent.iter().filter(|m| !m.is_read).count() as i32;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!(
            "Folder {}: {} recent messages cached (phase 1)",
            folder_path, recent.len()
        );

        // Phase 2: Backfill remaining messages in chunks
        let all_uids = crate::imap::fetch_all_uids(session, folder_path).await?;
        let cached_uids: Vec<i64> = {
            let cache = self.cache.lock().await;
            cache.load_cached_uids(account.id, folder_path)?
        };
        let cached_set: std::collections::HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids.iter().copied()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            tracing::info!("Folder {}: already fully cached", folder_path);
            return Ok(());
        }

        tracing::info!(
            "Folder {}: {} total, {} cached, {} missing — backfilling",
            folder_path, all_uids.len(), cached_set.len(), missing.len()
        );

        for chunk in missing.chunks(BACKFILL_CHUNK) {
            let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
            let uid_set_str = uid_set.join(",");

            match crate::imap::fetch_summaries_by_uids(session, &uid_set_str).await {
                Ok(summaries) => {
                    let cache = self.cache.lock().await;
                    for msg in &summaries {
                        let _ = cache.upsert_message(
                            account.id, folder_path, msg.uid,
                            &msg.subject, &msg.sender, msg.date,
                            msg.is_read, msg.is_starred, msg.has_attachments, &msg.preview,
                        );
                    }
                    tracing::debug!(
                        "Folder {}: backfilled chunk of {} messages",
                        folder_path, summaries.len()
                    );
                }
                Err(e) => {
                    tracing::error!("Folder {}: backfill chunk failed: {}", folder_path, e);
                }
            }
        }

        // Reconcile: delete stale messages no longer on server
        {
            let cache = self.cache.lock().await;
            let _ = cache.delete_stale_messages(account.id, folder_path, &all_uids);

            let unread = cache.count_unread(account.id, folder_path)?;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!("Folder {}: backfill complete", folder_path);
        Ok(())
    }
}
