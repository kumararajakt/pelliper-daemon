use crate::cache::{Account, Cache};
use crate::oauth;
use crate::secrets::Secrets;

const FIRST_PAGE_SIZE: usize = 200;
const BACKFILL_CHUNK: usize = 1000;
const PREFETCH_BODY_LIMIT: usize = 50;

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

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

    /// Sync a single account with retry on failure.
    pub async fn sync_account(
        &self,
        account: &Account,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(
            "Starting sync for {} ({})",
            account.email,
            account.auth_type
        );

        // Attempt 1
        match self.try_sync_account(account).await {
            Ok(()) => {
                tracing::info!("Sync completed for {}", account.email);
                return Ok(());
            }
            Err(e) => {
                tracing::warn!("Sync attempt 1 failed for {}: {}", account.email, e);
            }
        }

        // Attempt 2: fresh connection
        tracing::info!("Retrying sync for {} with fresh connection", account.email);
        match self.try_sync_account(account).await {
            Ok(()) => {
                tracing::info!("Sync completed for {} (retry)", account.email);
                Ok(())
            }
            Err(e) => {
                tracing::error!("Sync attempt 2 failed for {}: {}", account.email, e);
                Err(e)
            }
        }
    }

    /// Single attempt to sync an account.
    async fn try_sync_account(
        &self,
        account: &Account,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let host = account.imap_host.as_deref().unwrap_or("localhost");
        let port = account.imap_port.unwrap_or(993) as u16;
        let email = &account.email;

        let key_name = format!("pelliper:{}", email);
        let credential = self.secrets.get_password(&key_name).await?;

        if account.auth_type == "oauth" {
            self.sync_account_oauth(account, host, port, email, &credential)
                .await
        } else {
            self.sync_account_password(account, host, port, email, &credential)
                .await
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
                return Err("No refresh token configured. Re-add the account.".into());
            }
        };

        let client_id = std::env::var("PELLIPER_GOOGLE_CLIENT_ID").unwrap_or_else(|_| {
            "780679832751-ckm5ak0vjhl6f7uj1h8b0lp63tnb7b3j.apps.googleusercontent.com".to_string()
        });
        let client_secret = std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").unwrap_or_default();

        let token = oauth::refresh_access_token(
            "https://oauth2.googleapis.com/token",
            &client_id,
            &client_secret,
            &refresh_token,
        )
        .await?;

        tracing::info!("Refreshed access token for {}", email);

        let mut session =
            crate::imap::connect_session_oauth(host, port, email, &token.access_token).await?;

        let folders = session.list_folders().await?;
        tracing::info!("Found {} folders for {} (oauth)", folders.len(), email);

        {
            let cache = self.cache.lock().await;
            for folder in &folders {
                let _ = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect);
            }
        }

        for folder in &folders {
            if folder.noselect {
                continue;
            }
            if let Err(e) = self
                .sync_folder_windowed_raw(account, &folder.path, &mut session)
                .await
            {
                tracing::error!(
                    "Folder {} failed: {} — retrying with fresh session",
                    folder.path,
                    e
                );
                // Retry this folder with a fresh session
                if let Ok(mut fresh) =
                    crate::imap::connect_session_oauth(host, port, email, &token.access_token).await
                {
                    if let Err(e2) = self
                        .sync_folder_windowed_raw(account, &folder.path, &mut fresh)
                        .await
                    {
                        tracing::error!("Folder {} retry also failed: {}", folder.path, e2);
                    }
                    fresh.logout().await;
                }
            }
        }

        session.logout().await;
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
        let mut session =
            crate::imap::connect_session_password(host, port, email, password).await?;

        let folders = crate::imap::list_folders(&mut session).await?;
        tracing::info!("Found {} folders for {} (password)", folders.len(), email);

        {
            let cache = self.cache.lock().await;
            for folder in &folders {
                let _ = cache.upsert_folder(account.id, &folder.path, 0, folder.noselect);
            }
        }

        for folder in &folders {
            if folder.noselect {
                continue;
            }
            if let Err(e) = self
                .sync_folder_windowed_imap(account, &folder.path, &mut session)
                .await
            {
                tracing::error!(
                    "Folder {} failed: {} — retrying with fresh session",
                    folder.path,
                    e
                );
                if let Ok(mut fresh) =
                    crate::imap::connect_session_password(host, port, email, password).await
                {
                    if let Err(e2) = self
                        .sync_folder_windowed_imap(account, &folder.path, &mut fresh)
                        .await
                    {
                        tracing::error!("Folder {} retry also failed: {}", folder.path, e2);
                    }
                    crate::imap::logout_session(&mut fresh).await;
                }
            }
        }

        crate::imap::logout_session(&mut session).await;
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
        let recent = session
            .fetch_recent_summaries(folder_path, FIRST_PAGE_SIZE)
            .await?;

        {
            let cache = self.cache.lock().await;
            for msg in &recent {
                let _ = cache.upsert_message(
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
                    &msg.message_id,
                    &msg.references,
                );
            }
            let unread = recent.iter().filter(|m| !m.is_read).count() as i32;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!(
            "Folder {}: {} recent messages cached (phase 1)",
            folder_path,
            recent.len()
        );

        // Phase 2: Backfill remaining messages in chunks
        let all_uids = session.fetch_all_uids(folder_path).await?;
        let cached_uids: Vec<i64> = {
            let cache = self.cache.lock().await;
            cache.load_cached_uids(account.id, folder_path)?
        };
        let cached_set: std::collections::HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids
            .iter()
            .copied()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            tracing::info!("Folder {}: already fully cached", folder_path);
            return Ok(());
        }

        tracing::info!(
            "Folder {}: {} total, {} cached, {} missing — backfilling",
            folder_path,
            all_uids.len(),
            cached_set.len(),
            missing.len()
        );

        for chunk in missing.chunks(BACKFILL_CHUNK) {
            let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
            let uid_set_str = uid_set.join(",");

            match session.fetch_summaries_by_uids(&uid_set_str).await {
                Ok(summaries) => {
                    let cache = self.cache.lock().await;
                    for msg in &summaries {
                        let _ = cache.upsert_message(
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
                            &msg.message_id,
                            &msg.references,
                        );
                    }
                    tracing::debug!(
                        "Folder {}: backfilled chunk of {} messages",
                        folder_path,
                        summaries.len()
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

        // Prefetch bodies for the most recent messages
        self.prefetch_bodies_raw(account, folder_path, session)
            .await;

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
        let recent =
            crate::imap::fetch_recent_summaries(session, folder_path, FIRST_PAGE_SIZE).await?;

        {
            let cache = self.cache.lock().await;
            for msg in &recent {
                let _ = cache.upsert_message(
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
                    &msg.message_id,
                    &msg.references,
                );
            }
            let unread = recent.iter().filter(|m| !m.is_read).count() as i32;
            let _ = cache.update_folder_unread(account.id, folder_path, unread);
        }

        tracing::info!(
            "Folder {}: {} recent messages cached (phase 1)",
            folder_path,
            recent.len()
        );

        // Phase 2: Backfill remaining messages in chunks
        let all_uids = crate::imap::fetch_all_uids(session, folder_path).await?;
        let cached_uids: Vec<i64> = {
            let cache = self.cache.lock().await;
            cache.load_cached_uids(account.id, folder_path)?
        };
        let cached_set: std::collections::HashSet<i64> = cached_uids.into_iter().collect();
        let missing: Vec<i64> = all_uids
            .iter()
            .copied()
            .filter(|uid| !cached_set.contains(uid))
            .collect();

        if missing.is_empty() {
            tracing::info!("Folder {}: already fully cached", folder_path);
            return Ok(());
        }

        tracing::info!(
            "Folder {}: {} total, {} cached, {} missing — backfilling",
            folder_path,
            all_uids.len(),
            cached_set.len(),
            missing.len()
        );

        for chunk in missing.chunks(BACKFILL_CHUNK) {
            let uid_set: Vec<String> = chunk.iter().map(|u| u.to_string()).collect();
            let uid_set_str = uid_set.join(",");

            match crate::imap::fetch_summaries_by_uids(session, &uid_set_str).await {
                Ok(summaries) => {
                    let cache = self.cache.lock().await;
                    for msg in &summaries {
                        let _ = cache.upsert_message(
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
                            &msg.message_id,
                            &msg.references,
                        );
                    }
                    tracing::debug!(
                        "Folder {}: backfilled chunk of {} messages",
                        folder_path,
                        summaries.len()
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

    async fn prefetch_bodies_raw(
        &self,
        account: &Account,
        folder_path: &str,
        session: &mut crate::imap::RawImapSession,
    ) {
        let recent_uids: Vec<i64> = {
            let cache = self.cache.lock().await;
            match cache.load_recent_uids(account.id, folder_path, PREFETCH_BODY_LIMIT) {
                Ok(uids) => uids,
                Err(e) => {
                    tracing::error!("Failed to load recent UIDs for prefetch: {}", e);
                    return;
                }
            }
        };

        let to_fetch: Vec<i64> = {
            let cache = self.cache.lock().await;
            recent_uids
                .iter()
                .copied()
                .filter(|uid| match cache.has_body(account.id, folder_path, *uid) {
                    Ok(has) => !has,
                    Err(_) => true, // on error, try fetching anyway
                })
                .collect()
        };

        if to_fetch.is_empty() {
            tracing::debug!(
                "Folder {}: all {} recent bodies already cached",
                folder_path,
                PREFETCH_BODY_LIMIT
            );
            return;
        }

        tracing::info!(
            "Folder {}: prefetching {} bodies ({} already cached)",
            folder_path,
            to_fetch.len(),
            recent_uids.len() - to_fetch.len()
        );

        if let Err(e) = session.select_folder(folder_path).await {
            tracing::error!(
                "Folder {}: SELECT failed for body prefetch: {}",
                folder_path,
                e
            );
            return;
        }

        // Attempt 1
        match session.fetch_bodies_batch(&to_fetch).await {
            Ok(bodies) => {
                self.cache_prefetched_bodies(account, folder_path, &bodies);
                tracing::info!("Folder {}: prefetched {} bodies", folder_path, bodies.len());
            }
            Err(e) => {
                tracing::warn!(
                    "Folder {}: body prefetch failed: {} — retrying",
                    folder_path,
                    e
                );
                // Retry: re-select and fetch again
                if let Ok(()) = session.select_folder(folder_path).await {
                    if let Ok(bodies) = session.fetch_bodies_batch(&to_fetch).await {
                        self.cache_prefetched_bodies(account, folder_path, &bodies);
                        tracing::info!(
                            "Folder {}: prefetched {} bodies (retry)",
                            folder_path,
                            bodies.len()
                        );
                    }
                }
            }
        }
    }

    fn cache_prefetched_bodies(
        &self,
        account: &Account,
        folder_path: &str,
        bodies: &[(i64, Vec<u8>)],
    ) {
        // Use tokio::sync::Mutex directly — this runs in async context
        let cache_ref = &self.cache;
        // We can't .await here (sync fn), so parse bodies outside the lock
        let parsed_bodies: Vec<(i64, String, bool)> = bodies
            .iter()
            .map(|(uid, raw)| {
                let parsed = crate::imap::parse_message_body(raw);
                let html = if parsed.html.is_empty() {
                    format!("<pre>{}</pre>", html_escape(&parsed.text))
                } else {
                    parsed.html
                };
                (*uid, html, parsed.has_attachments)
            })
            .collect();

        // Spawn to handle cache writes asynchronously
        let cache = cache_ref.clone();
        let account_id = account.id;
        let folder = folder_path.to_string();
        tokio::spawn(async move {
            let cache = cache.lock().await;
            for (uid, html, has_attachments) in &parsed_bodies {
                let _ = cache.save_body(account_id, &folder, *uid, html);
                let _ = cache.update_has_attachment(account_id, &folder, *uid, *has_attachments);
            }
        });
    }
}
