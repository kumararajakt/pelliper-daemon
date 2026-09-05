use crate::cache::Cache;

pub struct SyncEngine {
    cache: Cache,
}

impl SyncEngine {
    pub fn new(cache: Cache) -> Self {
        Self { cache }
    }

    pub async fn sync_folder(&mut self, account_id: i64, folder: &str) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: IMAP sync logic
        // 1. Connect to IMAP
        // 2. SELECT folder
        // 3. FETCH new messages
        // 4. Update cache
        tracing::info!("Syncing folder {} for account {}", folder, account_id);
        Ok(())
    }

    pub async fn sync_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let accounts = self.cache.load_accounts()?;
        for account in &accounts {
            if account.enabled {
                self.sync_folder(account.id, "INBOX").await?;
            }
        }
        Ok(())
    }
}
