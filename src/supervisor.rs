use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::db::repo::AccountRow;
use crate::db::Database;
use crate::imap::client::ImapAuth;
use crate::imap::idle_worker::run_idle_worker;
use crate::imap::sync_worker::{run_sync_worker, SyncWakeCommand};
use crate::ipc::protocol::RpcNotification;
use crate::oauth::refresh_if_needed;
use crate::secrets::{load_secret, AccountSecret};

struct AccountHandle {
    cancel: CancellationToken,
    wake_tx: mpsc::Sender<SyncWakeCommand>,
}

#[derive(Clone)]
pub struct AccountSupervisor {
    db: Database,
    events: broadcast::Sender<RpcNotification>,
    handles: Arc<Mutex<HashMap<i64, AccountHandle>>>,
}

impl AccountSupervisor {
    pub fn new(db: Database, events: broadcast::Sender<RpcNotification>) -> Self {
        Self {
            db,
            events,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_all_enabled(&self) {
        if let Ok(accounts) = self.db.list_accounts() {
            for acc in accounts.into_iter().filter(|a| a.enabled) {
                self.spawn_account(acc).await;
            }
        }
    }

    pub async fn spawn_account(&self, account: AccountRow) {
        self.stop_account(account.id);

        let raw_secret = match load_secret(&account.email).await {
            Ok(Some(s)) => s,
            _ => return,
        };
        let secret = if let Some(ref prov) = account.oauth_provider {
            refresh_if_needed(&account.email, prov, raw_secret.clone()).await.unwrap_or(raw_secret)
        } else {
            raw_secret
        };

        let auth = match secret {
            AccountSecret::Password { password } => ImapAuth::Password {
                user: account.email.clone(),
                password,
            },
            AccountSecret::OAuth2 { access_token, .. } => ImapAuth::XOAuth2 {
                user: account.email.clone(),
                access_token,
            },
        };

        let cancel = CancellationToken::new();
        let (wake_tx, wake_rx) = mpsc::channel(64);

        {
            let mut map = self.handles.lock().expect("supervisor mutex poisoned");
            map.insert(
                account.id,
                AccountHandle {
                    cancel: cancel.clone(),
                    wake_tx: wake_tx.clone(),
                },
            );
        }

        info!(account_id = account.id, email = %account.email, "Spawning SyncWorker and IdleWorker");

        tokio::spawn(run_sync_worker(
            account.clone(),
            auth.clone(),
            self.db.clone(),
            self.events.clone(),
            wake_rx,
            cancel.clone(),
        ));

        tokio::spawn(run_idle_worker(account, auth, wake_tx, cancel));
    }

    pub fn stop_account(&self, account_id: i64) {
        let mut map = self.handles.lock().expect("supervisor mutex poisoned");
        if let Some(handle) = map.remove(&account_id) {
            handle.cancel.cancel();
        }
    }

    pub fn wake_account(&self, account_id: i64, cmd: SyncWakeCommand) {
        let map = self.handles.lock().expect("supervisor mutex poisoned");
        if let Some(handle) = map.get(&account_id) {
            let _ = handle.wake_tx.try_send(cmd);
        }
    }

    pub fn shutdown_all(&self) {
        let mut map = self.handles.lock().expect("supervisor mutex poisoned");
        for (_, handle) in map.drain() {
            handle.cancel.cancel();
        }
    }
}
