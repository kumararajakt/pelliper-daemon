use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::repo::AccountRow;
use crate::imap::client::{connect_imap_tls, ImapAuth};
use crate::imap::sync_worker::SyncWakeCommand;

const IDLE_REFRESH_SECS: u64 = 20 * 60;

/// Dedicated per-account IMAP IDLE worker on INBOX.
pub async fn run_idle_worker(
    account: AccountRow,
    auth: ImapAuth,
    wake_tx: mpsc::Sender<SyncWakeCommand>,
    cancel: CancellationToken,
) {
    let mut backoff_secs = 2u64;

    loop {
        if cancel.is_cancelled() {
            info!(account_id = account.id, "IdleWorker shutting down");
            break;
        }

        match connect_imap_tls(&account.imap_host, account.imap_port, &auth).await {
            Ok(mut session) => {
                backoff_secs = 2;
                if let Err(e) = session.select("INBOX").await {
                    warn!(account_id = account.id, "IdleWorker SELECT INBOX failed: {e}");
                    let _ = session.logout().await;
                } else {
                    let mut idle = session.idle();
                    if let Err(e) = idle.init().await {
                        warn!(account_id = account.id, "IdleWorker IDLE init failed: {e}");
                    } else {
                        let (idle_wait, _interrupt) = idle.wait_with_timeout(Duration::from_secs(IDLE_REFRESH_SECS));
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                info!(account_id = account.id, "IdleWorker cancelled during IDLE");
                                break;
                            }
                            res = idle_wait => {
                                match res {
                                    Ok(async_imap::extensions::idle::IdleResponse::NewData(_)) => {
                                        let _ = wake_tx.send(SyncWakeCommand::IdleEvent).await;
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!(account_id = account.id, "IdleWorker wait error: {e}");
                                    }
                                }
                            }
                        }
                        if let Ok(mut sess) = idle.done().await {
                            let _ = sess.logout().await;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(account_id = account.id, "IdleWorker connect failed: {e}; retrying in {backoff_secs}s");
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}
