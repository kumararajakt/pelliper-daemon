use crate::cache::Cache;
use crate::secrets::Secrets;
use crate::types::{Account, DaemonSignal, EmailAttachment, SignalSender};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) async fn send_with_auth(
    secrets: &Secrets,
    account: &Account,
    to: &str,
    cc: &str,
    bcc: &str,
    reply_to: &str,
    subject: &str,
    body: &str,
    in_reply_to: Option<&str>,
    references: Option<&str>,
    attachments: &[EmailAttachment],
) -> Result<(), String> {
    let auth: String = if account.auth_type == "oauth" {
        let refresh_token = account
            .refresh_token
            .as_deref()
            .ok_or_else(|| "No refresh token stored for account".to_string())?;
        let provider = crate::oauth::provider_for(account)?;
        let (client_id, client_secret) = crate::oauth::client_credentials(provider)?;
        let token = crate::oauth::refresh_access_token(
            provider.token_endpoint,
            &client_id,
            &client_secret,
            refresh_token,
        )
        .await
        .map_err(|e| format!("Failed to refresh access token: {}", e))?;
        token.access_token
    } else {
        let key_name = format!("pelliper:{}", account.email);
        secrets
            .get_password(&key_name)
            .await
            .map_err(|e| format!("Failed to retrieve stored password: {}", e))?
    };

    crate::smtp::send_email(
        account,
        to,
        cc,
        Some(bcc),
        Some(reply_to),
        subject,
        body,
        &auth,
        in_reply_to,
        references,
        attachments,
    )
    .await
    .map_err(|e| e.to_string())
}

const OUTBOX_FLUSH_INTERVAL_SECS: u64 = 30;

const MAX_OUTBOX_ATTEMPTS: i32 = 10;

pub(crate) async fn outbox_flusher(
    cache: Arc<Mutex<Cache>>,
    secrets: Secrets,
    signal_tx: Option<SignalSender>,
) {
    loop {
        let items = cache.lock().await.load_pending_outbox().unwrap_or_default();
        for item in items {
            let account = {
                let cache = cache.lock().await;
                cache.load_account(item.account_id).ok().flatten()
            };
            let account = match account {
                Some(a) => a,
                None => {
                    tracing::warn!(
                        "outbox: account {} for item {} no longer exists, dropping",
                        item.account_id,
                        item.id
                    );
                    let _ = cache.lock().await.delete_outbox(item.id);
                    continue;
                }
            };

            match send_with_auth(
                &secrets,
                &account,
                &item.to_addr,
                item.cc_addr.as_deref().unwrap_or(""),
                item.bcc_addr.as_deref().unwrap_or(""),
                item.reply_to.as_deref().unwrap_or(""),
                &item.subject,
                &item.body,
                None,
                None,
                &[],
            )
            .await
            {
                Ok(()) => {
                    tracing::info!("outbox: flushed item {} to {}", item.id, item.to_addr);
                    let _ = cache.lock().await.delete_outbox(item.id);
                    if let Some(ref tx) = signal_tx {
                        let _ = tx.send(DaemonSignal::OutboxItemSent {
                            account_id: item.account_id,
                            item_id: item.id,
                        });
                    }
                }
                Err(e) => {
                    let attempts = {
                        let cache = cache.lock().await;
                        cache.increment_outbox_attempts(item.id).unwrap_or(0)
                    };
                    if attempts >= MAX_OUTBOX_ATTEMPTS {
                        tracing::error!(
                            "outbox: item {} hit {} attempts, dropping: {}",
                            item.id,
                            MAX_OUTBOX_ATTEMPTS,
                            e
                        );
                        let _ = cache.lock().await.delete_outbox(item.id);
                    } else {
                        tracing::warn!(
                            "outbox: retry {} for item {} failed: {}",
                            attempts,
                            item.id,
                            e
                        );
                    }
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(OUTBOX_FLUSH_INTERVAL_SECS)).await;
    }
}
