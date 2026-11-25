use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use lettre::message::{Attachment, Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::Database;
use crate::ipc::protocol::RpcNotification;
use crate::oauth::refresh_if_needed;
use crate::secrets::{load_secret, AccountSecret};

#[derive(Debug, Clone)]
pub enum SmtpAuth {
    Password { user: String, password: String },
    XOAuth2 { user: String, access_token: String },
}

#[derive(Debug, Deserialize)]
struct OutboxPayload {
    #[serde(default)]
    to: String,
    #[serde(default)]
    cc: String,
    #[serde(default)]
    bcc: String,
    #[serde(default)]
    subject: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    attachments: Vec<OutboxAttachment>,
}

#[derive(Debug, Deserialize)]
struct OutboxAttachment {
    #[serde(default)]
    filename: String,
    #[serde(default)]
    local_path: String,
}

fn build_transport(host: &str, port: u16, tls_mode: &str, auth: &SmtpAuth) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    let builder = if tls_mode.eq_ignore_ascii_case("starttls") {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
            .with_context(|| format!("invalid SMTP STARTTLS host: {host}"))?
            .port(port)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::relay(host)
            .with_context(|| format!("invalid SMTP TLS host: {host}"))?
            .port(port)
    };

    let transport: AsyncSmtpTransport<Tokio1Executor> = match auth {
        SmtpAuth::Password { user, password } => {
            builder.credentials(Credentials::new(user.clone(), password.clone())).build()
        }
        SmtpAuth::XOAuth2 { user, access_token } => builder
            .authentication(vec![Mechanism::Xoauth2])
            .credentials(Credentials::new(user.clone(), access_token.clone()))
            .build(),
    };
    Ok(transport)
}

/// Tests SMTP connectivity and authentication without sending a message.
pub async fn test_smtp_connection(host: &str, port: u16, tls_mode: &str, auth: &SmtpAuth) -> Result<()> {
    let transport = build_transport(host, port, tls_mode, auth)?;
    let ok = transport
        .test_connection()
        .await
        .context("SMTP connection test failed")?;
    anyhow::ensure!(ok, "SMTP server rejected connection/credentials");
    Ok(())
}

/// Background worker that drains SQLite `outbox` and sends queued emails via SMTP.
pub async fn run_outbox_worker(
    db: Database,
    events: broadcast::Sender<RpcNotification>,
    mut wake_rx: mpsc::Receiver<()>,
    cancel: CancellationToken,
) {
    let poll_interval = Duration::from_secs(30);
    flush_outbox(&db, &events).await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("SmtpOutboxWorker shutting down");
                break;
            }
            _ = tokio::time::sleep(poll_interval) => {
                flush_outbox(&db, &events).await;
            }
            cmd = wake_rx.recv() => {
                if cmd.is_none() {
                    break;
                }
                flush_outbox(&db, &events).await;
            }
        }
    }
}

async fn flush_outbox(db: &Database, events: &broadcast::Sender<RpcNotification>) {
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let items = db.list_ready_outbox(now_ts).unwrap_or_default();
    for item in items {
        let res = send_single_outbox(db, item.account_id, &item.payload_json).await;
        match res {
            Ok(()) => {
                let _ = db.delete_outbox(item.id);
                let _ = events.send(RpcNotification::new(
                    "outbox.status",
                    json!({ "outbox_id": item.id, "status": "sent" }),
                ));
            }
            Err(e) => {
                let err_str = e.to_string();
                warn!(outbox_id = item.id, "SMTP send failed: {err_str}");
                let next_retry = now_ts + (30 * (item.retry_count + 1));
                let _ = db.mark_outbox_retry(item.id, item.retry_count + 1, next_retry, &err_str);
                let status = if item.retry_count + 1 >= 5 { "failed" } else { "retrying" };
                let _ = events.send(RpcNotification::new(
                    "outbox.status",
                    json!({ "outbox_id": item.id, "status": status, "error": err_str }),
                ));
            }
        }
    }
}

async fn send_single_outbox(db: &Database, account_id: i64, payload_json: &str) -> Result<()> {
    let acc = db
        .get_account(account_id)?
        .context("account not found for outbox item")?;
    let raw_secret = load_secret(&acc.email)
        .await?
        .context("credentials not found in keyring")?;
    let secret = if let Some(ref prov) = acc.oauth_provider {
        refresh_if_needed(&acc.email, prov, raw_secret).await?
    } else {
        raw_secret
    };

    let auth = match secret {
        AccountSecret::Password { password } => SmtpAuth::Password {
            user: acc.email.clone(),
            password,
        },
        AccountSecret::OAuth2 { access_token, .. } => SmtpAuth::XOAuth2 {
            user: acc.email.clone(),
            access_token,
        },
    };

    let payload: OutboxPayload = serde_json::from_str(payload_json)?;
    let from_mb: Mailbox = format!("{} <{}>", acc.display_name, acc.email).parse()?;
    let mut builder = Message::builder().from(from_mb).subject(&payload.subject);

    for addr in payload.to.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        builder = builder.to(addr.parse()?);
    }
    for addr in payload.cc.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        builder = builder.cc(addr.parse()?);
    }
    for addr in payload.bcc.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        builder = builder.bcc(addr.parse()?);
    }

    let message = if payload.attachments.is_empty() {
        builder.body(payload.body)?
    } else {
        let mut mp = MultiPart::mixed().singlepart(SinglePart::plain(payload.body));
        for att in &payload.attachments {
            if !att.local_path.is_empty() {
                if let Ok(bytes) = std::fs::read(&att.local_path) {
                    let fname = if att.filename.is_empty() {
                        "attachment".to_string()
                    } else {
                        att.filename.clone()
                    };
                    let ct = "application/octet-stream".parse().unwrap();
                    mp = mp.singlepart(Attachment::new(fname).body(bytes, ct));
                }
            }
        }
        builder.multipart(mp)?
    };

    let transport = build_transport(&acc.smtp_host, acc.smtp_port, &acc.smtp_tls, &auth)?;
    transport.send(message).await?;
    Ok(())
}
