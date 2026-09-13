use crate::cache::Account;
use lettre::message::header::ContentType;
use lettre::message::Mailbox;
use lettre::message::{Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailAttachment {
    pub path: String,
    pub filename: String,
}

pub async fn send_email(
    account: &Account,
    to: &str,
    cc: &str,
    bcc: Option<&str>,
    reply_to: Option<&str>,
    subject: &str,
    body: &str,
    auth: &str,
    in_reply_to: Option<&str>,
    references: Option<&str>,
    attachments: &[EmailAttachment],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let from: Mailbox = account
        .email
        .parse()
        .map_err(|e: lettre::address::AddressError| format!("Invalid from address: {}", e))?;

    let mut builder = Message::builder()
        .from(from)
        .subject(subject.trim().to_string());

    if let Some(mid) = in_reply_to.filter(|s| !s.is_empty()) {
        builder = builder.in_reply_to(mid.to_string());
    }
    if let Some(refs) = references.filter(|s| !s.is_empty()) {
        builder = builder.references(refs.to_string());
    }
    if let Some(r) = reply_to.filter(|s| !s.is_empty()) {
        builder = builder.reply_to(
            r.trim().parse().map_err(|e: lettre::address::AddressError| {
                format!("Invalid Reply-To '{}': {}", r, e)
            })?,
        );
    }

    for recipient in to.split([',', ';']) {
        let addr: Mailbox = recipient
            .trim()
            .parse()
            .map_err(|e: lettre::address::AddressError| format!("Invalid recipient '{}': {}", recipient, e))?;
        builder = builder.to(addr);
    }

    for recipient in cc.split([',', ';']) {
        let addr: Mailbox = recipient
            .trim()
            .parse()
            .map_err(|e: lettre::address::AddressError| format!("Invalid Cc '{}': {}", recipient, e))?;
        builder = builder.cc(addr);
    }

    if let Some(bcc) = bcc {
        for recipient in bcc.split([',', ';']) {
            let addr: Mailbox = recipient
                .trim()
                .parse()
                .map_err(|e: lettre::address::AddressError| format!("Invalid Bcc '{}': {}", recipient, e))?;
            builder = builder.bcc(addr);
        }
    }

    let body_part = if body.trim_start().starts_with('<') {
        MultiPart::alternative()
            .singlepart(SinglePart::plain(body.to_string()))
            .singlepart(SinglePart::html(body.to_string()))
    } else {
        MultiPart::alternative()
            .singlepart(SinglePart::plain(body.to_string()))
            .singlepart(SinglePart::html(format!("<pre>{}</pre>", body)))
    };

    let email = if attachments.is_empty() {
        builder
            .multipart(body_part)
            .map_err(|e| format!("Failed to build message: {}", e))?
    } else {
        let mut mixed = body_part;
        for att in attachments {
            let data = std::fs::read(&att.path)
                .map_err(|e| format!("Failed to read attachment '{}': {}", att.filename, e))?;
            let mime_type = mime_guess::from_path(&att.path)
                .first_or_octet_stream();
            let content_type = ContentType::parse(mime_type.essence_str())
                .unwrap_or(ContentType::TEXT_PLAIN);
            let mime_part = SinglePart::builder()
                .header(content_type)
                .body(data);
            mixed = mixed.singlepart(mime_part);
        }
        builder
            .multipart(mixed)
            .map_err(|e| format!("Failed to build message: {}", e))?
    };

    let host = account
        .smtp_host
        .as_deref()
        .ok_or("Account has no SMTP host configured")?;
    let port = account.smtp_port.unwrap_or(587) as u16;

    let security = account.smtp_security.as_deref().unwrap_or("starttls");
    let builder = if security == "ssl" || security == "tls" {
        AsyncSmtpTransport::<Tokio1Executor>::relay(host)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
    }
    .map_err(|e| format!("Failed to configure SMTP transport for {}: {}", host, e))?
    .port(port);

    let mailer = if account.auth_type == "oauth" {
        builder
            .credentials(Credentials::new(
                account.email.clone(),
                auth.to_string(),
            ))
            .authentication(vec![Mechanism::Xoauth2])
            .build()
    } else {
        builder.credentials(Credentials::new(account.email.clone(), auth.to_string())).build()
    };

    mailer.send(email).await?;
    Ok(())
}
