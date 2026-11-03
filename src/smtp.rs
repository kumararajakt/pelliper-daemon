use crate::types::{Account, EmailAttachment};
use lettre::message::header::ContentType;
use lettre::message::Attachment;
use lettre::message::Mailbox;
use lettre::message::{Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

pub fn split_recipient_list(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut angle = 0i32;
    for ch in value.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                current.push(ch);
            }
            '<' if !quoted => {
                angle += 1;
                current.push(ch);
            }
            '>' if !quoted => {
                angle = (angle - 1).max(0);
                current.push(ch);
            }
            ',' | ';' if !quoted && angle == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts
}

#[allow(clippy::too_many_arguments)]
pub fn build_message(
    from_email: &str,
    to: &str,
    cc: &str,
    bcc: Option<&str>,
    reply_to: Option<&str>,
    subject: &str,
    body: &str,
    in_reply_to: Option<&str>,
    references: Option<&str>,
    attachments: &[EmailAttachment],
) -> Result<Message, Box<dyn std::error::Error + Send + Sync>> {
    let from: Mailbox = from_email
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
        builder = builder.reply_to(r.trim().parse().map_err(
            |e: lettre::address::AddressError| format!("Invalid Reply-To '{}': {}", r, e),
        )?);
    }

    for recipient in split_recipient_list(to) {
        let addr: Mailbox = recipient
            .parse()
            .map_err(|e: lettre::address::AddressError| {
                format!("Invalid recipient '{}': {}", recipient, e)
            })?;
        builder = builder.to(addr);
    }

    for recipient in split_recipient_list(cc) {
        let addr: Mailbox = recipient
            .parse()
            .map_err(|e: lettre::address::AddressError| {
                format!("Invalid Cc '{}': {}", recipient, e)
            })?;
        builder = builder.cc(addr);
    }

    if let Some(bcc) = bcc {
        for recipient in split_recipient_list(bcc) {
            let addr: Mailbox = recipient
                .parse()
                .map_err(|e: lettre::address::AddressError| {
                    format!("Invalid Bcc '{}': {}", recipient, e)
                })?;
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

    if attachments.is_empty() {
        builder
            .multipart(body_part)
            .map_err(|e| format!("Failed to build message: {}", e).into())
    } else {
        let mut mixed = MultiPart::mixed().multipart(body_part);
        for att in attachments {
            let data = std::fs::read(&att.path)
                .map_err(|e| format!("Failed to read attachment '{}': {}", att.filename, e))?;
            let mime_type = mime_guess::from_path(&att.path).first_or_octet_stream();
            let content_type =
                ContentType::parse(mime_type.essence_str()).unwrap_or(ContentType::TEXT_PLAIN);
            let part = Attachment::new(att.filename.clone()).body(data, content_type);
            mixed = mixed.singlepart(part);
        }
        builder
            .multipart(mixed)
            .map_err(|e| format!("Failed to build message: {}", e).into())
    }
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
    let email = build_message(
        &account.email,
        to,
        cc,
        bcc,
        reply_to,
        subject,
        body,
        in_reply_to,
        references,
        attachments,
    )?;

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
            .credentials(Credentials::new(account.email.clone(), auth.to_string()))
            .authentication(vec![Mechanism::Xoauth2])
            .build()
    } else {
        builder
            .credentials(Credentials::new(account.email.clone(), auth.to_string()))
            .build()
    };

    mailer.send(email).await?;
    Ok(())
}

#[cfg(test)]
#[path = "smtp.test.rs"]
mod tests;
