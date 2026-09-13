use crate::cache::Account;
use lettre::message::Mailbox;
use lettre::message::{Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

/// Build and send an email through the account's SMTP relay.
///
/// `auth`: for OAuth accounts the XOAUTH2 access token (fresh — caller has
/// already refreshed it); for password accounts the account's password.
///
/// `in_reply_to` / `references` are optional threading headers; the values are
/// used verbatim (the original message-id / references chain from the cache,
/// already including angle brackets and separated by spaces).
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

    // Prefer a plain-text part, but send HTML when the body looks like markup
    // (e.g. the message view's `<pre>` / HTML bodies).
    let email = if body.trim_start().starts_with('<') {
        builder.multipart(
            MultiPart::alternative()
                .singlepart(SinglePart::plain(body.to_string()))
                .singlepart(SinglePart::html(body.to_string())),
        )
    } else {
        builder.body(body.to_string())
    }
    .map_err(|e| format!("Failed to build message: {}", e))?;

    let host = account
        .smtp_host
        .as_deref()
        .ok_or("Account has no SMTP host configured")?;
    let port = account.smtp_port.unwrap_or(587) as u16;

    // starttls_relay uses STARTTLS (port 587, the common default). If the
    // account explicitly wants implicit TLS, switch to `relay` (port 465).
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
                auth.to_string(), // access token used as the "secret"
            ))
            // Gmail requires the non-standard XOAUTH2 mechanism only.
            .authentication(vec![Mechanism::Xoauth2])
            .build()
    } else {
        builder.credentials(Credentials::new(account.email.clone(), auth.to_string())).build()
    };

    mailer.send(email).await?;
    Ok(())
}