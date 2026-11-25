use anyhow::{Context, Result};
use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use lettre::{AsyncSmtpTransport, Tokio1Executor};

#[derive(Debug, Clone)]
pub enum SmtpAuth {
    Password { user: String, password: String },
    XOAuth2 { user: String, access_token: String },
}

/// Tests SMTP connectivity and authentication without sending a message.
pub async fn test_smtp_connection(host: &str, port: u16, tls_mode: &str, auth: &SmtpAuth) -> Result<()> {
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

    let ok = transport
        .test_connection()
        .await
        .context("SMTP connection test failed")?;
    anyhow::ensure!(ok, "SMTP server rejected connection/credentials");
    Ok(())
}
