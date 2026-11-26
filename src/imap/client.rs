use std::sync::Arc;

use anyhow::{Context, Result};
use async_imap::Session;
use base64::Engine;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

pub type ImapTlsStream = TlsStream<TcpStream>;
pub type ImapSession = Session<ImapTlsStream>;

#[derive(Debug, Clone)]
pub enum ImapAuth {
    Password { user: String, password: String },
    XOAuth2 { user: String, access_token: String },
}

struct XOAuth2Auth {
    payload: String,
}

impl async_imap::Authenticator for XOAuth2Auth {
    type Response = String;
    fn process(&mut self, _challenge: &[u8]) -> Self::Response {
        self.payload.clone()
    }
}

fn build_tls_connector() -> TlsConnector {
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Connects and authenticates an IMAP TLS session.
pub async fn connect_imap_tls(host: &str, port: u16, auth: &ImapAuth) -> Result<ImapSession> {
    tracing::info!(host, port, "IMAP: connecting TCP");
    let tcp = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("failed to connect TCP to {host}:{port}"))?;

    let connector = build_tls_connector();
    let server_name = ServerName::try_from(host.to_string())
        .with_context(|| format!("invalid TLS server name: {host}"))?;
    tracing::info!(host, "IMAP: starting TLS handshake");
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context("IMAP TLS handshake failed")?;

    let mut client = async_imap::Client::new(tls);
    tracing::info!("IMAP: reading server greeting");
    let _greeting = client.read_response().await;
    tracing::info!("IMAP: authenticating");

    let session = match auth {
        ImapAuth::Password { user, password } => client
            .login(user, password)
            .await
            .map_err(|(e, _)| anyhow::anyhow!("IMAP login failed: {e}"))?,
        ImapAuth::XOAuth2 { user, access_token } => {
            let raw = format!("user={user}\x01auth=Bearer {access_token}\x01\x01");
            client
                .authenticate("XOAUTH2", XOAuth2Auth { payload: raw })
                .await
                .map_err(|(e, _)| anyhow::anyhow!("IMAP XOAUTH2 failed: {e}"))?
        }
    };
    tracing::info!("IMAP: authenticated successfully");

    Ok(session)
}

/// Verifies IMAP connectivity & credentials, then logs out cleanly.
pub async fn test_imap_connection(host: &str, port: u16, auth: &ImapAuth) -> Result<()> {
    let mut session = connect_imap_tls(host, port, auth).await?;
    let _ = session.logout().await;
    Ok(())
}
