use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_util::compat::TokioAsyncReadCompatExt;
use std::sync::Arc;

pub type CompatTls = tokio_util::compat::Compat<TlsStream<TcpStream>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IMAP_TIMEOUT: Duration = Duration::from_secs(15);

pub async fn test_connection_oauth(
    host: &str,
    port: u16,
    email: &str,
    access_token: &str,
) -> bool {
    let result = timeout(CONNECT_TIMEOUT, connect_tls_stream(host, port)).await;
    let stream = match result {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::error!("IMAP TLS connection failed for {}: {:?}", host, e);
            return false;
        }
        Err(_) => {
            tracing::error!("IMAP connection timed out for {}:{}", host, port);
            return false;
        }
    };

    let result = timeout(
        IMAP_TIMEOUT,
        do_oauth_handshake(stream, email, access_token),
    )
    .await;
    match result {
        Ok(Ok(())) => {
            tracing::info!("IMAP OAuth auth success for {}", email);
            true
        }
        Ok(Err(e)) => {
            tracing::error!("IMAP OAuth auth failed for {}: {}", email, e);
            false
        }
        Err(_) => {
            tracing::error!("IMAP OAuth auth timed out for {}", email);
            false
        }
    }
}

pub async fn test_connection_password(
    host: &str,
    port: u16,
    email: &str,
    password: &str,
) -> bool {
    let result = timeout(CONNECT_TIMEOUT, connect_tls(host, port)).await;
    let client = match result {
        Ok(Ok(client)) => client,
        Ok(Err(e)) => {
            tracing::error!("IMAP TLS connection failed for {}: {:?}", host, e);
            return false;
        }
        Err(_) => {
            tracing::error!("IMAP connection timed out for {}:{}", host, port);
            return false;
        }
    };

    let result = timeout(IMAP_TIMEOUT, client.login(email, password)).await;
    match result {
        Ok(Ok(mut session)) => {
            tracing::info!("IMAP password auth success for {}", email);
            let _ = timeout(IMAP_TIMEOUT, session.logout()).await;
            true
        }
        Ok(Err((_, e))) => {
            tracing::error!("IMAP password auth failed for {}: {:?}", email, e);
            false
        }
        Err(_) => {
            tracing::error!("IMAP password auth timed out for {}", email);
            false
        }
    }
}

/// Manually perform the XOAUTH2 IMAP handshake over a raw TLS stream.
///
/// This avoids async-imap's `Client::authenticate` which hangs on Gmail's
/// XOAUTH2 response because it misinterprets the server's continuation
/// challenge.
async fn do_oauth_handshake(
    stream: TlsStream<TcpStream>,
    email: &str,
    access_token: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Read server greeting (e.g. * OK …)
    line.clear();
    reader.read_line(&mut line).await?;
    tracing::debug!("IMAP greeting: {}", line.trim_end());

    // Send AUTHENTICATE XOAUTH2
    write_half
        .write_all(b"A001 AUTHENTICATE XOAUTH2\r\n")
        .await?;
    write_half.flush().await?;

    // Read the continuation challenge (should be "+ " or "+")
    line.clear();
    reader.read_line(&mut line).await?;
    tracing::debug!("IMAP challenge: {}", line.trim_end());

    if !line.starts_with('+') {
        return Err(format!("Expected continuation challenge, got: {}", line.trim_end()).into());
    }

    // Build and send the XOAUTH2 initial client response
    let auth_string = format!("user={}\x01auth=Bearer {}\x01\x01", email, access_token);
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(auth_string.as_bytes());
    tracing::debug!(
        "XOAUTH2 response length: {} (email: {})",
        encoded.len(),
        email
    );

    write_half.write_all(encoded.as_bytes()).await?;
    write_half.write_all(b"\r\n").await?;
    write_half.flush().await?;

    // Read responses until we get the tagged A001 reply.
    // Gmail (and other servers) may send untagged lines first:
    //   * CAPABILITY IMAP4rev1 ...
    //   * SASL_IR ...
    //   A001 OK ...
    // On failure it may also send a continuation " + <base64 error>",
    // which we must acknowledge with an empty line.
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        let trimmed = line.trim_end();
        tracing::debug!("IMAP auth line: {}", trimmed);

        if trimmed.is_empty() {
            // Skip blank lines
            continue;
        }

        // Continuation challenge (error detail from server)
        if trimmed.starts_with('+') {
            if trimmed.len() > 1 {
                // Decode the base64 error for logging
                let payload = trimmed.trim_start_matches("+").trim();
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(payload)
                    .unwrap_or_default();
                tracing::debug!(
                    "IMAP error challenge: {}",
                    String::from_utf8_lossy(&decoded)
                );
            }
            // Send empty response to acknowledge the error
            write_half.write_all(b"\r\n").await?;
            write_half.flush().await?;
            continue;
        }

        // Untagged response (* …) — just log and keep reading
        if trimmed.starts_with('*') {
            tracing::debug!("IMAP untagged: {}", trimmed);
            continue;
        }

        // Tagged response — this is the final result
        if trimmed.starts_with("A001") {
            if trimmed.contains("OK") {
                tracing::info!("XOAUTH2 auth succeeded: {}", trimmed);
                // Send LOGOUT to cleanly close the session
                let _ = write_half.write_all(b"A002 LOGOUT\r\n").await;
                let _ = write_half.flush().await;
                // Consume LOGOUT response (may be BYE + OK)
                let mut buf = [0u8; 1024];
                let _ = tokio::time::timeout(Duration::from_secs(3), reader.read(&mut buf)).await;
                return Ok(());
            } else {
                return Err(format!("Authentication failed: {}", trimmed).into());
            }
        }

        // Unknown line format — skip
        tracing::warn!("Unexpected IMAP line: {}", trimmed);
    }
}

/// Connect to an IMAP server with TLS, returning the raw TLS stream.
async fn connect_tls_stream(
    host: &str,
    port: u16,
) -> Result<TlsStream<TcpStream>, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", host, port);
    let tcp = TcpStream::connect(&addr).await?;

    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(tls_config));
    let domain = rustls::pki_types::ServerName::try_from(host.to_string())?;
    let tls_stream = connector.connect(domain, tcp).await?;

    Ok(tls_stream)
}

/// Connect to an IMAP server with TLS, returning an async-imap Client.
async fn connect_tls(
    host: &str,
    port: u16,
) -> Result<async_imap::Client<CompatTls>, Box<dyn std::error::Error + Send + Sync>> {
    let tls_stream = connect_tls_stream(host, port).await?;
    let compat = tls_stream.compat();
    let client = async_imap::Client::new(compat);
    Ok(client)
}
