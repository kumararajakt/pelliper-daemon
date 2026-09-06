use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_util::compat::TokioAsyncReadCompatExt;
use futures::TryStreamExt;
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
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        let trimmed = line.trim_end();
        tracing::debug!("IMAP auth line: {}", trimmed);

        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('+') {
            if trimmed.len() > 1 {
                let payload = trimmed.trim_start_matches("+").trim();
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(payload)
                    .unwrap_or_default();
                tracing::debug!("IMAP error challenge: {}", String::from_utf8_lossy(&decoded));
            }
            write_half.write_all(b"\r\n").await?;
            write_half.flush().await?;
            continue;
        }

        if trimmed.starts_with('*') {
            tracing::debug!("IMAP untagged: {}", trimmed);
            continue;
        }

        if trimmed.starts_with("A001") {
            if trimmed.contains("OK") {
                tracing::info!("XOAUTH2 auth succeeded: {}", trimmed);
                let _ = write_half.write_all(b"A002 LOGOUT\r\n").await;
                let _ = write_half.flush().await;
                let mut buf = [0u8; 1024];
                let _ = tokio::time::timeout(Duration::from_secs(3), reader.read(&mut buf)).await;
                return Ok(());
            } else {
                return Err(format!("Authentication failed: {}", trimmed).into());
            }
        }

        tracing::warn!("Unexpected IMAP line: {}", trimmed);
    }
}

/// Perform XOAUTH2 handshake and return a live session (not logged out).
///
/// Returns a `RawImapSession` that can send IMAP commands over the
/// authenticated TLS connection.
async fn do_oauth_session(
    stream: TlsStream<TcpStream>,
    email: &str,
    access_token: &str,
) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Read server greeting
    line.clear();
    reader.read_line(&mut line).await?;
    tracing::debug!("IMAP greeting: {}", line.trim_end());

    // Send AUTHENTICATE XOAUTH2
    write_half.write_all(b"A001 AUTHENTICATE XOAUTH2\r\n").await?;
    write_half.flush().await?;

    // Read continuation challenge
    line.clear();
    reader.read_line(&mut line).await?;
    if !line.starts_with('+') {
        return Err(format!("Expected challenge, got: {}", line.trim_end()).into());
    }

    // Send XOAUTH2 token
    let auth_string = format!("user={}\x01auth=Bearer {}\x01\x01", email, access_token);
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(auth_string.as_bytes());
    write_half.write_all(encoded.as_bytes()).await?;
    write_half.write_all(b"\r\n").await?;
    write_half.flush().await?;

    // Read until tagged OK
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        let trimmed = line.trim_end();

        if trimmed.is_empty() { continue; }

        if trimmed.starts_with('+') {
            write_half.write_all(b"\r\n").await?;
            write_half.flush().await?;
            continue;
        }
        if trimmed.starts_with('*') { continue; }

        if trimmed.starts_with("A001") {
            if trimmed.contains("OK") {
                tracing::info!("XOAUTH2 session auth succeeded for {}", email);
                return Ok(RawImapSession {
                    reader,
                    writer: write_half,
                    tag: 1,
                });
            } else {
                return Err(format!("OAuth auth failed: {}", trimmed).into());
            }
        }
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

// ---------------------------------------------------------------------------
// Session management for sync
// ---------------------------------------------------------------------------

/// Information about a discovered IMAP folder.
pub struct ImapFolder {
    pub path: String,
    pub delimiter: Option<String>,
    pub noselect: bool,
}

/// Summary of an IMAP message (header-level fields only).
pub struct ImapMessageSummary {
    pub uid: i64,
    pub subject: String,
    pub sender: String,
    pub date: i64,
    pub is_read: bool,
    pub is_starred: bool,
    pub has_attachments: bool,
    pub preview: String,
}

/// A raw IMAP session over a TLS stream, used for OAuth accounts where
/// async-imap's `authenticate` hangs.
pub struct RawImapSession {
    reader: BufReader<tokio::io::ReadHalf<TlsStream<TcpStream>>>,
    writer: tokio::io::WriteHalf<TlsStream<TcpStream>>,
    tag: u32,
}

impl RawImapSession {
    fn next_tag(&mut self) -> String {
        self.tag += 1;
        format!("T{}", self.tag)
    }

    async fn send_command(&mut self, cmd: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::debug!("IMAP send: {}", cmd);
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read lines until we get a tagged response matching `tag`.
    /// Returns (tagged_line, untagged_lines).
    /// Handles IMAP literals: {<size>}\r\n<data>\r\n
    async fn read_response(&mut self, tag: &str) -> Result<(String, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
        let mut untagged = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            self.reader.read_line(&mut line).await?;
            let trimmed = line.trim_end().to_string();
            if trimmed.is_empty() { continue; }

            // Continuation — server needs us to send + for literal data
            if trimmed.starts_with('+') {
                self.writer.write_all(b"+ \r\n").await?;
                self.writer.flush().await?;
                continue;
            }

            // Tagged response — check if it starts with our tag
            if trimmed.starts_with(tag) {
                return Ok((trimmed, untagged));
            }

            // IMAP literal: line ending with {<size>} — read the literal data
            // and append it to the line so parsers can extract header content.
            if let Some(brace_pos) = trimmed.rfind('{') {
                if trimmed.ends_with('}') {
                    if let Ok(size) = trimmed[brace_pos+1..trimmed.len()-1].parse::<usize>() {
                        tracing::debug!("IMAP literal: {} bytes", size);
                        // Read exactly `size` bytes of literal data
                        let mut literal_buf = vec![0u8; size];
                        self.reader.read_exact(&mut literal_buf).await?;
                        // Read the trailing CRLF after the literal
                        let mut crlf = [0u8; 2];
                        let _ = self.reader.read_exact(&mut crlf).await;
                        // Append the literal data (as UTF-8) to the line
                        let literal_text = String::from_utf8_lossy(&literal_buf);
                        let mut full_line = trimmed;
                        full_line.push_str("\r\n");
                        full_line.push_str(&literal_text);
                        untagged.push(full_line);
                        continue;
                    }
                }
            }

            // Regular untagged line
            untagged.push(trimmed);
        }
    }

    /// LIST all folders.
    pub async fn list_folders(&mut self) -> Result<Vec<ImapFolder>, Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!("{} LIST \"\" \"*\"", tag)).await?;
        let (tagged, untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("LIST failed: {}", tagged).into());
        }

        let mut folders = Vec::new();
        for line in &untagged {
            // Format: * LIST (\flags) "delimiter" "name"
            if let Some((name, noselect)) = parse_list_response(line) {
                folders.push(ImapFolder { path: name, delimiter: None, noselect });
            }
        }

        tracing::info!("Discovered {} folders (raw)", folders.len());
        Ok(folders)
    }

    /// EXAMINE a folder and fetch all message summaries.
    pub async fn fetch_message_summaries(
        &mut self,
        folder: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        // EXAMINE (read-only SELECT)
        let tag = self.next_tag();
        self.send_command(&format!("{} EXAMINE {}", tag, quote_imap_path(folder))).await?;
        let (tagged, examine_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("EXAMINE failed: {}", tagged).into());
        }

        // Parse EXISTS from untagged lines: * 42 EXISTS
        let exists = examine_untagged.iter()
            .filter_map(|l| l.strip_prefix("* "))
            .filter_map(|s| s.strip_suffix(" EXISTS"))
            .filter_map(|s| s.parse::<usize>().ok())
            .next()
            .unwrap_or(0);

        if exists == 0 {
            return Ok(Vec::new());
        }

        // Fetch UID + headers + flags for all messages
        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH 1:* (UID BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE)] FLAGS)",
            tag
        )).await?;
        let (tagged, fetch_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("FETCH failed: {}", tagged).into());
        }

        let mut summaries = Vec::new();
        for line in &fetch_untagged {
            tracing::debug!("FETCH untagged line (len={}): {}", line.len(), &line[..line.len().min(200)]);
            if let Some(msg) = parse_fetch_response(line) {
                tracing::debug!("Parsed message uid={} subject='{}' sender='{}'", msg.uid, msg.subject, msg.sender);
                summaries.push(msg);
            }
        }

        tracing::info!("Fetched {} message summaries from {} (raw)", summaries.len(), folder);
        Ok(summaries)
    }

    /// Send LOGOUT.
    pub async fn logout(&mut self) {
        let _ = self.send_command("X LOGOUT").await;
    }

    /// SELECT a folder (read-write).
    pub async fn select_folder(&mut self, folder: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!("{} SELECT {}", tag, quote_imap_path(folder))).await?;
        let (tagged, _) = self.read_response(&tag).await?;
        if !tagged.contains("OK") {
            return Err(format!("SELECT failed: {}", tagged).into());
        }
        Ok(())
    }

    /// Start IDLE on the currently selected folder.
    /// Returns once the server sends the `+ go ahead` continuation.
    pub async fn idle_start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!("{} IDLE", tag)).await?;
        // Read until we get the continuation "+ go ahead"
        let mut line = String::new();
        loop {
            line.clear();
            self.reader.read_line(&mut line).await?;
            let trimmed = line.trim_end();
            if trimmed.is_empty() { continue; }
            if trimmed.starts_with('+') {
                tracing::debug!("IDLE started: {}", trimmed);
                return Ok(());
            }
            // Any other response means IDLE was rejected
            return Err(format!("IDLE rejected: {}", trimmed).into());
        }
    }

    /// Read one line during IDLE. Returns the untagged notification line.
    /// Returns None if the server ended IDLE (tagged response).
    pub async fn idle_read_line(&mut self) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut line = String::new();
        line.clear();
        self.reader.read_line(&mut line).await?;
        let trimmed = line.trim_end().to_string();
        if trimmed.is_empty() {
            return Ok(Some(trimmed));
        }
        // Tagged response means IDLE ended (server timeout or BYE)
        if !trimmed.starts_with('*') && !trimmed.starts_with('+') {
            tracing::debug!("IDLE ended: {}", trimmed);
            return Ok(None);
        }
        Ok(Some(trimmed))
    }

    /// Send DONE to stop IDLE, then read the tagged response.
    pub async fn idle_done(&mut self, tag: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.writer.write_all(b"DONE\r\n").await?;
        self.writer.flush().await?;
        // Read until we get the tagged response
        let mut line = String::new();
        loop {
            line.clear();
            self.reader.read_line(&mut line).await?;
            let trimmed = line.trim_end();
            if trimmed.is_empty() { continue; }
            if trimmed.starts_with(tag) {
                tracing::debug!("IDLE DONE: {}", trimmed);
                return Ok(());
            }
        }
    }

    /// Get the current tag value (for IDLE tag tracking).
    pub fn current_tag(&self) -> u32 {
        self.tag
    }
}

/// Parse a LIST response line: * LIST (\flags) "delimiter" "name"
fn parse_list_response(line: &str) -> Option<(String, bool)> {
    // Find the name — it's the last quoted or unquoted token
    let rest = line.strip_prefix("* LIST ")?;
    // Parse flags: find closing paren
    let mut noselect = false;
    let after_flags = if rest.starts_with('(') {
        let end = rest.find(')')? + 1;
        let flags = &rest[1..end-1];
        noselect = flags.contains("\\Noselect") || flags.contains("\\noselect");
        end
    } else {
        0
    };
    let rest = rest[after_flags..].trim_start();
    // Skip delimiter (quoted or NIL)
    let rest = if rest.starts_with('"') {
        let end = rest[1..].find('"')? + 2;
        rest[end..].trim_start()
    } else if rest.starts_with("NIL") {
        rest[3..].trim_start()
    } else {
        rest
    };
    // The rest is the folder name (may be quoted)
    let name = if rest.starts_with('"') {
        let end = rest[1..].find('"')? + 2;
        rest[1..end-1].to_string()
    } else {
        rest.to_string()
    };
    Some((name, noselect))
}

/// Parse a FETCH response line for UID, flags, and body.
/// After read_response appends literal data, the line looks like:
/// * <seq> FETCH (UID <uid> FLAGS (\Seen) BODY[HEADER.FIELDS ...] {<size>}\r\n<headers>
fn parse_fetch_response(line: &str) -> Option<ImapMessageSummary> {
    let rest = line.strip_prefix("* ")?;

    // Extract UID — look for "UID" followed by a number
    let uid = {
        let mut found_uid = false;
        let mut uid_val = 0i64;
        for token in rest.split_whitespace() {
            if found_uid {
                uid_val = token.parse().unwrap_or(0);
                break;
            }
            if token == "UID" || token == "(UID" {
                found_uid = true;
            }
        }
        if uid_val == 0 {
            tracing::warn!("UID not found in FETCH line (first 300 chars): {}", &rest[..rest.len().min(300)]);
        }
        uid_val
    };

    // Extract flags
    let is_read = rest.contains("\\Seen");
    let is_starred = rest.contains("\\Flagged");

    // Find the literal size marker: BODY[...] {<size>}
    // Must find the brace that closes the size marker, not any brace inside header content
    let body_start = rest.find("BODY[HEADER.FIELDS")?;
    // Find "{" after the BODY[...] section — search from after the closing bracket
    let bracket_end = rest[body_start..].find(']')?;
    let search_from = body_start + bracket_end + 1;
    let brace_start = rest[search_from..].find('{')?;
    let brace_pos = search_from + brace_start;
    let brace_end = rest[brace_pos..].find('}')?;
    let after_brace = brace_pos + brace_end + 1;

    // After {size}\r\n comes the literal header text
    let header_text = if after_brace < rest.len() {
        rest[after_brace..].trim().to_string()
    } else {
        String::new()
    };

    // Strip trailing closing paren that might be appended
    let header_text = header_text.trim_end_matches(')').trim().to_string();

    let subject = parse_header(&header_text, "Subject");
    let sender = parse_header(&header_text, "From");
    let date_str = parse_header(&header_text, "Date");
    let date = parse_date(&date_str);

    if subject.is_empty() && sender.is_empty() && uid != 0 {
        tracing::debug!("Empty headers for uid={}, header_text (first 200): {:?}", uid, &header_text[..header_text.len().min(200)]);
    }

    Some(ImapMessageSummary {
        uid,
        subject,
        sender,
        date,
        is_read,
        is_starred,
        has_attachments: false,
        preview: String::new(),
    })
}

fn quote_imap_path(path: &str) -> String {
    if path.contains(' ') || path.contains('/') {
        format!("\"{}\"", path)
    } else {
        path.to_string()
    }
}

/// Connect and log in with password, returning an async-imap Session.
pub async fn connect_session_password(
    host: &str,
    port: u16,
    email: &str,
    password: &str,
) -> Result<async_imap::Session<CompatTls>, Box<dyn std::error::Error + Send + Sync>> {
    let client = connect_tls(host, port).await?;
    let session = timeout(IMAP_TIMEOUT, client.login(email, password))
        .await?
        .map_err(|(err, _client)| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Login failed: {}", err).into()
        })?;
    Ok(session)
}

/// Connect via XOAUTH2 and return a live RawImapSession.
pub async fn connect_session_oauth(
    host: &str,
    port: u16,
    email: &str,
    access_token: &str,
) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let stream = connect_tls_stream(host, port).await?;
    do_oauth_session(stream, email, access_token).await
}

/// Connect with password and return a live RawImapSession.
pub async fn connect_session_password_raw(
    host: &str,
    port: u16,
    email: &str,
    password: &str,
) -> Result<RawImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let stream = connect_tls_stream(host, port).await?;
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Read server greeting
    line.clear();
    reader.read_line(&mut line).await?;

    // Send LOGIN
    let tag = "A001";
    let cmd = format!("{} LOGIN {} {}", tag, quote_imap_path(email), quote_imap_path(password));
    write_half.write_all(cmd.as_bytes()).await?;
    write_half.write_all(b"\r\n").await?;
    write_half.flush().await?;

    // Read until tagged OK
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        let trimmed = line.trim_end();
        if trimmed.is_empty() { continue; }
        if trimmed.starts_with('+') {
            write_half.write_all(b"\r\n").await?;
            write_half.flush().await?;
            continue;
        }
        if trimmed.starts_with('*') { continue; }
        if trimmed.starts_with(tag) {
            if trimmed.contains("OK") {
                return Ok(RawImapSession { reader, writer: write_half, tag: 1 });
            } else {
                return Err(format!("LOGIN failed: {}", trimmed).into());
            }
        }
    }
}

/// List all folders via an async-imap Session.
pub async fn list_folders(
    session: &mut async_imap::Session<CompatTls>,
) -> Result<Vec<ImapFolder>, Box<dyn std::error::Error + Send + Sync>> {
    let mailboxes_stream = timeout(IMAP_TIMEOUT, session.list(None, Some("*")))
        .await??;
    let mailboxes: Vec<_> = mailboxes_stream.try_collect().await?;

    let mut folders = Vec::new();
    for mailbox in &mailboxes {
        let name = mailbox.name().to_string();
        let delimiter = mailbox.delimiter().map(|d| d.to_string());
        let noselect = mailbox.attributes().iter().any(|a| matches!(a, async_imap::imap_proto::types::NameAttribute::NoSelect));
        folders.push(ImapFolder { path: name, delimiter, noselect });
    }

    tracing::info!("Discovered {} folders", folders.len());
    Ok(folders)
}

/// Fetch all message summaries from a folder via async-imap.
pub async fn fetch_message_summaries(
    session: &mut async_imap::Session<CompatTls>,
    folder: &str,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    // SELECT the folder (read-only)
    let mailbox = timeout(IMAP_TIMEOUT, session.examine(folder)).await??;
    let exists = mailbox.exists;

    if exists == 0 {
        return Ok(Vec::new());
    }

    // Fetch all messages
    let messages_stream = timeout(
        IMAP_TIMEOUT,
        session.uid_fetch("1:*", "(UID BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE)] FLAGS)"),
    )
    .await??;
    let messages: Vec<_> = messages_stream.try_collect().await?;

    let mut summaries = Vec::new();
    for message in &messages {
        let uid = message.uid.unwrap_or(0) as i64;
        let flags: Vec<_> = message.flags().collect();
        let is_read = flags.iter().any(|f| matches!(f, async_imap::types::Flag::Seen));
        let is_starred = flags.iter().any(|f| matches!(f, async_imap::types::Flag::Flagged));

        // Parse headers from the body
        let header_bytes = message.body().unwrap_or(b"");
        let header_str = String::from_utf8_lossy(header_bytes);

        let subject = parse_header(&header_str, "Subject");
        let sender = parse_header(&header_str, "From");
        let date_str = parse_header(&header_str, "Date");
        let date = parse_date(&date_str);

        summaries.push(ImapMessageSummary {
            uid,
            subject,
            sender,
            date,
            is_read,
            is_starred,
            has_attachments: false, // will be detected when body is fetched
            preview: String::new(),
        });
    }

    tracing::info!("Fetched {} message summaries from {}", summaries.len(), folder);
    Ok(summaries)
}

/// LOGOUT from a session.
pub async fn logout_session(session: &mut async_imap::Session<CompatTls>) {
    let _ = timeout(IMAP_TIMEOUT, session.logout()).await;
}

/// Parse a single header value from raw RFC822 headers.
/// Handles folded headers (continuation lines starting with whitespace).
fn parse_header(headers: &str, name: &str) -> String {
    let mut result = String::new();
    let mut found = false;
    let prefix = format!("{}:", name);
    for line in headers.lines() {
        if !found {
            if let Some(value) = line.strip_prefix(&prefix) {
                result = decode_rfc2047(value.trim());
                found = true;
            }
        } else if line.starts_with(' ') || line.starts_with('\t') {
            // RFC 822 continuation line — append with a space
            result.push(' ');
            result.push_str(&decode_rfc2047(line.trim()));
        } else {
            break;
        }
    }
    result
}

/// Decode RFC 2047 encoded words: =?charset?encoding?text?=
fn decode_rfc2047(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(start) = remaining.find("=?") {
        // Copy text before the encoded word
        output.push_str(&remaining[..start]);
        remaining = &remaining[start..];

        // Find the end of the encoded word
        if let Some(end) = remaining.find("?=") {
            let encoded = &remaining[2..end]; // skip =? and before ?=
            remaining = &remaining[end + 2..]; // skip ?=

            // Parse: charset?encoding?text
            let parts: Vec<&str> = encoded.splitn(3, '?').collect();
            if parts.len() == 3 {
                let charset = parts[0].to_uppercase();
                let encoding = parts[1].to_uppercase();
                let text = parts[2];

                let decoded_bytes = match encoding.as_str() {
                    "B" => {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.decode(text).unwrap_or_default()
                    }
                    "Q" => decode_quoted_printable(text),
                    _ => Vec::new(),
                };

                let decoded = if charset == "UTF-8" || charset == "UTF8" {
                    String::from_utf8_lossy(&decoded_bytes).to_string()
                } else {
                    // Best effort: try UTF-8, fall back to lossy conversion
                    String::from_utf8(decoded_bytes).unwrap_or_default()
                };
                output.push_str(&decoded);
            } else {
                output.push_str("=?");
                output.push_str(encoded);
                output.push_str("?=");
            }
        } else {
            output.push_str(remaining);
            break;
        }
    }
    // If no encoded words found, return the original input
    if output.is_empty() && !input.is_empty() {
        return input.to_string();
    }
    output
}

/// Decode RFC 2047 Q-encoding (quoted-printable variant)
fn decode_quoted_printable(input: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' && i + 2 < bytes.len() {
            // =XX hex encoding
            let hex = &input[i + 1..i + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                output.push(byte);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'_' {
            output.push(b' ');
        } else {
            output.push(bytes[i]);
        }
        i += 1;
    }
    output
}

/// Parse an RFC822 date string into a Unix timestamp (best-effort).
fn parse_date(date_str: &str) -> i64 {
    if date_str.is_empty() {
        return 0;
    }
    // Try chrono parsing
    use chrono::DateTime;
    if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
        return dt.timestamp();
    }
    // Fallback: try common formats
    if let Ok(dt) = DateTime::parse_from_str(date_str, "%d %b %Y %H:%M:%S %z") {
        return dt.timestamp();
    }
    0
}
