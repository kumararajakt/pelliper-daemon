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
    pub kind: FolderKind,
}

/// Semantic classification of a folder based on SPECIAL-USE attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum FolderKind {
    Inbox,
    Sent,
    Drafts,
    Trash,
    Junk,
    Archive,
    Starred,
    Custom,
}

impl FolderKind {
    /// Parse from IMAP flag string (e.g., "\\Sent" -> Sent).
    pub fn from_flag(flag: &str) -> Option<Self> {
        match flag.to_lowercase().as_str() {
            "\\inbox" | "inbox" => Some(FolderKind::Inbox),
            "\\sent" | "sent" => Some(FolderKind::Sent),
            "\\drafts" | "drafts" => Some(FolderKind::Drafts),
            "\\trash" | "trash" => Some(FolderKind::Trash),
            "\\junk" | "junk" | "\\spam" | "spam" => Some(FolderKind::Junk),
            "\\archive" | "archive" | "\\all" | "all" => Some(FolderKind::Archive),
            "\\flagged" | "flagged" | "\\starred" | "starred" => Some(FolderKind::Starred),
            _ => None,
        }
    }

    /// Classify by path heuristics (fallback when no SPECIAL-USE attribute).
    pub fn from_path(path: &str) -> Self {
        let leaf = path.rsplit('/').next().unwrap_or(path);
        let lower = leaf.to_lowercase();
        match lower.as_str() {
            "inbox" => FolderKind::Inbox,
            "sent" | "sent items" | "sent mail" | "sent messages" => FolderKind::Sent,
            "drafts" | "draft" => FolderKind::Drafts,
            "trash" | "deleted items" | "deleted messages" | "bin" | "deleted" => FolderKind::Trash,
            "junk" | "junk e-mail" | "spam" => FolderKind::Junk,
            "archive" | "archives" | "all mail" | "all" => FolderKind::Archive,
            "starred" | "flagged" | "important" => FolderKind::Starred,
            _ => FolderKind::Custom,
        }
    }
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
    pub message_id: String,
    pub references: String,
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
            if let Some((name, noselect, kind)) = parse_list_response(line) {
                folders.push(ImapFolder { path: name, delimiter: None, noselect, kind });
            }
        }

        tracing::info!("Discovered {} folders (raw)", folders.len());
        Ok(folders)
    }

    /// EXAMINE a folder and return the total message count.
    pub async fn examine_folder(
        &mut self,
        folder: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!("{} EXAMINE {}", tag, quote_imap_path(folder))).await?;
        let (tagged, examine_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("EXAMINE failed: {}", tagged).into());
        }

        let exists = examine_untagged.iter()
            .filter_map(|l| l.strip_prefix("* "))
            .filter_map(|s| s.strip_suffix(" EXISTS"))
            .filter_map(|s| s.parse::<usize>().ok())
            .next()
            .unwrap_or(0);

        Ok(exists)
    }

    /// Fetch the most recent `limit` message summaries (newest first).
    pub async fn fetch_recent_summaries(
        &mut self,
        folder: &str,
        limit: usize,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        let total = self.examine_folder(folder).await?;
        if total == 0 {
            return Ok(Vec::new());
        }

        // Fetch from highest UID down, grab `limit` messages
        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH {}:* (UID ENVELOPE BODYSTRUCTURE BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE MESSAGE-ID REFERENCES IN-REPLY-TO)] FLAGS)",
            tag, total
        )).await?;
        let (tagged, fetch_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("FETCH recent failed: {}", tagged).into());
        }

        let mut summaries = Vec::new();
        for line in &fetch_untagged {
            if let Some(msg) = parse_fetch_response(line) {
                summaries.push(msg);
            }
        }

        // Sort newest first and take only `limit`
        summaries.sort_by(|a, b| b.date.cmp(&a.date));
        summaries.truncate(limit);

        tracing::info!("Fetched {} recent messages from {}", summaries.len(), folder);
        Ok(summaries)
    }

    /// Fetch all cached UIDs for a folder (lightweight: UID only).
    pub async fn fetch_all_uids(
        &mut self,
        folder: &str,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        let total = self.examine_folder(folder).await?;
        if total == 0 {
            return Ok(Vec::new());
        }

        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH 1:* (UID)",
            tag
        )).await?;
        let (tagged, fetch_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("FETCH UIDs failed: {}", tagged).into());
        }

        let mut uids = Vec::new();
        for line in &fetch_untagged {
            if let Some(uid) = parse_uid_only(line) {
                uids.push(uid);
            }
        }

        Ok(uids)
    }

    /// Fetch specific message summaries by UID set string.
    pub async fn fetch_summaries_by_uids(
        &mut self,
        uid_set: &str,
    ) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH {} (UID ENVELOPE BODYSTRUCTURE BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE MESSAGE-ID REFERENCES IN-REPLY-TO)] FLAGS)",
            tag, uid_set
        )).await?;
        let (tagged, fetch_untagged) = self.read_response(&tag).await?;

        if !tagged.contains("OK") {
            return Err(format!("FETCH by UIDs failed: {}", tagged).into());
        }

        let mut summaries = Vec::new();
        for line in &fetch_untagged {
            if let Some(msg) = parse_fetch_response(line) {
                summaries.push(msg);
            }
        }

        Ok(summaries)
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
            "{} UID FETCH 1:* (UID ENVELOPE BODYSTRUCTURE BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE MESSAGE-ID REFERENCES IN-REPLY-TO)] FLAGS)",
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

    /// Fetch the full body of a message by UID (PEEK — does not set \Seen).
    /// Returns the raw RFC822 message bytes.
    pub async fn fetch_body(&mut self, uid: i64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH {} (BODY.PEEK[])",
            tag, uid
        )).await?;

        // Read response, collecting literal data
        let mut body_data = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            self.reader.read_line(&mut line).await?;
            let trimmed = line.trim_end().to_string();
            if trimmed.is_empty() { continue; }

            // Continuation — send + for literal
            if trimmed.starts_with('+') {
                self.writer.write_all(b"+ \r\n").await?;
                self.writer.flush().await?;
                continue;
            }

            // Tagged response — done
            if trimmed.starts_with(&tag) {
                if trimmed.contains("OK") && !body_data.is_empty() {
                    return Ok(body_data);
                } else if !trimmed.contains("OK") {
                    return Err(format!("FETCH BODY[] failed: {}", trimmed).into());
                }
                // OK but no data — empty message
                return Ok(Vec::new());
            }

            // Untagged line — check for literal
            if trimmed.starts_with('*') {
                // Look for literal: BODY[] {<size>}
                if let Some(body_pos) = trimmed.find("BODY[]") {
                    let after_body = &trimmed[body_pos + 6..];
                    if let Some(brace_start) = after_body.find('{') {
                        if let Some(brace_end) = after_body[brace_start..].find('}') {
                            let size_str = &after_body[brace_start + 1..brace_start + brace_end];
                            if let Ok(size) = size_str.parse::<usize>() {
                                // Read exactly `size` bytes
                                body_data.resize(size, 0);
                                self.reader.read_exact(&mut body_data).await?;
                                // Read trailing CRLF
                                let mut crlf = [0u8; 2];
                                let _ = self.reader.read_exact(&mut crlf).await;
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Fetch bodies for multiple UIDs in a single FETCH command (PEEK).
    /// Returns Vec of (uid, raw_bytes).
    pub async fn fetch_bodies_batch(
        &mut self,
        uids: &[i64],
    ) -> Result<Vec<(i64, Vec<u8>)>, Box<dyn std::error::Error + Send + Sync>> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        let uid_set: Vec<String> = uids.iter().map(|u| u.to_string()).collect();
        let uid_set_str = uid_set.join(",");

        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID FETCH {} (UID BODY.PEEK[])",
            tag, uid_set_str
        )).await?;

        let mut results = Vec::new();
        let mut current_uid: Option<i64> = None;
        let mut body_data = Vec::new();
        let mut line = String::new();

        loop {
            line.clear();
            self.reader.read_line(&mut line).await?;
            let trimmed = line.trim_end().to_string();
            if trimmed.is_empty() { continue; }

            if trimmed.starts_with('+') {
                self.writer.write_all(b"+ \r\n").await?;
                self.writer.flush().await?;
                continue;
            }

            if trimmed.starts_with(&tag) {
                // Save last body if pending
                if let Some(uid) = current_uid.take() {
                    if !body_data.is_empty() {
                        results.push((uid, std::mem::take(&mut body_data)));
                    }
                }
                break;
            }

            if trimmed.starts_with('*') {
                // Check for new FETCH response — extract UID
                if let Some(uid) = parse_uid_only(&trimmed) {
                    // Save previous body if any
                    if let Some(prev_uid) = current_uid.take() {
                        if !body_data.is_empty() {
                            results.push((prev_uid, std::mem::take(&mut body_data)));
                        }
                    }
                    current_uid = Some(uid);
                }

                // Check for literal: BODY[] {<size>}
                if let Some(body_pos) = trimmed.find("BODY[]") {
                    let after_body = &trimmed[body_pos + 6..];
                    if let Some(brace_start) = after_body.find('{') {
                        if let Some(brace_end) = after_body[brace_start..].find('}') {
                            let size_str = &after_body[brace_start + 1..brace_start + brace_end];
                            if let Ok(size) = size_str.parse::<usize>() {
                                body_data.resize(size, 0);
                                self.reader.read_exact(&mut body_data).await?;
                                let mut crlf = [0u8; 2];
                                let _ = self.reader.read_exact(&mut crlf).await;
                                continue;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Mark a message as seen (read).
    pub async fn mark_seen(&mut self, uid: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tag = self.next_tag();
        self.send_command(&format!(
            "{} UID STORE {} +FLAGS (\\Seen)",
            tag, uid
        )).await?;
        let (tagged, _) = self.read_response(&tag).await?;
        if !tagged.contains("OK") {
            return Err(format!("STORE failed: {}", tagged).into());
        }
        Ok(())
    }
}

/// Parse a LIST response line: * LIST (\flags) "delimiter" "name"
/// Returns (name, noselect, kind).
fn parse_list_response(line: &str) -> Option<(String, bool, FolderKind)> {
    let rest = line.strip_prefix("* LIST ")?;
    let mut noselect = false;
    let mut kind = FolderKind::Custom;
    let mut special_use_found = false;

    // Parse flags: find closing paren
    let after_flags = if rest.starts_with('(') {
        let end = rest.find(')')? + 1;
        let flags = &rest[1..end-1];
        noselect = flags.contains("\\Noselect") || flags.contains("\\noselect");

        // Parse SPECIAL-USE attributes
        for flag in flags.split_whitespace() {
            if let Some(k) = FolderKind::from_flag(flag) {
                kind = k;
                special_use_found = true;
                break;
            }
        }
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

    // If no SPECIAL-USE attribute was found, classify by path
    if !special_use_found {
        kind = FolderKind::from_path(&name);
    }

    Some((name, noselect, kind))
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

    // Detect attachments from BODYSTRUCTURE (if present in the response)
    let has_attachments = detect_attachments_raw(rest);

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
    let message_id = parse_header(&header_text, "Message-ID");
    let references = parse_header(&header_text, "References");
    let in_reply_to = parse_header(&header_text, "In-Reply-To");

    // Use In-Reply-To as fallback for References if empty
    let references = if references.is_empty() { in_reply_to } else { references };

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
        has_attachments,
        preview: String::new(),
        message_id,
        references,
    })
}

/// Heuristically detect attachments from a raw BODYSTRUCTURE section in a
/// FETCH response line. Looks for non-inline, non-text content types like
/// application/*, image/*, video/*, audio/*, or attachments with filenames.
fn detect_attachments_raw(line: &str) -> bool {
    // BODYSTRUCTURE is a parenthesized list. Find it and scan a window.
    let Some(start) = line.find("BODYSTRUCTURE (") else {
        return false;
    };
    let window: String = line[start + "BODYSTRUCTURE (".len()..]
        .chars()
        .take(4096)
        .collect();

    // Skip the outermost multipart wrapper: check for attachment indicators.
    // Split on " (" to isolate parts and look for content types.
    let lower = window.to_lowercase();

    // Direct filename/name params indicate an attachment (e.g. "image/png" ... "name" "foo.png")
    if window.contains("\"name\"") || window.contains("\"filename\"") {
        return true;
    }

    // attachment disposition (as opposed to inline)
    if lower.contains("attachment") && !window.contains("\"inline\"") {
        return true;
    }

    // Non-text single part content types like application/*, image/*, video/*, audio/*
    for token in ["application/", "image/", "video/", "audio/", "font/", "model/", "x-" ] {
        if lower.contains(token) {
            return true;
        }
    }

    false
}

fn quote_imap_path(path: &str) -> String {
    if path.contains(' ') || path.contains('/') {
        format!("\"{}\"", path)
    } else {
        path.to_string()
    }
}

/// Parse a FETCH response for UID only (no header parsing).
fn parse_uid_only(line: &str) -> Option<i64> {
    let rest = line.strip_prefix("* ")?;
    let mut found_uid = false;
    for token in rest.split_whitespace() {
        if found_uid {
            return token.parse().ok();
        }
        if token == "UID" || token == "(UID" {
            found_uid = true;
        }
    }
    None
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

        // Check SPECIAL-USE attributes first, fall back to path heuristics
        let mut kind = FolderKind::Custom;
        let mut special_use_found = false;
        for attr in mailbox.attributes() {
            match attr {
                async_imap::imap_proto::types::NameAttribute::NoSelect => {},
                async_imap::imap_proto::types::NameAttribute::NoInferiors => {},
                async_imap::imap_proto::types::NameAttribute::Extension(ext) => {
                    if let Some(k) = FolderKind::from_flag(ext) {
                        kind = k;
                        special_use_found = true;
                        break;
                    }
                }
                _ => {}
            }
        }

        if !special_use_found {
            kind = FolderKind::from_path(&name);
        }

        folders.push(ImapFolder { path: name, delimiter, noselect, kind });
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

    // Fetch all messages using structured ENVELOPE/BODYSTRUCTURE
    let messages_stream = timeout(
        IMAP_TIMEOUT,
        session.uid_fetch(
            "1:*",
            "(UID ENVELOPE BODYSTRUCTURE FLAGS BODY.PEEK[HEADER.FIELDS (REFERENCES IN-REPLY-TO)])",
        ),
    )
    .await??;
    let messages: Vec<_> = messages_stream.try_collect().await?;

    let mut summaries = Vec::new();
    for message in &messages {
        let body_bytes = message.body().unwrap_or(b"");
        let (references, _in_reply_to) = extract_reference_headers(body_bytes);
        let has_attachments = message
            .bodystructure()
            .map(has_attachments_from_bodystructure)
            .unwrap_or(false);
        if let Some(summary) = fetch_to_summary(message, has_attachments, &references) {
            summaries.push(summary);
        }
    }

    tracing::info!("Fetched {} message summaries from {}", summaries.len(), folder);
    Ok(summaries)
}

/// Fetch the most recent `limit` message summaries from a folder (async-imap).
pub async fn fetch_recent_summaries(
    session: &mut async_imap::Session<CompatTls>,
    folder: &str,
    limit: usize,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = timeout(IMAP_TIMEOUT, session.examine(folder)).await??;
    let exists = mailbox.exists as usize;

    if exists == 0 {
        return Ok(Vec::new());
    }

    // Fetch from highest UID down. Uses structured ENVELOPE/BODYSTRUCTURE for
    // fast parsing plus the References header (ENVELOPE omits References).
    let from_uid = exists;
    let range = format!("{}:*", from_uid);
    let messages_stream = timeout(
        IMAP_TIMEOUT,
        session.uid_fetch(
            &range,
            "(UID ENVELOPE BODYSTRUCTURE FLAGS BODY.PEEK[HEADER.FIELDS (REFERENCES IN-REPLY-TO)])",
        ),
    )
    .await??;
    let messages: Vec<_> = messages_stream.try_collect().await?;

    let mut summaries = Vec::new();
    for message in &messages {
        let body_bytes = message.body().unwrap_or(b"");
        let (references, _in_reply_to) = extract_reference_headers(body_bytes);
        let has_attachments = message
            .bodystructure()
            .map(has_attachments_from_bodystructure)
            .unwrap_or(false);
        if let Some(summary) = fetch_to_summary(message, has_attachments, &references) {
            summaries.push(summary);
        }
    }

    // Sort newest first and take only `limit`
    summaries.sort_by(|a, b| b.date.cmp(&a.date));
    summaries.truncate(limit);

    tracing::info!("Fetched {} recent messages from {}", summaries.len(), folder);
    Ok(summaries)
}

/// Fetch all UIDs from a folder (async-imap).
pub async fn fetch_all_uids(
    session: &mut async_imap::Session<CompatTls>,
    folder: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = timeout(IMAP_TIMEOUT, session.examine(folder)).await??;
    let exists = mailbox.exists as usize;

    if exists == 0 {
        return Ok(Vec::new());
    }

    let messages_stream = timeout(
        IMAP_TIMEOUT,
        session.uid_fetch("1:*", "(UID)"),
    )
    .await??;
    let messages: Vec<_> = messages_stream.try_collect().await?;

    let uids: Vec<i64> = messages.iter()
        .filter_map(|m| m.uid.map(|u| u as i64))
        .collect();

    Ok(uids)
}

/// Convert an async-imap `Fetch` into an `ImapMessageSummary` using the
/// structured ENVELOPE + BODYSTRUCTURE data, falling back to the raw
/// header parse if the envelope is unavailable.
fn fetch_to_summary(
    message: &async_imap::types::Fetch,
    has_attachments: bool,
    references_value: &str,
) -> Option<ImapMessageSummary> {
    let uid = message.uid.unwrap_or(0) as i64;
    if uid == 0 {
        return None;
    }

    let flags: Vec<_> = message.flags().collect();
    let is_read = flags.iter().any(|f| matches!(f, async_imap::types::Flag::Seen));
    let is_starred = flags.iter().any(|f| matches!(f, async_imap::types::Flag::Flagged));

    // Preferred: structured ENVELOPE
    if let Some(env) = message.envelope() {
        let subject = env
            .subject
            .as_ref()
            .map(|s| decode_rfc2047(&String::from_utf8_lossy(s)))
            .unwrap_or_default();
        let sender = env
            .from
            .as_ref()
            .and_then(|from| from.first())
            .map(|addr| format_address(addr))
            .unwrap_or_default();
        let date = env
            .date
            .as_ref()
            .map(|d| parse_date(&String::from_utf8_lossy(d)))
            .unwrap_or(0);
        let message_id = env
            .message_id
            .as_ref()
            .map(|m| String::from_utf8_lossy(m).trim().to_string())
            .unwrap_or_default();
        let in_reply_to = env
            .in_reply_to
            .as_ref()
            .map(|r| String::from_utf8_lossy(r).trim().to_string())
            .unwrap_or_default();
        let references = if references_value.is_empty() {
            in_reply_to.clone()
        } else {
            references_value.to_string()
        };

        return Some(ImapMessageSummary {
            uid,
            subject,
            sender,
            date,
            is_read,
            is_starred,
            has_attachments,
            preview: String::new(),
            message_id,
            references,
        });
    }

    // Fallback: raw header parse
    let header_bytes = message.body().unwrap_or(b"");
    let header_str = String::from_utf8_lossy(header_bytes);
    let subject = parse_header(&header_str, "Subject");
    let sender = parse_header(&header_str, "From");
    let date_str = parse_header(&header_str, "Date");
    let date = parse_date(&date_str);
    let message_id = parse_header(&header_str, "Message-ID");
    let references = parse_header(&header_str, "References");
    let in_reply_to = parse_header(&header_str, "In-Reply-To");
    let references = if references.is_empty() { in_reply_to } else { references };

    Some(ImapMessageSummary {
        uid,
        subject,
        sender,
        date,
        is_read,
        is_starred,
        has_attachments,
        preview: String::new(),
        message_id,
        references,
    })
}

/// Format an async-imap address like "Name <mail@example.com>" (or just the email).
fn format_address(addr: &async_imap::imap_proto::types::Address) -> String {
    let name = addr
        .name
        .as_ref()
        .map(|n| decode_rfc2047(&String::from_utf8_lossy(n)))
        .unwrap_or_default();
    let mailbox = addr.mailbox.as_ref().map(|m| String::from_utf8_lossy(m)).unwrap_or_default();
    let host = addr.host.as_ref().map(|h| String::from_utf8_lossy(h)).unwrap_or_default();

    if !mailbox.is_empty() && !host.is_empty() {
        let email = format!("{}@{}", mailbox, host);
        if !name.is_empty() && !name.contains('@') {
            format!("{} <{}>", name, email)
        } else {
            email
        }
    } else if !name.is_empty() {
        name
    } else {
        String::new()
    }
}

/// Walk the BODYSTRUCTURE tree and determine if any part is a real attachment
/// (not inline, not text/plain, text/html, text/calendar, or multipart/*).
fn has_attachments_from_bodystructure(bs: &async_imap::imap_proto::types::BodyStructure) -> bool {
    use async_imap::imap_proto::types::BodyStructure;
    match bs {
        BodyStructure::Multipart { bodies, .. } => {
            bodies.iter().any(has_attachments_from_bodystructure)
        }
        BodyStructure::Message { common, .. } => {
            let is_inline = common
                .disposition
                .as_ref()
                .map(|d| d.ty.to_lowercase() == "inline")
                .unwrap_or(false);
            !is_inline
        }
        BodyStructure::Basic { common, .. } => {
            let is_inline = common
                .disposition
                .as_ref()
                .map(|d| d.ty.to_lowercase() == "inline")
                .unwrap_or(false);
            if is_inline {
                return false;
            }
            // Basic covers application/*, image/*, etc — these are attachments
            // unless disposition is inline.
            !matches!(common.ty.ty.to_lowercase().as_str(), "text")
        }
        BodyStructure::Text { common, .. } => {
            // text/plain is the message body; text/html / text/calendar are
            // attachments only when not inline.
            let is_inline = common
                .disposition
                .as_ref()
                .map(|d| d.ty.to_lowercase() == "inline")
                .unwrap_or(false);
            let sub = common.ty.subtype.to_lowercase();
            if is_inline {
                false
            } else {
                !matches!(sub.as_str(), "plain")
            }
        }
    }
}

/// Extract the References and In-Reply-To values from a raw header block.
fn extract_reference_headers(header_bytes: &[u8]) -> (String, String) {
    let header_str = String::from_utf8_lossy(header_bytes);
    let references = parse_header(&header_str, "References");
    let in_reply_to = parse_header(&header_str, "In-Reply-To");
    (references, in_reply_to)
}

/// Fetch specific message summaries by UID set string (async-imap).
/// Uses structured ENVELOPE/BODYSTRUCTURE for fast parsing, with the
/// References header fetched alongside (ENVELOPE omits References).
pub async fn fetch_summaries_by_uids(
    session: &mut async_imap::Session<CompatTls>,
    uid_set: &str,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    let messages_stream = timeout(
        IMAP_TIMEOUT,
        session.uid_fetch(
            uid_set,
            "(UID ENVELOPE BODYSTRUCTURE FLAGS BODY.PEEK[HEADER.FIELDS (REFERENCES IN-REPLY-TO)])",
        ),
    )
    .await??;
    let messages: Vec<_> = messages_stream.try_collect().await?;

    let mut summaries = Vec::new();
    for message in &messages {
        let body_bytes = message.body().unwrap_or(b"");
        let (references, _in_reply_to) = extract_reference_headers(body_bytes);
        let has_attachments = message
            .bodystructure()
            .map(has_attachments_from_bodystructure)
            .unwrap_or(false);
        if let Some(summary) = fetch_to_summary(message, has_attachments, &references) {
            summaries.push(summary);
        }
    }

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
    use rfc2047_decoder::{Decoder, RecoverStrategy};
    match Decoder::new()
        .too_long_encoded_word_strategy(RecoverStrategy::Decode)
        .decode(input.as_bytes())
    {
        Ok(s) => s,
        Err(_) => input.to_string(),
    }
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

// ---------------------------------------------------------------------------
// MIME body parsing (via mail-parser crate)
// ---------------------------------------------------------------------------

pub struct ParsedBody {
    pub html: String,
    pub text: String,
    pub has_attachments: bool,
}

/// Parse a raw RFC822 message into HTML + plain text parts.
pub fn parse_message_body(raw: &[u8]) -> ParsedBody {
    use mail_parser::{MessageParser, PartType};

    let parsed = match MessageParser::default().parse(raw) {
        Some(p) => p,
        None => {
            let fallback = String::from_utf8_lossy(raw);
            return ParsedBody {
                html: format!("<pre>{}</pre>", html_escape(&fallback)),
                text: fallback.to_string(),
                has_attachments: false,
            };
        }
    };

    // Collect HTML parts
    let mut html = String::new();
    for part in parsed.html_bodies() {
        match &part.body {
            PartType::Html(s) => {
                html.push_str(s);
                html.push('\n');
            }
            _ => {}
        }
    }

    // Collect plain text parts
    let mut text = String::new();
    for part in parsed.text_bodies() {
        match &part.body {
            PartType::Text(s) => {
                text.push_str(s);
                text.push('\n');
            }
            _ => {}
        }
    }

    // Check for attachments
    let has_attachments = parsed.attachments().next().is_some();

    // If no HTML but we have text, wrap in <pre>
    if html.is_empty() && !text.is_empty() {
        html = format!("<pre>{}</pre>", html_escape(&text));
    }

    // If still empty, try body_text as fallback
    if html.is_empty() && text.is_empty() {
        if let Some(body_text) = parsed.body_text(0) {
            text = body_text.to_string();
            html = format!("<pre>{}</pre>", html_escape(&text));
        }
    }

    tracing::debug!(
        "mail-parser: html_len={} text_len={} attachments={}",
        html.len(),
        text.len(),
        has_attachments
    );

    ParsedBody { html, text, has_attachments }
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
