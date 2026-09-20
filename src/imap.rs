use base64::Engine;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_util::compat::TokioAsyncReadCompatExt;

use async_imap::imap_proto::types::{BodyStructure, SectionPath};
use async_imap::types::{Fetch, Flag, NameAttribute};
use futures::StreamExt;

pub type CompatTls = tokio_util::compat::Compat<TlsStream<TcpStream>>;
/// The authenticated IMAP session type used by the worker.
pub type ImapSession = async_imap::Session<CompatTls>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IMAP_TIMEOUT: Duration = Duration::from_secs(15);

/// Summary fetch query: UID, flags, body structure, inline preview bytes,
/// and the headers needed for subject/sender/date/threading.
const SUMMARY_FETCH_QUERY: &str = "(UID FLAGS BODYSTRUCTURE BODY.PEEK[1]<0.2048> BODY.PEEK[HEADER.FIELDS (SUBJECT FROM DATE MESSAGE-ID REFERENCES IN-REPLY-TO)])";

/// Inline preview characters kept per summary.
const PREVIEW_CHARS: usize = 240;

pub async fn test_connection_oauth(host: &str, port: u16, email: &str, access_token: &str) -> bool {
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

    let result = timeout(IMAP_TIMEOUT, async {
        let mut client = async_imap::Client::new(stream.compat());
        // Consume the server greeting: async-imap's `authenticate` does not
        // read it before AUTHENTICATE, which deadlocks against Gmail's
        // XOAUTH2 flow (chatmail/async-imap issue #84).
        client.read_response().await?.ok_or("no server greeting")?;
        let auth = XoAuth2 {
            user: email.to_owned(),
            access_token: access_token.to_owned(),
        };
        let mut session = client
            .authenticate("XOAUTH2", &auth)
            .await
            .map_err(|(e, _)| e.to_string())?;
        let _ = timeout(Duration::from_secs(3), session.logout()).await;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
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

pub async fn test_connection_password(host: &str, port: u16, email: &str, password: &str) -> bool {
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

/// XOAUTH2 SASL authenticator for async-imap's `Client::authenticate`.
///
/// async-imap drives the base64 SASL exchange; this type supplies the initial
/// client response that Gmail's XOAUTH2 expects. The server greeting must be
/// consumed before authenticating (see `connect_session_oauth`).
struct XoAuth2 {
    user: String,
    access_token: String,
}

impl async_imap::Authenticator for &XoAuth2 {
    type Response = String;

    fn process(&mut self, _data: &[u8]) -> Self::Response {
        format!(
            "user={}\x01auth=Bearer {}\x01\x01",
            self.user, self.access_token
        )
    }
}

/// Connect via XOAUTH2 and return a live authenticated session.
pub async fn connect_session_oauth(
    host: &str,
    port: u16,
    email: &str,
    access_token: &str,
) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let stream = connect_tls_stream(host, port).await?;
    let mut client = async_imap::Client::new(stream.compat());

    // Consume the server greeting before authenticating: async-imap's
    // `authenticate` sends AUTHENTICATE immediately and never reads the
    // greeting, which deadlocks Gmail's XOAUTH2 flow (chatmail/async-imap
    // issue #84, maintainer-confirmed workaround).
    client.read_response().await?.ok_or("no server greeting")?;

    let auth = XoAuth2 {
        user: email.to_owned(),
        access_token: access_token.to_owned(),
    };
    let session = client
        .authenticate("XOAUTH2", &auth)
        .await
        .map_err(|(e, _)| e.to_string())?;
    Ok(session)
}

/// Connect with a password and return a live authenticated session.
pub async fn connect_session_password(
    host: &str,
    port: u16,
    email: &str,
    password: &str,
) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let client = connect_tls(host, port).await?;
    // `login` reads responses in a loop and skips the greeting itself.
    let session = client
        .login(email, password)
        .await
        .map_err(|(e, _)| e.to_string())?;
    Ok(session)
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

    /// Stable lowercase string, persisted in the cache `folders.kind` column
    /// and consumed by the UI model (mirror of `FolderModel::roleFromKind`).
    pub fn as_str(&self) -> &'static str {
        match self {
            FolderKind::Inbox => "inbox",
            FolderKind::Sent => "sent",
            FolderKind::Drafts => "drafts",
            FolderKind::Trash => "trash",
            FolderKind::Junk => "junk",
            FolderKind::Archive => "archive",
            FolderKind::Starred => "starred",
            FolderKind::Custom => "custom",
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

/// Fetch the newest `limit` message summaries for a folder, newest first.
///
/// Uses a *sequence-number* range (`{last}:*`) so the page is the newest
/// `limit` messages even when UIDs are sparse — the old UID FETCH {exists}:*
/// only returned the single newest message on a fresh mailbox.
pub async fn fetch_recent_summaries(
    session: &mut ImapSession,
    folder: &str,
    limit: usize,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = session.examine(folder).await.map_err(|e| e.to_string())?;
    let total = mailbox.exists;
    if total == 0 {
        return Ok(Vec::new());
    }
    let start = total.saturating_sub(limit as u32).max(1);
    let stream = session
        .fetch(format!("{}:*", start), SUMMARY_FETCH_QUERY)
        .await
        .map_err(|e| e.to_string())?;
    futures::pin_mut!(stream);
    let mut summaries = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.map_err(|e| e.to_string())?;
        if let Some(msg) = summary_from_fetch(&f) {
            summaries.push(msg);
        }
    }
    summaries.sort_by(|a, b| b.date.cmp(&a.date));
    summaries.truncate(limit);
    tracing::info!(
        "Fetched {} recent messages from {}",
        summaries.len(),
        folder
    );
    Ok(summaries)
}

/// Fetch summaries for an explicit UID set (used by backfill chunks).
pub async fn fetch_summaries_by_uids(
    session: &mut ImapSession,
    folder: &str,
    uid_set: &str,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    let mut stream = session
        .uid_fetch(uid_set, SUMMARY_FETCH_QUERY)
        .await
        .map_err(|e| e.to_string())?;
    let mut summaries = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.map_err(|e| e.to_string())?;
        if let Some(msg) = summary_from_fetch(&f) {
            summaries.push(msg);
        }
    }
    Ok(summaries)
}

/// All UIDs in a folder (from UID SEARCH ALL), for backfill planning.
pub async fn fetch_all_uids(
    session: &mut ImapSession,
    folder: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    let uids = session.uid_search("ALL").await.map_err(|e| e.to_string())?;
    Ok(uids.into_iter().map(|u| u as i64).collect())
}

/// Fetch the raw RFC822 body of one message.
pub async fn fetch_body(
    session: &mut ImapSession,
    folder: &str,
    uid: i64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    let mut stream = session
        .uid_fetch(uid.to_string(), "(BODY.PEEK[])")
        .await
        .map_err(|e| e.to_string())?;
    let mut body = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.map_err(|e| e.to_string())?;
        if let Some(bytes) = f.body() {
            body = bytes.to_vec();
        }
    }
    Ok(body)
}

/// Add/remove message flags by UID. The caller must have SELECTed the folder
/// (read-write); STORE requires a writable mailbox.
pub async fn store_flags(
    session: &mut ImapSession,
    uid: i64,
    add: &[&str],
    remove: &[&str],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !add.is_empty() {
        let list = add.join(" ");
        let stream = session
            .uid_store(uid.to_string(), format!("+FLAGS ({})", list))
            .await
            .map_err(|e| e.to_string())?;
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
    }
    if !remove.is_empty() {
        let list = remove.join(" ");
        let stream = session
            .uid_store(uid.to_string(), format!("-FLAGS ({})", list))
            .await
            .map_err(|e| e.to_string())?;
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
    }
    Ok(())
}

/// List all folders, classifying each by SPECIAL-USE attribute with a
/// name-heuristic fallback (same contract as `FolderKind::as_str`).
pub async fn list_folders(
    session: &mut ImapSession,
) -> Result<Vec<ImapFolder>, Box<dyn std::error::Error + Send + Sync>> {
    let mut folders = Vec::new();
    let stream = session
        .list(None, Some("*"))
        .await
        .map_err(|e| e.to_string())?;
    futures::pin_mut!(stream);
    while let Some(name) = stream.next().await {
        let name = name.map_err(|e| e.to_string())?;
        let mut noselect = false;
        let mut kind = FolderKind::Custom;
        let mut special_use_found = false;
        for attr in name.attributes() {
            match attr {
                NameAttribute::NoSelect => noselect = true,
                NameAttribute::All => {
                    kind = FolderKind::Archive;
                    special_use_found = true;
                }
                NameAttribute::Archive => {
                    kind = FolderKind::Archive;
                    special_use_found = true;
                }
                NameAttribute::Drafts => {
                    kind = FolderKind::Drafts;
                    special_use_found = true;
                }
                NameAttribute::Flagged => {
                    kind = FolderKind::Starred;
                    special_use_found = true;
                }
                NameAttribute::Junk => {
                    kind = FolderKind::Junk;
                    special_use_found = true;
                }
                NameAttribute::Sent => {
                    kind = FolderKind::Sent;
                    special_use_found = true;
                }
                NameAttribute::Trash => {
                    kind = FolderKind::Trash;
                    special_use_found = true;
                }
                NameAttribute::Extension(name) => {
                    if let Some(k) = FolderKind::from_flag(name.as_ref()) {
                        kind = k;
                        special_use_found = true;
                    }
                }
                _ => {}
            }
        }
        if !special_use_found {
            kind = FolderKind::from_path(name.name());
        }
        folders.push(ImapFolder {
            path: name.name().to_string(),
            noselect,
            kind,
        });
    }
    tracing::info!("Discovered {} folders", folders.len());
    Ok(folders)
}

/// Find a folder by semantic kind (e.g. Trash for delete/move-to-trash).
pub async fn find_folder_by_kind(
    session: &mut ImapSession,
    wanted: FolderKind,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let folders = list_folders(session).await?;
    Ok(folders
        .into_iter()
        .find(|f| f.kind == wanted && !f.noselect)
        .map(|f| f.path))
}

/// Convert a library `Fetch` response into an `ImapMessageSummary`.
///
/// Headers come from the `BODY[HEADER.FIELDS (...)]` literal (normalized by
/// imap-proto to `SectionPath::Full(Header)`, so `fetch.header()` returns it);
/// the preview from the `BODY[1]<0.2048>` partial (matched as
/// `SectionPath::Part([1], None)`); attachments from the typed BODYSTRUCTURE.
fn summary_from_fetch(fetch: &Fetch) -> Option<ImapMessageSummary> {
    let uid = fetch.uid.unwrap_or(0) as i64;
    if uid == 0 {
        tracing::warn!("UID not found in FETCH response");
    }

    let mut is_read = false;
    let mut is_starred = false;
    for flag in fetch.flags() {
        match flag {
            Flag::Seen => is_read = true,
            Flag::Flagged => is_starred = true,
            _ => {}
        }
    }

    let has_attachments = fetch
        .bodystructure()
        .map(bodystructure_has_attachments)
        .unwrap_or(false);

    // Inline preview from the BODY[1] partial (may be absent on some servers).
    let preview = fetch
        .section(&SectionPath::Part(vec![1], None))
        .map(preview_from_unknown)
        .unwrap_or_default();

    // Header block from the BODY[HEADER.FIELDS (...)] literal.
    let header_bytes = fetch.header().unwrap_or_default();
    let header_text = String::from_utf8_lossy(header_bytes);

    let subject = parse_header(&header_text, "Subject");
    let sender = parse_header(&header_text, "From");
    let date_str = parse_header(&header_text, "Date");
    let date = parse_date(&date_str);
    // Normalize Message-IDs (strip angle brackets / comments) so threading
    // links match regardless of raw vs ENVELOPE source.
    let message_id = normalize_message_id(&parse_header(&header_text, "Message-ID"));
    let references_raw = parse_header(&header_text, "References");
    let in_reply_to = normalize_references(&parse_header(&header_text, "In-Reply-To"));

    // Use In-Reply-To as fallback for References if empty.
    let references = if references_raw.is_empty() {
        in_reply_to
    } else {
        normalize_references(&references_raw)
    };

    if subject.is_empty() && sender.is_empty() && uid != 0 {
        tracing::debug!(
            "Empty headers for uid={}, header_text (first 200): {:?}",
            uid,
            &header_text[..header_text.len().min(200)]
        );
    }

    Some(ImapMessageSummary {
        uid,
        subject,
        sender,
        date,
        is_read,
        is_starred,
        has_attachments,
        preview,
        message_id,
        references,
    })
}

/// Detect attachments from a typed BODYSTRUCTURE: a part with a name/filename
/// parameter, an explicit `attachment` disposition, or a non-text top-level
/// type (application/image/video/audio/font/model). Mirrors the old raw
/// BODYSTRUCTURE text heuristic.
fn bodystructure_has_attachments(bs: &BodyStructure<'_>) -> bool {
    match bs {
        BodyStructure::Multipart { bodies, .. } => bodies.iter().any(bodystructure_has_attachments),
        BodyStructure::Basic { common, .. }
        | BodyStructure::Text { common, .. }
        | BodyStructure::Message { common, .. } => {
            // A name/filename parameter directly signals an attachment.
            let named = common.ty.params.as_ref().is_some_and(|params| {
                params.iter().any(|(k, _)| {
                    k.eq_ignore_ascii_case("name") || k.eq_ignore_ascii_case("filename")
                })
            }) || common.disposition.as_ref().is_some_and(|d| {
                d.params.as_ref().is_some_and(|params| {
                    params
                        .iter()
                        .any(|(k, _)| k.eq_ignore_ascii_case("filename"))
                })
            });
            if named {
                return true;
            }
            // Explicit attachment disposition.
            if common
                .disposition
                .as_ref()
                .is_some_and(|d| d.ty.eq_ignore_ascii_case("attachment"))
            {
                return true;
            }
            // Non-text top-level type → treated as a binary attachment.
            let main = common.ty.ty.to_ascii_lowercase();
            matches!(
                main.as_str(),
                "application" | "image" | "video" | "audio" | "font" | "model"
            ) || main.starts_with("x-")
        }
    }
}

/// Build an inline preview from raw fetched bytes when the transfer encoding
/// is unknown (raw session path). Heuristic: base64 if it looks like base64,
/// otherwise apply quoted-printable decoding (a no-op for plain ASCII).
fn preview_from_unknown(bytes: &[u8]) -> String {
    let cleaned: Vec<u8> = bytes
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if cleaned.len() % 4 == 0
        && cleaned.len() >= 16
        && cleaned.iter().all(|b| {
            b.is_ascii_alphanumeric()
                || *b == b'+'
                || *b == b'/'
                || *b == b'='
                || *b == b'-'
                || *b == b'_'
        })
    {
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&cleaned) {
            return clean_preview(&decoded);
        }
    }
    clean_preview(&decode_quoted_printable(bytes))
}

/// Normalize preview bytes into clean display text: strip HTML tags, collapse
/// whitespace, and truncate to PREVIEW_CHARS characters.
fn clean_preview(raw: &[u8]) -> String {
    let mut text = String::from_utf8_lossy(raw).to_string();
    if text.contains('<') && text.contains('>') {
        text = strip_html_tags(&text);
    }
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(PREVIEW_CHARS)
        .collect()
}

/// Remove HTML tags (text between '<' and '>'), keeping the text in between.
fn strip_html_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if in_tag => {}
            _ => out.push(c),
        }
    }
    out
}

/// Decode quoted-printable: `=XX` hex escapes and soft line breaks (`=\r\n`).
/// Safe for plain text (unrecognized `=` is kept as-is).
fn decode_quoted_printable(bytes: &[u8]) -> Vec<u8> {
    fn hex_val(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'=' && i + 2 < bytes.len() && bytes[i + 1] == b'\r' && bytes[i + 2] == b'\n'
        {
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
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

/// Strip RFC 5322 comments from a header value.  Comments are balanced
/// `(...)` groups; the comment text itself is discarded.
fn strip_rfc_comments(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0usize;
    for ch in s.chars() {
        match ch {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => out.push(ch),
            _ => {}
        }
    }
    out
}

/// Extract all Message-IDs from a raw header value (References, In-Reply-To,
/// or Message-ID), returning their canonical form: angle brackets removed,
/// whitespace trimmed, RFC 5322 comments skipped.  Entities without `@` are
/// only kept when angle-bracketed (strict ID form).
fn extract_message_ids(raw: &str) -> Vec<String> {
    let text = strip_rfc_comments(raw);
    let mut ids = Vec::new();

    // Angle-bracketed ids, handled first so comments can't confuse them.
    let mut rest = text.trim();
    while let Some(open) = rest.find('<') {
        let tail = &rest[open + 1..];
        match tail.find('>') {
            Some(close) => {
                let id = tail[..close].trim();
                if !id.is_empty() {
                    ids.push(id.to_string());
                }
                rest = tail[close + 1..].trim();
            }
            None => break,
        }
    }

    // Remaining bare tokens that mention '@' (non-bracketed ids).
    for tok in rest.split_whitespace() {
        let tok = tok.trim_matches(|c| c == ',' || c == ';' || c == '<' || c == '>');
        if tok.contains('@') && !tok.is_empty() && !ids.iter().any(|i| i == tok) {
            ids.push(tok.to_string());
        }
    }

    // Dedup preserving first occurrence.
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    ids
}

/// Canonical form of a single Message-ID header value (or "") — the first
/// id token, without angle brackets or comments.
fn normalize_message_id(raw: &str) -> String {
    extract_message_ids(raw)
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// Canonical form of a References / In-Reply-To header value: the extracted
/// Message-IDs, space-separated.  Empty string when no ids are present.
fn normalize_references(raw: &str) -> String {
    extract_message_ids(raw).join(" ")
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

pub struct ParsedAttachment {
    pub name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

pub struct ParsedBody {
    pub html: String,
    pub text: String,
    pub has_attachments: bool,
    pub attachments: Vec<ParsedAttachment>,
}

/// Parse a raw RFC822 message into HTML + plain text parts, plus the list of
/// real attachments (binary parts with a filename or an `attachment`
/// disposition; inline `cid:` images are excluded).
pub fn parse_message_body(raw: &[u8]) -> ParsedBody {
    use mail_parser::{MimeHeaders, PartType};

    let parsed = match mail_parser::MessageParser::default().parse(raw) {
        Some(p) => p,
        None => {
            let fallback = String::from_utf8_lossy(raw);
            return ParsedBody {
                html: format!("<pre>{}</pre>", html_escape(&fallback)),
                text: fallback.to_string(),
                has_attachments: false,
                attachments: Vec::new(),
            };
        }
    };

    let mut attachments = Vec::new();
    for part in parsed.attachments() {
        let name = part.attachment_name().unwrap_or("").trim().to_string();
        let is_attachment = part
            .content_disposition()
            .map(|cd| cd.c_type.eq_ignore_ascii_case("attachment"))
            .unwrap_or(false);
        // Skip parts that are neither dispositioned as attachments nor named:
        // typically embedded inline images (logo, emoji) referenced by cid:.
        if !is_attachment && name.is_empty() {
            continue;
        }
        let content_type = match part.content_type() {
            Some(ct) => format!(
                "{}/{}",
                ct.c_type,
                ct.c_subtype.as_deref().unwrap_or_default()
            ),
            None => "application/octet-stream".to_string(),
        };
        attachments.push(ParsedAttachment {
            name,
            content_type,
            bytes: part.contents().to_vec(),
        });
    }

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

    // Check for attachments (matches the downloadable list above, so the
    // paperclip indicator tracks the attachment panel).
    let has_attachments = !attachments.is_empty();

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

    ParsedBody {
        html,
        text,
        has_attachments,
        attachments,
    }
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_message_id_strips_brackets_and_comments() {
        assert_eq!(normalize_message_id("<a@b.com>"), "a@b.com");
        assert_eq!(normalize_message_id("a@b.com"), "a@b.com");
        // RFC 5322 comment trailing the id
        assert_eq!(normalize_message_id("<a@b.com> (Comment)"), "a@b.com");
        // Comment first, then the id
        assert_eq!(normalize_message_id("(C) <a@b.com>"), "a@b.com");
        // Nested comments must not disrupt extraction
        assert_eq!(normalize_message_id("<a@b.com> (x (y))"), "a@b.com");
        assert_eq!(normalize_message_id(""), "");
        assert_eq!(normalize_message_id("   "), "");
    }

    #[test]
    fn normalize_references_joins_and_dedups() {
        assert_eq!(
            normalize_references("<a@b.com> <c@d.com>"),
            "a@b.com c@d.com"
        );
        // Trailing comment is dropped
        assert_eq!(
            normalize_references("<a@b.com> <c@d.com> (thread topic)"),
            "a@b.com c@d.com"
        );
        // Bare ids without angle brackets are kept
        assert_eq!(normalize_references("a@b.com c@d.com"), "a@b.com c@d.com");
        // Duplicates are removed
        assert_eq!(normalize_references("<a@b.com> <a@b.com>"), "a@b.com");
        assert_eq!(normalize_references(""), "");
    }

    #[test]
    fn threading_matches_after_normalization() {
        // A raw-path message stored `message_id` with brackets; the parent's
        // References (ENVELOPE source) had none. After normalization both are
        // the same bare id, so threadmodel.cpp's linkage finds the parent.
        let child_message_id = normalize_message_id("<reply@host>");
        let parent_refs = normalize_references("reply@host");
        assert_eq!(child_message_id, parent_refs);
    }
}
