use base64::Engine;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_util::compat::TokioAsyncReadCompatExt;

use async_imap::imap_proto::types::{Address, BodyStructure, SectionPath};
use async_imap::types::{Fetch, Flag, NameAttribute};
use futures::StreamExt;

use crate::types::{FolderKind, ImapFolder, ImapMessageSummary, MessageHeaders};

pub type CompatTls = tokio_util::compat::Compat<TlsStream<TcpStream>>;

pub type ImapSession = async_imap::Session<CompatTls>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IMAP_TIMEOUT: Duration = Duration::from_secs(15);

const SUMMARY_FETCH_QUERY: &str =
    "(UID FLAGS BODYSTRUCTURE ENVELOPE BODY.PEEK[HEADER.FIELDS (REFERENCES IN-REPLY-TO)])";

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

pub async fn connect_session_oauth(
    host: &str,
    port: u16,
    email: &str,
    access_token: &str,
) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let stream = connect_tls_stream(host, port).await?;
    let mut client = async_imap::Client::new(stream.compat());

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

pub async fn connect_session_password(
    host: &str,
    port: u16,
    email: &str,
    password: &str,
) -> Result<ImapSession, Box<dyn std::error::Error + Send + Sync>> {
    let client = connect_tls(host, port).await?;

    let session = client
        .login(email, password)
        .await
        .map_err(|(e, _)| e.to_string())?;
    Ok(session)
}

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

async fn connect_tls(
    host: &str,
    port: u16,
) -> Result<async_imap::Client<CompatTls>, Box<dyn std::error::Error + Send + Sync>> {
    let tls_stream = connect_tls_stream(host, port).await?;
    let compat = tls_stream.compat();
    let client = async_imap::Client::new(compat);
    Ok(client)
}

pub async fn folder_uidvalidity(
    session: &mut ImapSession,
    folder: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = session.examine(folder).await.map_err(|e| e.to_string())?;
    Ok(mailbox.uid_validity.map(i64::from).unwrap_or(0))
}

pub async fn examine_folder(
    session: &mut ImapSession,
    folder: &str,
) -> Result<(i64, u32, Option<u32>), Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = session.examine(folder).await.map_err(|e| e.to_string())?;
    Ok((
        mailbox.uid_validity.map(i64::from).unwrap_or(0),
        mailbox.exists,
        mailbox.uid_next,
    ))
}

fn trusted_highest_uid(exists: u32, uid_next: Option<u32>) -> Option<u32> {
    let high = uid_next?.checked_sub(1)?;
    (high >= exists).then_some(high)
}

pub async fn mailbox_highest_uid(
    session: &mut ImapSession,
    folder: &str,
    exists: u32,
    uid_next: Option<u32>,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    if exists == 0 {
        return Ok(0);
    }
    if let Some(high) = trusted_highest_uid(exists, uid_next) {
        return Ok(i64::from(high));
    }
    let uids = fetch_all_uids(session, folder).await?;
    match uids.iter().copied().max() {
        Some(max) if max >= i64::from(exists) => Ok(max),
        Some(_) => Err(format!(
            "mailbox reported {exists} messages but the highest UID is below that"
        )
        .into()),
        None => {
            Err(format!("mailbox reported {exists} messages but UID SEARCH returned none").into())
        }
    }
}

pub async fn uid_search_after(
    session: &mut ImapSession,
    folder: &str,
    last_seen: i64,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    if last_seen < 0 {
        return Ok(Vec::new());
    }
    session.examine(folder).await.map_err(|e| e.to_string())?;
    let query = format!("UID {}:*", last_seen.saturating_add(1));
    let uids = session
        .uid_search(&query)
        .await
        .map_err(|e| e.to_string())?;
    Ok(filter_uids_after(
        uids.into_iter().map(i64::from).collect(),
        last_seen,
    ))
}

fn filter_uids_after(uids: Vec<i64>, last_seen: i64) -> Vec<i64> {
    let mut uids: Vec<i64> = uids.into_iter().filter(|u| *u > last_seen).collect();
    uids.sort_unstable();
    uids.dedup();
    uids
}

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
    summaries.sort_by(|a, b| b.headers.date_ts.cmp(&a.headers.date_ts));
    summaries.truncate(limit);
    tracing::info!(
        "Fetched {} recent messages from {}",
        summaries.len(),
        folder
    );
    Ok(summaries)
}

pub async fn fetch_summaries_by_uids(
    session: &mut ImapSession,
    folder: &str,
    uid_set: &str,
) -> Result<Vec<ImapMessageSummary>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    if uid_set.is_empty() {
        return Ok(Vec::new());
    }
    let stream = session
        .uid_fetch(uid_set, SUMMARY_FETCH_QUERY)
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
    Ok(summaries)
}

pub async fn fetch_uid_flags(
    session: &mut ImapSession,
    folder: &str,
    uid_set: &str,
) -> Result<Vec<(i64, bool, bool)>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    if uid_set.is_empty() {
        return Ok(Vec::new());
    }
    let stream = session
        .uid_fetch(uid_set, "(UID FLAGS)")
        .await
        .map_err(|e| e.to_string())?;
    futures::pin_mut!(stream);
    let mut results = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.map_err(|e| e.to_string())?;
        if let Some(uid) = f.uid {
            let mut is_read = false;
            let mut is_starred = false;
            for flag in f.flags() {
                match flag {
                    Flag::Seen => is_read = true,
                    Flag::Flagged => is_starred = true,
                    _ => {}
                }
            }
            results.push((uid as i64, is_read, is_starred));
        }
    }
    Ok(results)
}

pub async fn fetch_all_uids(
    session: &mut ImapSession,
    folder: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    session.examine(folder).await.map_err(|e| e.to_string())?;
    let uids = session.uid_search("ALL").await.map_err(|e| e.to_string())?;
    Ok(uids.into_iter().map(|u| u as i64).collect())
}

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

pub async fn list_folders(
    session: &mut ImapSession,
) -> Result<Vec<ImapFolder>, Box<dyn std::error::Error + Send + Sync>> {
    let mut folders = Vec::new();

    let mut by_attribute: Vec<bool> = Vec::new();
    let stream = session
        .list(None, Some("*"))
        .await
        .map_err(|e| e.to_string())?;
    futures::pin_mut!(stream);
    while let Some(name) = stream.next().await {
        let name = name.map_err(|e| e.to_string())?;
        let noselect = name.attributes().contains(&NameAttribute::NoSelect);
        let (kind, from_attribute) = classify_with_source(name.name(), name.attributes());
        by_attribute.push(from_attribute);
        folders.push(ImapFolder {
            path: name.name().to_string(),
            noselect,
            kind,
        });
    }
    demote_name_matched_impostors(&mut folders, &by_attribute);
    tracing::info!("Discovered {} folders", folders.len());
    Ok(folders)
}

pub fn classify_with_source(path: &str, attrs: &[NameAttribute]) -> (FolderKind, bool) {
    let mut kind = FolderKind::Custom;
    let mut from_attribute = false;
    for attr in attrs {
        match attr {
            NameAttribute::All | NameAttribute::Archive => kind = FolderKind::Archive,
            NameAttribute::Drafts => kind = FolderKind::Drafts,
            NameAttribute::Flagged => kind = FolderKind::Starred,
            NameAttribute::Junk => kind = FolderKind::Junk,
            NameAttribute::Sent => kind = FolderKind::Sent,
            NameAttribute::Trash => kind = FolderKind::Trash,
            NameAttribute::Extension(name) => {
                if let Some(k) = FolderKind::from_flag(name.as_ref()) {
                    kind = k;
                } else {
                    continue;
                }
            }
            _ => continue,
        }
        from_attribute = true;
    }
    if !from_attribute {
        kind = FolderKind::from_path(path);
    }
    (kind, from_attribute)
}

pub fn demote_name_matched_impostors(folders: &mut [ImapFolder], by_attribute: &[bool]) {
    for role in [
        FolderKind::Sent,
        FolderKind::Drafts,
        FolderKind::Trash,
        FolderKind::Junk,
        FolderKind::Archive,
        FolderKind::Starred,
    ] {
        let has_authoritative = folders
            .iter()
            .zip(by_attribute)
            .any(|(f, attr)| f.kind == role && *attr);
        if has_authoritative {
            for (f, attr) in folders.iter_mut().zip(by_attribute) {
                if f.kind == role && !*attr {
                    tracing::debug!(
                        "demoting name-matched {:?} impostor {} (server has a {:?} folder)",
                        role,
                        f.path,
                        role
                    );
                    f.kind = FolderKind::Custom;
                }
            }
        }
    }
}

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

    let preview = fetch
        .section(&SectionPath::Part(vec![1], None))
        .map(preview_from_unknown)
        .unwrap_or_default();

    let header_bytes = fetch.header().unwrap_or_default();
    let header_text = String::from_utf8_lossy(header_bytes);
    let env = fetch.envelope();

    let subject = env
        .and_then(|e| envelope_scalar(e.subject.as_deref()))
        .unwrap_or_default();
    let date_header = env
        .and_then(|e| envelope_scalar(e.date.as_deref()))
        .unwrap_or_default();
    let raw_message_id = env
        .and_then(|e| envelope_scalar(e.message_id.as_deref()))
        .unwrap_or_default();
    let envelope_in_reply_to = env
        .and_then(|e| envelope_scalar(e.in_reply_to.as_deref()))
        .unwrap_or_default();

    const NO_ADDRESSES: &str = "[]";
    let (from, from_hdr, reply_to, to, cc, bcc) = match &env {
        Some(e) => (
            envelope_addresses(e.from.as_deref()),
            envelope_addresses(e.sender.as_deref()),
            envelope_addresses(e.reply_to.as_deref()),
            envelope_addresses(e.to.as_deref()),
            envelope_addresses(e.cc.as_deref()),
            envelope_addresses(e.bcc.as_deref()),
        ),
        None => (
            NO_ADDRESSES.to_string(),
            NO_ADDRESSES.to_string(),
            NO_ADDRESSES.to_string(),
            NO_ADDRESSES.to_string(),
            NO_ADDRESSES.to_string(),
            NO_ADDRESSES.to_string(),
        ),
    };

    let date = parse_date(&date_header);

    let message_id = normalize_message_id(&raw_message_id);
    let mut in_reply_to = normalize_references(&envelope_in_reply_to);
    if in_reply_to.is_empty() {
        in_reply_to = normalize_references(&parse_header(&header_text, "In-Reply-To"));
    }

    let references_raw = parse_header(&header_text, "References");
    let references = if references_raw.is_empty() {
        in_reply_to.clone()
    } else {
        normalize_references(&references_raw)
    };

    let headers = MessageHeaders {
        subject: subject.clone(),
        from: from.clone(),
        sender: from_hdr,
        reply_to,
        to,
        cc,
        bcc,
        date: date_header,
        date_ts: date,
        in_reply_to,
        references,
        message_id,
    };

    if subject.is_empty() && from == "[]" && uid != 0 {
        tracing::debug!(
            "Empty ENVELOPE for uid={}, fetched header text: {:?}",
            uid,
            &header_text[..header_text.len().min(200)]
        );
    }

    Some(ImapMessageSummary {
        uid,
        from,
        is_read,
        is_starred,
        has_attachments,
        preview,
        headers,
    })
}

pub fn first_address(list_json: &str) -> Option<(String, String)> {
    #[derive(serde::Deserialize)]
    struct Entry {
        #[serde(default)]
        name: String,
        #[serde(default)]
        email: String,
    }
    let entries: Vec<Entry> = serde_json::from_str(list_json).ok()?;
    let first = entries.into_iter().next()?;
    if first.email.is_empty() && first.name.is_empty() {
        None
    } else {
        Some((first.email, first.name))
    }
}

fn envelope_scalar(value: Option<&[u8]>) -> Option<String> {
    let raw = value.filter(|b| !b.is_empty())?;
    Some(decode_rfc2047(&String::from_utf8_lossy(raw)))
}

#[derive(serde::Serialize)]
struct AddressEntry {
    name: String,
    email: String,
}

fn envelope_addresses(list: Option<&[Address<'_>]>) -> String {
    let entries: Vec<AddressEntry> = list
        .unwrap_or_default()
        .iter()
        .map(envelope_address)
        .filter(|e| !e.name.is_empty() || !e.email.is_empty())
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

fn envelope_address(addr: &Address<'_>) -> AddressEntry {
    let part = |v: Option<&[u8]>| -> String {
        v.filter(|b| !b.is_empty())
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default()
    };
    let mailbox = part(addr.mailbox.as_deref());
    let host = part(addr.host.as_deref());

    let (name_from_mailbox, email) = if mailbox.is_empty() {
        (String::new(), host)
    } else if host.is_empty() {
        (mailbox, String::new())
    } else {
        (String::new(), format!("{}@{}", mailbox, host))
    };
    let name = addr
        .name
        .as_deref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_rfc2047(&String::from_utf8_lossy(b)))
        .unwrap_or_default();
    AddressEntry {
        name: if name.is_empty() {
            name_from_mailbox
        } else {
            name
        },
        email,
    }
}

fn address_list_from_raw(value: &str) -> String {
    let entries: Vec<AddressEntry> = split_address_list(value)
        .into_iter()
        .filter_map(|one| parse_one_address(&one).map(|(email, name)| AddressEntry { name, email }))
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

fn parse_one_address(one: &str) -> Option<(String, String)> {
    let one = one.trim();
    if one.is_empty() {
        return None;
    }

    if let Some(group) = one.strip_suffix(":;") {
        let group = group.trim();
        return if group.is_empty() {
            None
        } else {
            Some((String::new(), group.to_string()))
        };
    }
    let (name, email) = match (one.rfind('<'), one.rfind('>')) {
        (Some(lt), Some(gt)) if gt > lt => (
            decode_rfc2047(one[..lt].trim().trim_matches('"').trim()),
            one[lt + 1..gt].trim().to_string(),
        ),
        _ => (String::new(), one.to_string()),
    };
    let email = email.trim_matches(['<', '>']).trim().to_string();
    if name.is_empty() && email.is_empty() {
        None
    } else {
        Some((email, name))
    }
}

fn split_address_list(value: &str) -> Vec<String> {
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
            ',' if !quoted && angle == 0 => parts.push(std::mem::take(&mut current)),
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts.into_iter().map(|p| p.trim().to_string()).collect()
}

pub fn split_header_block(raw: &[u8]) -> &[u8] {
    let mut start = 0usize;
    while start < raw.len() {
        let Some(rel) = raw[start..].iter().position(|&b| b == b'\n') else {
            break;
        };
        let line_end = start + rel;

        if raw[start..line_end].iter().all(|&b| b == b'\r') {
            return match raw[..start].strip_suffix(b"\r\n") {
                Some(block) => block,
                None => raw[..start].strip_suffix(b"\n").unwrap_or(&raw[..start]),
            };
        }
        start = line_end + 1;
    }
    raw
}

pub fn headers_from_raw_message(raw: &[u8]) -> MessageHeaders {
    let block = split_header_block(raw);
    let text = String::from_utf8_lossy(block);
    let at = |name: &str| -> String { parse_header(&text, name) };
    let date = at("Date");
    let in_reply_to_raw = at("In-Reply-To");
    let references_raw = at("References");
    let in_reply_to = normalize_references(&in_reply_to_raw);
    MessageHeaders {
        subject: at("Subject"),
        from: address_list_from_raw(&at("From")),
        sender: address_list_from_raw(&at("Sender")),
        reply_to: address_list_from_raw(&at("Reply-To")),
        to: address_list_from_raw(&at("To")),
        cc: address_list_from_raw(&at("Cc")),
        bcc: address_list_from_raw(&at("Bcc")),
        date_ts: parse_date(&date),
        date,
        in_reply_to: in_reply_to.clone(),

        references: if references_raw.is_empty() {
            in_reply_to
        } else {
            normalize_references(&references_raw)
        },
        message_id: normalize_message_id(&at("Message-ID")),
    }
}

fn bodystructure_has_attachments(bs: &BodyStructure<'_>) -> bool {
    match bs {
        BodyStructure::Multipart { bodies, .. } => bodies.iter().any(bodystructure_has_attachments),
        BodyStructure::Basic { common, .. }
        | BodyStructure::Text { common, .. }
        | BodyStructure::Message { common, .. } => {
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

            if common
                .disposition
                .as_ref()
                .is_some_and(|d| d.ty.eq_ignore_ascii_case("attachment"))
            {
                return true;
            }

            let main = common.ty.ty.to_ascii_lowercase();
            matches!(
                main.as_str(),
                "application" | "image" | "video" | "audio" | "font" | "model"
            ) || main.starts_with("x-")
        }
    }
}

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
            result.push(' ');
            result.push_str(&decode_rfc2047(line.trim()));
        } else {
            break;
        }
    }
    result
}

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

fn extract_message_ids(raw: &str) -> Vec<String> {
    let text = strip_rfc_comments(raw);
    let mut ids = Vec::new();

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

    for tok in rest.split_whitespace() {
        let tok = tok.trim_matches(|c| c == ',' || c == ';' || c == '<' || c == '>');
        if tok.contains('@') && !tok.is_empty() && !ids.iter().any(|i| i == tok) {
            ids.push(tok.to_string());
        }
    }

    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    ids
}

fn normalize_message_id(raw: &str) -> String {
    extract_message_ids(raw)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn normalize_references(raw: &str) -> String {
    extract_message_ids(raw).join(" ")
}

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

fn parse_date(date_str: &str) -> i64 {
    if date_str.is_empty() {
        return 0;
    }

    use chrono::DateTime;
    if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
        return dt.timestamp();
    }

    if let Ok(dt) = DateTime::parse_from_str(date_str, "%d %b %Y %H:%M:%S %z") {
        return dt.timestamp();
    }
    0
}

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

    let has_attachments = !attachments.is_empty();

    if html.is_empty() && !text.is_empty() {
        html = format!("<pre>{}</pre>", html_escape(&text));
    }

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

pub async fn supports_idle(session: &mut ImapSession) -> bool {
    match session.capabilities().await {
        Ok(caps) => caps.has_str("IDLE"),
        Err(_) => false,
    }
}

#[cfg(test)]
#[path = "imap.test.rs"]
mod tests;
