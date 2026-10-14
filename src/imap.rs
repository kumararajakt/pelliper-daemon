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
/// The authenticated IMAP session type used by the worker.
pub type ImapSession = async_imap::Session<CompatTls>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IMAP_TIMEOUT: Duration = Duration::from_secs(15);

/// Summary fetch query: UID, flags, body structure, the server-computed
/// `ENVELOPE`, and the two threading headers `ENVELOPE` has no field for.
///
/// `ENVELOPE` covers the ten RFC2822 fields the server can decode for us, and
/// is the only source of `To`/`Cc`/`Bcc`/`Reply-To`. It has exactly ten
/// fields and **no `references`**, so `HEADER.FIELDS (REFERENCES
/// IN-REPLY-TO)` is fetched alongside it: `src/threadmodel.cpp` builds its
/// union-find from `references_`, and with it empty every message would be
/// its own thread root.
///
/// The field list is deliberately narrow. Fetching the whole block
/// (`BODY.PEEK[HEADER]`) cost ~5.6 KB per message — almost entirely
/// `Return-Path`, `DKIM-Signature`, `ARC-Seal` and `Received`, none of which
/// anything reads. `PEEK` keeps `\Seen` unset.
///
/// Both attributes cost the same round trip: this is one FETCH either way.
const SUMMARY_FETCH_QUERY: &str =
    "(UID FLAGS BODYSTRUCTURE ENVELOPE BODY.PEEK[HEADER.FIELDS (REFERENCES IN-REPLY-TO)])";

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

/// Read a folder's UIDVALIDITY, the value that tells us whether the server's
/// UIDs are still the ones we cached. EXAMINE avoids taking a writable lock.
/// Returns 0 when the server does not report one, which callers must treat as
/// "unknown" rather than as a change.
pub async fn folder_uidvalidity(
    session: &mut ImapSession,
    folder: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let mailbox = session.examine(folder).await.map_err(|e| e.to_string())?;
    Ok(mailbox.uid_validity.map(i64::from).unwrap_or(0))
}

/// EXAMINE a folder once and return everything the sync loop needs:
/// `(uidvalidity, exists, uid_next)` — 0 / None when the server does not
/// report them. One round trip instead of probing each value separately.
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

/// The highest UID implied by `UIDNEXT`, but only when the server metadata is
/// internally consistent — Flectar's rule. UIDs are unique positive integers,
/// so `UIDNEXT - 1` (the highest UID ever assigned) cannot sit below the
/// number of live messages; an inconsistent or absent UIDNEXT reads as
/// "untrusted" and the caller falls back to SEARCH.
fn trusted_highest_uid(exists: u32, uid_next: Option<u32>) -> Option<u32> {
    let high = uid_next?.checked_sub(1)?;
    (high >= exists).then_some(high)
}

/// The mailbox's highest UID: `UIDNEXT - 1` when consistent with EXISTS,
/// otherwise the maximum of `UID SEARCH ALL`. Returns 0 for an empty mailbox.
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

/// Every UID strictly above `last_seen`: `UID SEARCH UID (last_seen+1):*`.
/// The client-side filter is load-bearing, not redundant — when `last_seen`
/// is above the mailbox's highest UID the `n:*` range reverses (RFC 3501) and
/// could return that older highest UID.
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

/// Keep only the UIDs strictly above `last_seen`, sorted ascending and
/// deduped (SEARCH results are a set, but ordering must not be assumed).
fn filter_uids_after(uids: Vec<i64>, last_seen: i64) -> Vec<i64> {
    let mut uids: Vec<i64> = uids.into_iter().filter(|u| *u > last_seen).collect();
    uids.sort_unstable();
    uids.dedup();
    uids
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
    summaries.sort_by(|a, b| b.headers.date_ts.cmp(&a.headers.date_ts));
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
    // Parallel to `folders`: whether the kind came from a SPECIAL-USE attribute.
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

/// Classify a mailbox, also reporting whether the kind came from an RFC 6154
/// SPECIAL-USE attribute (`true`) rather than name matching (`false`).
///
/// The flag is what lets `list_folders` prefer the server's real special-use
/// folder when it also exposes a stray folder that merely *looks* like it fills
/// the same role — e.g. Gmail's `[Gmail]/Trash` (`\Trash`) sitting next to a
/// plain top-level `Trash` label.
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
        // Servers without RFC 6154 support (Outlook/Exchange, older Dovecot)
        // send no attributes at all, so the name heuristic is the only signal
        // they will ever produce.
        kind = FolderKind::from_path(path);
    }
    (kind, from_attribute)
}

/// When a role has a real SPECIAL-USE folder, demote any name-matched impostor
/// of that role to `Custom`.
///
/// Without this, a stray folder such as a plain top-level `Trash` label can
/// shadow the server's actual `[Gmail]/Trash`, and mail "deleted" into it never
/// leaves the mailbox — it just gains a label, while the sidebar shows the role
/// twice. `Inbox` is excluded: it is never resolved by heuristic lookup.
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

/// Convert a library `Fetch` response into an `ImapMessageSummary`.
///
/// The typed `ENVELOPE` supplies every address and scalar header, and
/// `HEADER.FIELDS (REFERENCES IN-REPLY-TO)` — which imap-proto also normalizes
/// to `SectionPath::Full(Header)`, so `fetch.header()` returns it — supplies
/// the two fields ENVELOPE cannot express. Whichever the server populated wins,
/// envelope first, so servers that omit one still produce a full row.
/// Attachments come from the typed BODYSTRUCTURE.
///
/// `preview` is read from the `BODY[1]` partial (`SectionPath::Part([1], None)`),
/// which `SUMMARY_FETCH_QUERY` does not currently request — so it is always
/// empty until a `BODY.PEEK[1]<0.N>` item is added back to the query.
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

    // Inline preview from the BODY[1] partial; absent because the fetch query
    // does not request it, so this is always empty.
    let preview = fetch
        .section(&SectionPath::Part(vec![1], None))
        .map(preview_from_unknown)
        .unwrap_or_default();

    let header_bytes = fetch.header().unwrap_or_default();
    let header_text = String::from_utf8_lossy(header_bytes);
    let env = fetch.envelope();

    // ENVELOPE is now the *only* source for these: the fetched header block
    // holds just References and In-Reply-To, so there is nothing to fall back
    // to for the rest. A server that omits ENVELOPE therefore yields empty
    // scalars — it would also break every other IMAP client.
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

    // The six address lists, each already JSON. A list the server did not send
    // is the empty array, and so is a wholly absent ENVELOPE, so the column
    // contract in `cache.rs` ("absent is `[]`, not NULL") holds either way.
    const NO_ADDRESSES: &str = "[]";
    let (from, from_hdr, reply_to, to, cc, bcc) = match &env {
        Some(e) => (
            envelope_addresses(e.from.as_deref()),
            envelope_addresses(e.sender.as_deref()),
            envelope_addresses(e.reply_to.as_deref()),
            envelope_addresses(e.to.as_deref()),
            envelope_addresses(e.cc.as_deref()),
            // Stripped by the sending MTA, so ENVELOPE rarely carries it.
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

    // Message-IDs are normalized (angle brackets / comments stripped) so that
    // threading links match no matter which source supplied them — the UI's
    // union-find in `src/threadmodel.cpp` compares these strings verbatim.
    let message_id = normalize_message_id(&raw_message_id);
    let mut in_reply_to = normalize_references(&envelope_in_reply_to);
    if in_reply_to.is_empty() {
        in_reply_to = normalize_references(&parse_header(&header_text, "In-Reply-To"));
    }
    // `References` has no ENVELOPE field, so the header block is its only
    // source. Fall back to In-Reply-To so a single-level reply still threads
    // when the sender omitted References.
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

/// The first `{name, email}` entry of a stored address header, for indexing a
/// single correspondent (e.g. the sender autocomplete).
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

/// Decode an `ENVELOPE` scalar (date, subject, in-reply-to, message-id).
/// Returns `None` for NIL so the caller can fall back to the header block.
fn envelope_scalar(value: Option<&[u8]>) -> Option<String> {
    let raw = value.filter(|b| !b.is_empty())?;
    Some(decode_rfc2047(&String::from_utf8_lossy(raw)))
}

/// One entry of a stored address header, serialized into the JSON array that
/// lives in the `*_addr` columns of `message_headers`.
///
/// `name` and `email` are always present (possibly empty strings) so the UI
/// never has to test for a missing key.
#[derive(serde::Serialize)]
struct AddressEntry {
    name: String,
    email: String,
}

/// Format an `ENVELOPE` address list as a JSON array of `{name, email}`
/// objects, e.g. `[{"name":"Alice","email":"alice@example.com"}]`.
///
/// Structured rather than a flat display string so recipients stay
/// addressable: group syntax (a `host` of NIL) yields an entry with an empty
/// `email` and the group name in `name`, which a display string would lose.
/// The UI turns one of these back into display text on read.
fn envelope_addresses(list: Option<&[Address<'_>]>) -> String {
    let entries: Vec<AddressEntry> = list
        .unwrap_or_default()
        .iter()
        .map(envelope_address)
        .filter(|e| !e.name.is_empty() || !e.email.is_empty())
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Split one `ENVELOPE` address into its personal name and addr-spec.
fn envelope_address(addr: &Address<'_>) -> AddressEntry {
    let part = |v: Option<&[u8]>| -> String {
        v.filter(|b| !b.is_empty())
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default()
    };
    let mailbox = part(addr.mailbox.as_deref());
    let host = part(addr.host.as_deref());
    // A NIL host with a non-empty mailbox is group syntax: the mailbox holds
    // the *group name*, not an addr-spec, so it belongs in `name` with an
    // empty `email`. Putting it in `email` would hand the UI something that
    // looks addressable but is not.
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

/// Parse an address header out of a raw header block into the same JSON shape
/// `envelope_addresses` produces, so both write paths agree on one format.
///
/// Used only by the body-fetch path, which has the whole message; the FETCH
/// path gets structured addresses from ENVELOPE directly.
fn address_list_from_raw(value: &str) -> String {
    let entries: Vec<AddressEntry> = split_address_list(value)
        .into_iter()
        .filter_map(|one| parse_one_address(&one).map(|(email, name)| AddressEntry { name, email }))
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Split one address into `(email, display name)`.
///
/// Takes the last `<...>` as the addr-spec and everything before it as the
/// display name; with no angle brackets the whole thing is the addr-spec.
/// Strips RFC2047 encoding from the name, matching `envelope_address`.
fn parse_one_address(one: &str) -> Option<(String, String)> {
    let one = one.trim();
    if one.is_empty() {
        return None;
    }
    // Group syntax, e.g. `undisclosed-recipients:;`. The name is the group
    // label; there is no addr-spec, matching how `envelope_address` treats a
    // NIL host.
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

/// Split an address header on top-level commas, respecting quoted strings and
/// angle brackets so `Doe, Jane <j@x>` stays in one piece.
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

/// Split a raw RFC822 message into its header block and body.
///
/// Returns everything before the first blank line, excluding that line's own
/// terminator, so the result is exactly the header lines. Handles both CRLF
/// and bare-LF endings. A message with no blank line is treated as
/// all-headers rather than erroring.
pub fn split_header_block(raw: &[u8]) -> &[u8] {
    let mut start = 0usize;
    while start < raw.len() {
        let Some(rel) = raw[start..].iter().position(|&b| b == b'\n') else {
            break; // unterminated final line
        };
        let line_end = start + rel;
        // An empty line ("\n") or a bare CR ("\r\n") ends the header block.
        if raw[start..line_end].iter().all(|&b| b == b'\r') {
            // Drop the terminator of the header line above it.
            return match raw[..start].strip_suffix(b"\r\n") {
                Some(block) => block,
                None => raw[..start].strip_suffix(b"\n").unwrap_or(&raw[..start]),
            };
        }
        start = line_end + 1;
    }
    raw
}

/// Build a `MessageHeaders` from a complete raw RFC822 message — the
/// `BODY.PEEK[]` payload.
///
/// The body-fetch path already holds every header, so this fills in
/// `message_headers` at no extra network cost. It covers messages the
/// header-only FETCH never reached (older mail, backfill that has not run)
/// and refreshes rows whose ENVELOPE fields were previously empty.
/// Build a `MessageHeaders` from a complete raw RFC822 message.
///
/// Used by the body-fetch path, which already has the whole message in hand,
/// so it reads every header directly. Address fields go out in the same JSON
/// shape the FETCH path produces, so the two paths agree on one column format
/// — that matters because they can write the same message in either order.
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
        // Same fallback the FETCH path applies, so both agree on one column.
        references: if references_raw.is_empty() {
            in_reply_to
        } else {
            normalize_references(&references_raw)
        },
        message_id: normalize_message_id(&at("Message-ID")),
    }
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

/// Whether the server advertises support for the IDLE extension. The IDLE
/// extension lets the server push mailbox changes without the client polling.
pub async fn supports_idle(session: &mut ImapSession) -> bool {
    match session.capabilities().await {
        Ok(caps) => caps.has_str("IDLE"),
        Err(_) => false,
    }
}

#[cfg(test)]
#[path = "imap.test.rs"]
mod tests;
