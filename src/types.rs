//! Shared data types used across the daemon (cache, IMAP sync, SMTP).

#[derive(Debug, Clone)]
pub struct Account {
    pub id: i64,
    pub email: String,
    pub display_name: Option<String>,
    pub provider: Option<String>,
    pub auth_type: String,
    pub imap_host: Option<String>,
    pub imap_port: Option<i64>,
    pub imap_security: Option<String>,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<i64>,
    pub smtp_security: Option<String>,
    pub username: Option<String>,
    pub refresh_token: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Folder {
    pub path: String,
    pub unread_count: i32,
    pub noselect: bool,
    /// SPECIAL-USE-derived classification ("inbox", "sent", ..., "custom").
    pub kind: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Message {
    pub uid: i64,
    pub subject: Option<String>,
    pub sender: Option<String>,
    pub date: i64,
    pub in_reply_to: Option<String>,
    pub message_id: Option<String>,
    pub references: Option<String>,
    pub has_attachments: bool,
    pub is_read: bool,
    pub is_starred: bool,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AttachmentRecord {
    pub name: String,
    pub file_path: String,
    pub content_type: String,
    pub size: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OutboxItem {
    pub id: i64,
    pub account_id: i64,
    pub to_addr: String,
    pub cc_addr: Option<String>,
    pub bcc_addr: Option<String>,
    pub reply_to: Option<String>,
    pub subject: String,
    pub body: String,
    pub attempts: i32,
}

/// The kind of a queued non-send mutation, persisted as `pending_actions.kind`.
///
/// Sends are *not* here — they keep their own `outbox` table and SMTP flusher,
/// because they need a different transport and a longer attempt budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// Set read/starred flags; payload is a [`SetFlagsPayload`].
    SetFlags,
    /// Move to another folder; payload is a [`MovePayload`].
    Move,
    /// Permanently delete (flag `\Deleted` + EXPUNGE); payload is empty.
    Delete,
}

impl ActionKind {
    /// Stable lowercase string stored in the database and embedded in logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionKind::SetFlags => "set_flags",
            ActionKind::Move => "move",
            ActionKind::Delete => "delete",
        }
    }

    /// Parse the persisted form. `None` for an unknown kind, which the cache
    /// treats as an error rather than guessing at a destructive operation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "set_flags" => Some(ActionKind::SetFlags),
            "move" => Some(ActionKind::Move),
            "delete" => Some(ActionKind::Delete),
            _ => None,
        }
    }
}

/// Payload for [`ActionKind::SetFlags`]. `None` leaves a flag unchanged, matching
/// the D-Bus `set_message_read` / `set_message_starred` contract.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SetFlagsPayload {
    pub is_read: Option<bool>,
    pub is_starred: Option<bool>,
}

/// Payload for [`ActionKind::Move`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MovePayload {
    /// Destination folder path.
    pub dest: String,
    /// Whether to re-sync the destination afterwards so the moved message shows
    /// up immediately. An explicit move refreshes the destination; a
    /// delete-to-trash does not (the UI is leaving the folder anyway).
    pub refresh_dest: bool,
}

/// One durable, retryable non-send mutation, as read from `pending_actions`.
///
/// The intent is written there *before* the server is touched, so an offline or
/// crashed operation survives a restart and is retried by the worker's drain
/// loop instead of being lost. Scheduling metadata (`state`, `not_before`,
/// `created_at`, `last_error`) is owned by the cache and not carried here.
#[derive(Debug, Clone)]
pub struct PendingAction {
    pub id: i64,
    pub folder_path: String,
    pub uid: i64,
    pub kind: ActionKind,
    /// Kind-specific JSON (see [`SetFlagsPayload`] / [`MovePayload`]).
    pub payload: String,
    /// Attempts made so far, before the one about to run.
    pub attempts: i32,
}

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

/// Every header of a message, stored in the `message_headers` cache table and
/// cached on both FETCH paths: `ImapSession::summary_from_fetch` (from the
/// server's `ENVELOPE`) and `headers_from_raw_message` (from a full RFC822
/// message, which the body-fetch path already has in hand).
///
/// The two paths produce identical values for the same message, which is what
/// makes it safe for either to write the row.
///
/// The fields mirror RFC3501's `ENVELOPE` — date, subject, from, sender,
/// reply-to, to, cc, bcc, in-reply-to, message-id — plus `references`. That
/// last one is the reason the summary query still fetches any header bytes at
/// all: `ENVELOPE` has exactly ten fields and none of them is `References`, so
/// it can only come from `HEADER.FIELDS (REFERENCES IN-REPLY-TO)`.
#[derive(Debug, Clone, Default)]
pub struct MessageHeaders {
    pub subject: String,
    /// The six address headers, each a JSON array of `{"name":…,"email":…}`
    /// objects — e.g. `[{"name":"Alice","email":"alice@example.com"}]`.
    ///
    /// Structured rather than a display string so recipients stay
    /// addressable: a group entry has no addr-spec, and the UI wants the
    /// display name and the address separately. The UI renders one of these
    /// back to text on read (`addressDisplayString()` in `src/addressfield.h`).
    pub from: String,
    pub sender: String,
    pub reply_to: String,
    pub to: String,
    pub cc: String,
    pub bcc: String,
    /// The raw `Date:` header, verbatim.
    pub date: String,
    /// The `Date:` header parsed to a unix timestamp. This — not `date` — is
    /// what every recency sort uses; ordering by the header string sorts on
    /// the weekday name.
    pub date_ts: i64,
    pub in_reply_to: String,
    /// `References:`, normalized to bare Message-IDs. ENVELOPE has no such
    /// field, so this only ever comes from the `HEADER.FIELDS (REFERENCES
    /// IN-REPLY-TO)` item in `SUMMARY_FETCH_QUERY`. Without it threading has
    /// nothing to link, so that item is not optional.
    pub references: String,
    pub message_id: String,
}

impl MessageHeaders {
    /// True when this carries nothing worth writing to the cache, so callers
    /// can skip the row instead of storing an empty shell.
    ///
    /// The address fields are JSON arrays, so an absent header is the two-byte
    /// string `"[]"` rather than `""` — both count as empty here.
    pub fn is_empty(&self) -> bool {
        fn blank(v: &str) -> bool {
            v.is_empty() || v == "[]"
        }
        blank(&self.subject)
            && blank(&self.from)
            && blank(&self.date)
            && blank(&self.to)
            && self.message_id.is_empty()
    }
}

/// What the worker needs from a FETCHed message: its identity, its cached
/// *state* (flags, attachment flag, preview), and its headers.
///
/// Every header-derived field lives in `headers` rather than being flattened
/// onto this struct — `subject`/`date`/`message_id`/`references` used to be
/// duplicated here and in the `messages` table, and both copies are gone.
pub struct ImapMessageSummary {
    pub uid: i64,
    /// The `From` list, kept flattened because the daemon reads one entry from
    /// it to feed the correspondent index (`addresses` table). Same JSON
    /// shape as `headers.from`, so it is never a second format.
    pub from: String,
    pub is_read: bool,
    pub is_starred: bool,
    pub has_attachments: bool,
    pub preview: String,
    /// Every header, and the only place subject/date/threading fields live.
    pub headers: MessageHeaders,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailAttachment {
    pub path: String,
    pub filename: String,
}
