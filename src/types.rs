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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailAttachment {
    pub path: String,
    pub filename: String,
}
