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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SearchResult {
    pub account_id: i64,
    pub folder_path: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    SetFlags,
    Move,
    Delete,
    CreateFolder,
    RenameFolder,
    DeleteFolder,
}

impl ActionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionKind::SetFlags => "set_flags",
            ActionKind::Move => "move",
            ActionKind::Delete => "delete",
            ActionKind::CreateFolder => "create_folder",
            ActionKind::RenameFolder => "rename_folder",
            ActionKind::DeleteFolder => "delete_folder",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "set_flags" => Some(ActionKind::SetFlags),
            "move" => Some(ActionKind::Move),
            "delete" => Some(ActionKind::Delete),
            "create_folder" => Some(ActionKind::CreateFolder),
            "rename_folder" => Some(ActionKind::RenameFolder),
            "delete_folder" => Some(ActionKind::DeleteFolder),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SetFlagsPayload {
    pub is_read: Option<bool>,
    pub is_starred: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MovePayload {
    pub dest: String,

    pub refresh_dest: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RenameFolderPayload {
    pub new_name: String,
}

#[derive(Debug, Clone)]
pub struct PendingAction {
    pub id: i64,
    pub folder_path: String,
    pub uid: i64,
    pub kind: ActionKind,

    pub payload: String,

    pub attempts: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStage {
    Header,

    Body,
}

impl SyncStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncStage::Header => "header",
            SyncStage::Body => "body",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "header" => Some(SyncStage::Header),
            "body" => Some(SyncStage::Body),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncFailure {
    pub folder_path: String,
    pub uid: i64,
    pub stage: SyncStage,

    pub attempts: i32,
}

pub struct ImapFolder {
    pub path: String,
    pub noselect: bool,
    pub kind: FolderKind,
}

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

#[derive(Debug, Clone, Default)]
pub struct MessageHeaders {
    pub subject: String,

    pub from: String,
    pub sender: String,
    pub reply_to: String,
    pub to: String,
    pub cc: String,
    pub bcc: String,

    pub date: String,

    pub date_ts: i64,
    pub in_reply_to: String,

    pub references: String,
    pub message_id: String,
}

impl MessageHeaders {
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

pub struct ImapMessageSummary {
    pub uid: i64,

    pub from: String,
    pub is_read: bool,
    pub is_starred: bool,
    pub has_attachments: bool,
    pub preview: String,

    pub headers: MessageHeaders,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmailAttachment {
    pub path: String,
    pub filename: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonSignal {
    NewMailReceived {
        account_id: i64,
        folder_path: String,
        count: i32,
    },
    FolderUnreadChanged {
        account_id: i64,
        folder_path: String,
        unread_count: i32,
    },
    MessageFlagsChanged {
        account_id: i64,
        folder_path: String,
        uid: i64,
        is_read: bool,
        is_starred: bool,
    },
    OutboxItemSent {
        account_id: i64,
        item_id: i64,
    },
}

pub type SignalSender = tokio::sync::mpsc::UnboundedSender<DaemonSignal>;
