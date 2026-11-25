use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FolderRole {
    Inbox,
    Sent,
    Drafts,
    Important,
    Custom,
    Archive,
    Spam,
    Trash,
}

impl FolderRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Sent => "sent",
            Self::Drafts => "drafts",
            Self::Important => "important",
            Self::Custom => "custom",
            Self::Archive => "archive",
            Self::Spam => "spam",
            Self::Trash => "trash",
        }
    }

    pub fn priority(&self) -> i32 {
        match self {
            Self::Inbox => 0,
            Self::Sent | Self::Drafts | Self::Important => 10,
            Self::Custom => 20,
            Self::Archive => 30,
            Self::Spam | Self::Trash => 40,
        }
    }
}

/// Classifies an IMAP folder using RFC 6154 `SPECIAL-USE` attributes first, falling back to name matching.
pub fn classify_folder(name: &str, special_use_attrs: &[String]) -> (FolderRole, i32) {
    for attr in special_use_attrs {
        let lower = attr.to_ascii_lowercase();
        let role = match lower.as_str() {
            "\\inbox" => Some(FolderRole::Inbox),
            "\\sent" => Some(FolderRole::Sent),
            "\\drafts" => Some(FolderRole::Drafts),
            "\\important" | "\\flagged" => Some(FolderRole::Important),
            "\\archive" | "\\all" => Some(FolderRole::Archive),
            "\\junk" => Some(FolderRole::Spam),
            "\\trash" => Some(FolderRole::Trash),
            _ => None,
        };
        if let Some(r) = role {
            return (r, r.priority());
        }
    }

    let leaf = name
        .rsplit(['/', '.'])
        .next()
        .unwrap_or(name)
        .trim()
        .to_ascii_lowercase();

    let role = match leaf.as_str() {
        "inbox" => FolderRole::Inbox,
        "sent" | "sent items" | "sent mail" | "sent messages" => FolderRole::Sent,
        "draft" | "drafts" => FolderRole::Drafts,
        "important" | "starred" | "flagged" => FolderRole::Important,
        "archive" | "all mail" => FolderRole::Archive,
        "spam" | "junk" | "junk e-mail" | "bulk mail" => FolderRole::Spam,
        "trash" | "deleted" | "deleted items" | "deleted messages" | "bin" => FolderRole::Trash,
        _ => FolderRole::Custom,
    };
    (role, role.priority())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn special_use_overrides_localized_names() {
        let (role, prio) = classify_folder("[Gmail]/Gesendet", &["\\Sent".to_string()]);
        assert_eq!(role, FolderRole::Sent);
        assert_eq!(prio, 10);
    }

    #[test]
    fn priority_orders_inbox_before_custom_before_trash() {
        let (_, p_inbox) = classify_folder("INBOX", &[]);
        let (_, p_sent) = classify_folder("Sent", &[]);
        let (_, p_work) = classify_folder("Work/Receipts", &[]);
        let (_, p_archive) = classify_folder("Archive", &[]);
        let (_, p_trash) = classify_folder("Trash", &[]);

        assert!(p_inbox < p_sent);
        assert!(p_sent < p_work);
        assert!(p_work < p_archive);
        assert!(p_archive < p_trash);
    }
}
