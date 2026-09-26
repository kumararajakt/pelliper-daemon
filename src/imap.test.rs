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

// ---------------------------------------------------------------------------
// Folder classification
// ---------------------------------------------------------------------------

#[test]
fn special_use_attribute_is_authoritative() {
    // Gmail marks All Mail with \All rather than \Archive; both mean Archive.
    assert_eq!(
        classify_with_source("[Gmail]/All Mail", &[NameAttribute::All]),
        (FolderKind::Archive, true)
    );
    assert_eq!(
        classify_with_source("[Gmail]/Trash", &[NameAttribute::Trash]),
        (FolderKind::Trash, true)
    );
    // An attribute beats a contradicting name: a folder called "Archive" that
    // the server flags \Sent is Sent.
    assert_eq!(
        classify_with_source("Archive", &[NameAttribute::Sent]),
        (FolderKind::Sent, true)
    );
}

#[test]
fn name_heuristic_is_used_only_without_a_special_use_attribute() {
    // Servers with no RFC 6154 support send no attributes at all.
    assert_eq!(
        classify_with_source("Deleted", &[]),
        (FolderKind::Trash, false)
    );
    assert_eq!(
        classify_with_source("[Gmail]/Sent Mail", &[]),
        (FolderKind::Sent, false)
    );
    // Irrelevant attributes (\Noselect, \Noinferiors, \Marked) must not suppress
    // the heuristic — only a real SPECIAL-USE flag makes the kind authoritative.
    assert_eq!(
        classify_with_source(
            "Trash",
            &[
                NameAttribute::NoSelect,
                NameAttribute::NoInferiors,
                NameAttribute::Marked
            ]
        ),
        (FolderKind::Trash, false)
    );
    // A name matching nothing stays Custom.
    assert_eq!(
        classify_with_source("Mailspring/Snoozed", &[]),
        (FolderKind::Custom, false)
    );
}

fn folder(path: &str, kind: FolderKind) -> ImapFolder {
    ImapFolder {
        path: path.to_string(),
        noselect: false,
        kind,
    }
}

#[test]
fn name_matched_impostor_is_demoted_when_server_has_the_real_folder() {
    // The live case from account 1: Gmail's real [Gmail]/Trash next to a
    // user-created top-level "Trash" label.
    let mut folders = vec![
        folder("INBOX", FolderKind::Inbox),
        folder("[Gmail]/Trash", FolderKind::Trash),
        folder("Trash", FolderKind::Trash), // name-matched impostor
    ];
    let by_attribute = vec![false, true, false];
    demote_name_matched_impostors(&mut folders, &by_attribute);

    assert_eq!(folders[1].kind, FolderKind::Trash, "real trash survives");
    assert_eq!(folders[2].kind, FolderKind::Custom, "impostor demoted");
}

#[test]
fn name_heuristic_survives_when_no_authoritative_folder_exists() {
    // Outlook/Exchange-style server: \Trash absent, so the name match is the
    // only signal available and must not be demoted away.
    let mut folders = vec![
        folder("INBOX", FolderKind::Inbox),
        folder("Deleted Items", FolderKind::Trash),
    ];
    let by_attribute = vec![false, false];
    demote_name_matched_impostors(&mut folders, &by_attribute);

    assert_eq!(folders[1].kind, FolderKind::Trash);
}

#[test]
fn demotion_clears_every_duplicated_role() {
    // Account 1's real listing: Archive and Trash each appear twice.
    let mut folders = vec![
        folder("Archive", FolderKind::Archive),
        folder("[Gmail]/All Mail", FolderKind::Archive),
        folder("Trash", FolderKind::Trash),
        folder("[Gmail]/Trash", FolderKind::Trash),
        folder("[Gmail]/Starred", FolderKind::Starred),
        folder("[Gmail]/Important", FolderKind::Starred),
    ];
    let by_attribute = vec![false, true, false, true, true, true];
    demote_name_matched_impostors(&mut folders, &by_attribute);

    assert_eq!(folders[0].kind, FolderKind::Custom);
    assert_eq!(folders[1].kind, FolderKind::Archive);
    assert_eq!(folders[2].kind, FolderKind::Custom);
    assert_eq!(folders[3].kind, FolderKind::Trash);
    // Both \Flagged folders are authoritative, so this pass cannot separate
    // them — Gmail marks both Starred and Important \Flagged. Known residual
    // duplicate, asserted so a future change is noticed rather than silent.
    assert_eq!(folders[4].kind, FolderKind::Starred);
    assert_eq!(folders[5].kind, FolderKind::Starred);
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
