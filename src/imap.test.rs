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

// ---------------------------------------------------------------------------
// message_headers: header block splitting and extraction
// ---------------------------------------------------------------------------

#[test]
fn split_header_block_stops_at_the_blank_line() {
    let raw = b"Subject: Hi\r\nFrom: a@b.com\r\n\r\nbody text\r\nmore body";
    assert_eq!(split_header_block(raw), b"Subject: Hi\r\nFrom: a@b.com");
}

#[test]
fn split_header_block_handles_bare_lf() {
    let raw = b"Subject: Hi\nFrom: a@b.com\n\nbody";
    assert_eq!(split_header_block(raw), b"Subject: Hi\nFrom: a@b.com");
}

#[test]
fn split_header_block_survives_a_missing_separator() {
    // No blank line: treat the whole thing as headers rather than losing it.
    let raw = b"Subject: Hi\r\nFrom: a@b.com";
    assert_eq!(split_header_block(raw), raw);
}

#[test]
fn split_header_block_does_not_stop_on_a_blank_line_inside_a_value() {
    // A folded header continues with leading whitespace, never an empty line,
    // so the first truly empty line is the only separator.
    let raw = b"Subject: one\r\n two\r\nFrom: a@b.com\r\n\r\nbody";
    assert_eq!(
        split_header_block(raw),
        b"Subject: one\r\n two\r\nFrom: a@b.com"
    );
}

#[test]
fn headers_from_raw_message_splits_every_address_header() {
    // The body-fetch path has the whole message, so it reads every header
    // directly. It must produce exactly the shape the ENVELOPE path produces
    // for the same message, because either path can be the one to write the
    // row.
    let raw = concat!(
        "Return-Path: <bounce@mx.example.com>\r\n",
        "Received: from mx.example.com (mx.example.com [10.0.0.1])\r\n",
        "\tby mail.example.net with ESMTP id ABC123\r\n",
        "Authentication-Results: mx.example.com; dkim=pass header.i=@example.com\r\n",
        "List-Id: Dev List <dev.example.com>\r\n",
        "X-Original-Message-Id: <raw-1@example.com>\r\n",
        "Message-ID: <m1@example.com>\r\n",
        "In-Reply-To: <parent@example.com>\r\n",
        "References: <root@example.com> <parent@example.com>\r\n",
        "From: =?utf-8?B?w4FsaWNl?= <alice@example.com>\r\n",
        "Sender: alice@example.com\r\n",
        "Reply-To: Alice <alice@work.example.com>\r\n",
        "To: Bob <bob@example.com>, carol@example.com\r\n",
        "Cc: dave@example.com\r\n",
        "Subject: =?utf-8?B?SGVsbG8gd29ybGQ=?=\r\n",
        "Date: Mon, 5 Jan 2026 10:00:00 +0000\r\n",
        "MIME-Version: 1.0\r\n",
        "\r\n",
        "body goes here\r\n",
    )
    .as_bytes();

    let h = headers_from_raw_message(raw);

    // RFC2047 decoded, folded values joined. The name is non-ASCII on purpose:
    // =?utf-8?B?w4FsaWNl?= is base64 for "Álice".
    assert_eq!(h.subject, "Hello world");
    // Normalized on both write paths (bracket-stripped), because threadmodel's
    // union-find compares these strings verbatim.
    assert_eq!(h.message_id, "m1@example.com");
    assert_eq!(h.in_reply_to, "parent@example.com");
    // References is normalized to bare Message-IDs space-joined, same as the
    // FETCH path, so threading compares equal strings either way.
    assert_eq!(h.references, "root@example.com parent@example.com");
    assert_eq!(h.date, "Mon, 5 Jan 2026 10:00:00 +0000");
    assert!(h.date_ts > 0);

    // Addresses come out as JSON arrays of {name, email}, split per address.
    assert_eq!(h.from, r#"[{"name":"Álice","email":"alice@example.com"}]"#);
    assert_eq!(h.sender, r#"[{"name":"","email":"alice@example.com"}]"#);
    assert_eq!(
        h.reply_to,
        r#"[{"name":"Alice","email":"alice@work.example.com"}]"#
    );
    assert_eq!(
        h.to,
        r#"[{"name":"Bob","email":"bob@example.com"},{"name":"","email":"carol@example.com"}]"#
    );
    assert_eq!(h.cc, r#"[{"name":"","email":"dave@example.com"}]"#);
    // BCC is stripped by the sending MTA, so it is legitimately absent — and an
    // absent list is the empty array, not an empty string.
    assert_eq!(h.bcc, "[]");

    assert!(!h.is_empty());
}

#[test]
fn an_empty_fetch_produces_an_empty_headers_row() {
    // save_message_headers skips these rather than storing a shell, so a
    // server that returns neither ENVELOPE nor a header block costs nothing.
    assert!(MessageHeaders::default().is_empty());
    assert!(headers_from_raw_message(b"").is_empty());
    // A message whose only headers are address lists that came back empty
    // must still count as empty rather than storing `"[]"` everywhere.
    assert!(MessageHeaders {
        from: "[]".into(),
        to: "[]".into(),
        ..Default::default()
    }
    .is_empty());
}

#[test]
fn envelope_addresses_produce_json_name_and_email() {
    use async_imap::imap_proto::types::Address as A;
    use std::borrow::Cow;

    let named = A {
        name: Some(Cow::Borrowed(&b"Alice"[..])),
        adl: None,
        mailbox: Some(Cow::Borrowed(&b"alice"[..])),
        host: Some(Cow::Borrowed(&b"example.com"[..])),
    };

    // No personal name: the email is still addressable on its own.
    let bare = A {
        name: None,
        adl: None,
        mailbox: Some(Cow::Borrowed(&b"carol"[..])),
        host: Some(Cow::Borrowed(&b"example.com"[..])),
    };

    // Group syntax: mailbox holds the group name, host is NIL. A display
    // string would lose this; the JSON keeps the name with an empty address.
    let group = A {
        name: None,
        adl: None,
        mailbox: Some(Cow::Borrowed(&b"undisclosed"[..])),
        host: None,
    };

    // Nothing usable at all is dropped, not stored as an empty entry.
    let empty = A {
        name: None,
        adl: None,
        mailbox: None,
        host: None,
    };

    let named_one = [named];
    assert_eq!(
        envelope_addresses(Some(&named_one)),
        r#"[{"name":"Alice","email":"alice@example.com"}]"#
    );
    let named_and_bare = [
        A {
            name: Some(Cow::Borrowed(&b"Alice"[..])),
            adl: None,
            mailbox: Some(Cow::Borrowed(&b"alice"[..])),
            host: Some(Cow::Borrowed(&b"example.com"[..])),
        },
        bare,
    ];
    assert_eq!(
        envelope_addresses(Some(&named_and_bare)),
        r#"[{"name":"Alice","email":"alice@example.com"},{"name":"","email":"carol@example.com"}]"#
    );
    assert_eq!(
        envelope_addresses(Some(&[group])),
        r#"[{"name":"undisclosed","email":""}]"#
    );
    assert_eq!(envelope_addresses(Some(&[empty])), "[]");
    assert_eq!(envelope_addresses(Some(&[])), "[]");
    assert_eq!(envelope_addresses(None), "[]");
}

#[test]
fn address_lists_split_on_top_level_commas_only() {
    // A quoted display name or an angle-addr keeps its comma to itself.
    assert_eq!(
        split_address_list(r#""Doe, Jane" <j@x.com>, bob@x.com"#),
        vec![
            "\"Doe, Jane\" <j@x.com>".to_string(),
            "bob@x.com".to_string()
        ]
    );
    assert_eq!(
        split_address_list("a@x.com, b@x.com, c@x.com"),
        vec!["a@x.com", "b@x.com", "c@x.com"]
    );
    assert!(split_address_list("").is_empty());
}

#[test]
fn parse_one_address_splits_name_from_addr_spec() {
    assert_eq!(
        parse_one_address("Alice <alice@example.com>"),
        Some(("alice@example.com".into(), "Alice".into()))
    );
    // Bare addr-spec: no display name.
    assert_eq!(
        parse_one_address("carol@example.com"),
        Some(("carol@example.com".into(), String::new()))
    );
    // Group entry: RFC3501 spells these `label:;` — a name, no addr-spec,
    // matching how `envelope_address` treats a NIL host.
    assert_eq!(
        parse_one_address("undisclosed-recipients:;"),
        Some((String::new(), "undisclosed-recipients".into()))
    );
    assert_eq!(parse_one_address("  "), None);
}

#[test]
fn first_address_yields_the_correspondent_for_the_index() {
    assert_eq!(
        first_address(
            r#"[{"name":"Alice","email":"alice@example.com"},{"name":"B","email":"b@x"}]"#
        ),
        Some(("alice@example.com".to_string(), "Alice".to_string()))
    );
    // Bare address, no name — still indexable.
    assert_eq!(
        first_address(r#"[{"name":"","email":"carol@example.com"}]"#),
        Some(("carol@example.com".to_string(), String::new()))
    );
    assert_eq!(first_address("[]"), None);
    assert_eq!(first_address(""), None);
    assert_eq!(first_address("not json"), None);
}

#[test]
fn envelope_scalar_treats_nil_as_absent() {
    // NIL and empty both mean "server gave us nothing" so the caller falls
    // back to the raw header block.
    assert_eq!(envelope_scalar(None), None);
    assert_eq!(envelope_scalar(Some(b"")), None);
    assert_eq!(
        envelope_scalar(Some(&b"Mon, 5 Jan 2026 10:00:00 +0000"[..])),
        Some("Mon, 5 Jan 2026 10:00:00 +0000".to_string())
    );
}

// ---------------------------------------------------------------------------
// Sync cursors: UIDNEXT consistency + incremental UID filtering
// ---------------------------------------------------------------------------

#[test]
fn trusted_highest_uid_requires_uidnext_consistent_with_exists() {
    // UIDNEXT 101 with 5 live messages: 100 is the highest assigned UID.
    assert_eq!(trusted_highest_uid(5, Some(101)), Some(100));
    // UIDNEXT exactly one above an empty mailbox.
    assert_eq!(trusted_highest_uid(0, Some(1)), Some(0));
    // Missing or zero UIDNEXT (IMAP4rev1 allows omitting it) is untrusted.
    assert_eq!(trusted_highest_uid(5, None), None);
    assert_eq!(trusted_highest_uid(5, Some(0)), None);
    // Inconsistent: UIDNEXT-1 below the live count cannot be a valid high
    // watermark (Flectar's rule); fall back to SEARCH instead.
    assert_eq!(trusted_highest_uid(50, Some(10)), None);
    assert_eq!(trusted_highest_uid(2, Some(2)), None);
}

#[test]
fn filter_uids_after_keeps_only_strictly_newer_uids_sorted() {
    // Unsorted + duplicates come back sorted, deduped, strictly above.
    assert_eq!(filter_uids_after(vec![9, 7, 100, 9, 5], 7), vec![9, 100]);
    assert_eq!(filter_uids_after(vec![5, 6, 7], 7), Vec::<i64>::new());
    // The RFC 3501 range-reversal quirk: when `last_seen` is above the
    // mailbox's highest UID, the server can hand back that older highest UID
    // even though it is not new.
    assert_eq!(filter_uids_after(vec![1000, 3], 4_000), Vec::<i64>::new());
    assert_eq!(filter_uids_after(vec![], 0), Vec::<i64>::new());
}
