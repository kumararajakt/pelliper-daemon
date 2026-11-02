use super::*;

#[test]
fn normalize_message_id_strips_brackets_and_comments() {
    assert_eq!(normalize_message_id("<a@b.com>"), "a@b.com");
    assert_eq!(normalize_message_id("a@b.com"), "a@b.com");

    assert_eq!(normalize_message_id("<a@b.com> (Comment)"), "a@b.com");

    assert_eq!(normalize_message_id("(C) <a@b.com>"), "a@b.com");

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

    assert_eq!(
        normalize_references("<a@b.com> <c@d.com> (thread topic)"),
        "a@b.com c@d.com"
    );

    assert_eq!(normalize_references("a@b.com c@d.com"), "a@b.com c@d.com");

    assert_eq!(normalize_references("<a@b.com> <a@b.com>"), "a@b.com");
    assert_eq!(normalize_references(""), "");
}

#[test]
fn special_use_attribute_is_authoritative() {
    assert_eq!(
        classify_with_source("[Gmail]/All Mail", &[NameAttribute::All]),
        (FolderKind::Archive, true)
    );
    assert_eq!(
        classify_with_source("[Gmail]/Trash", &[NameAttribute::Trash]),
        (FolderKind::Trash, true)
    );

    assert_eq!(
        classify_with_source("Archive", &[NameAttribute::Sent]),
        (FolderKind::Sent, true)
    );
}

#[test]
fn name_heuristic_is_used_only_without_a_special_use_attribute() {
    assert_eq!(
        classify_with_source("Deleted", &[]),
        (FolderKind::Trash, false)
    );
    assert_eq!(
        classify_with_source("[Gmail]/Sent Mail", &[]),
        (FolderKind::Sent, false)
    );

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
    let mut folders = vec![
        folder("INBOX", FolderKind::Inbox),
        folder("[Gmail]/Trash", FolderKind::Trash),
        folder("Trash", FolderKind::Trash),
    ];
    let by_attribute = vec![false, true, false];
    demote_name_matched_impostors(&mut folders, &by_attribute);

    assert_eq!(folders[1].kind, FolderKind::Trash, "real trash survives");
    assert_eq!(folders[2].kind, FolderKind::Custom, "impostor demoted");
}

#[test]
fn name_heuristic_survives_when_no_authoritative_folder_exists() {
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

    assert_eq!(folders[4].kind, FolderKind::Starred);
    assert_eq!(folders[5].kind, FolderKind::Starred);
}

#[test]
fn threading_matches_after_normalization() {
    let child_message_id = normalize_message_id("<reply@host>");
    let parent_refs = normalize_references("reply@host");
    assert_eq!(child_message_id, parent_refs);
}

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
    let raw = b"Subject: Hi\r\nFrom: a@b.com";
    assert_eq!(split_header_block(raw), raw);
}

#[test]
fn split_header_block_does_not_stop_on_a_blank_line_inside_a_value() {
    let raw = b"Subject: one\r\n two\r\nFrom: a@b.com\r\n\r\nbody";
    assert_eq!(
        split_header_block(raw),
        b"Subject: one\r\n two\r\nFrom: a@b.com"
    );
}

#[test]
fn headers_from_raw_message_splits_every_address_header() {
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

    assert_eq!(h.subject, "Hello world");

    assert_eq!(h.message_id, "m1@example.com");
    assert_eq!(h.in_reply_to, "parent@example.com");

    assert_eq!(h.references, "root@example.com parent@example.com");
    assert_eq!(h.date, "Mon, 5 Jan 2026 10:00:00 +0000");
    assert!(h.date_ts > 0);

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

    assert_eq!(h.bcc, "[]");

    assert!(!h.is_empty());
}

#[test]
fn an_empty_fetch_produces_an_empty_headers_row() {
    assert!(MessageHeaders::default().is_empty());
    assert!(headers_from_raw_message(b"").is_empty());

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

    let bare = A {
        name: None,
        adl: None,
        mailbox: Some(Cow::Borrowed(&b"carol"[..])),
        host: Some(Cow::Borrowed(&b"example.com"[..])),
    };

    let group = A {
        name: None,
        adl: None,
        mailbox: Some(Cow::Borrowed(&b"undisclosed"[..])),
        host: None,
    };

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

    assert_eq!(
        parse_one_address("carol@example.com"),
        Some(("carol@example.com".into(), String::new()))
    );

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
    assert_eq!(envelope_scalar(None), None);
    assert_eq!(envelope_scalar(Some(b"")), None);
    assert_eq!(
        envelope_scalar(Some(&b"Mon, 5 Jan 2026 10:00:00 +0000"[..])),
        Some("Mon, 5 Jan 2026 10:00:00 +0000".to_string())
    );
}

#[test]
fn trusted_highest_uid_requires_uidnext_consistent_with_exists() {
    assert_eq!(trusted_highest_uid(5, Some(101)), Some(100));

    assert_eq!(trusted_highest_uid(0, Some(1)), Some(0));

    assert_eq!(trusted_highest_uid(5, None), None);
    assert_eq!(trusted_highest_uid(5, Some(0)), None);

    assert_eq!(trusted_highest_uid(50, Some(10)), None);
    assert_eq!(trusted_highest_uid(2, Some(2)), None);
}

#[test]
fn filter_uids_after_keeps_only_strictly_newer_uids_sorted() {
    assert_eq!(filter_uids_after(vec![9, 7, 100, 9, 5], 7), vec![9, 100]);
    assert_eq!(filter_uids_after(vec![5, 6, 7], 7), Vec::<i64>::new());

    assert_eq!(filter_uids_after(vec![1000, 3], 4_000), Vec::<i64>::new());
    assert_eq!(filter_uids_after(vec![], 0), Vec::<i64>::new());
}
