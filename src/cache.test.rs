use super::*;
use crate::types::{ActionKind, MessageHeaders, SyncStage};
use std::sync::atomic::{AtomicU32, Ordering};

fn scratch() -> (Cache, PathBuf) {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pelliper-cache-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let cache = Cache::open(&dir).expect("open");
    cache.migrate().expect("migrate");
    cache.fts_init().expect("fts_init");
    (cache, dir)
}

fn add_account(cache: &Cache) {
    cache
        .conn
        .execute(
            "INSERT INTO accounts (id, email) VALUES (1, 'a@example.com')",
            [],
        )
        .expect("account");
}

fn add_message(cache: &Cache, uid: i64) {
    cache
        .upsert_message(1, "INBOX", uid, false, false, false, "")
        .expect("upsert_message");
}

fn sample_headers() -> MessageHeaders {
    MessageHeaders {
        subject: "hi".to_string(),
        from: r#"[{"name":"Alice","email":"alice@example.com"}]"#.to_string(),
        sender: "[]".to_string(),
        reply_to: r#"[{"name":"Alice","email":"alice@work.example.com"}]"#.to_string(),
        to: r#"[{"name":"","email":"bob@example.com"}]"#.to_string(),
        cc: "[]".to_string(),
        bcc: "[]".to_string(),
        date: "Mon, 5 Jan 2026 10:00:00 +0000".to_string(),
        date_ts: 1_767_602_400,
        in_reply_to: String::new(),
        references: String::new(),
        message_id: "<m1@example.com>".to_string(),
    }
}

#[test]
fn migrate_creates_message_headers() {
    let (cache, _dir) = scratch();
    let found: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_headers'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(found, 1, "message_headers must exist after migrate");
}

#[test]
fn headers_round_trip_with_the_address_lists_still_json() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let h = sample_headers();
    cache.save_message_headers(1, "INBOX", 7, &h).expect("save");

    let (subject, from, reply_to, to, cc, date, message_id): (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ) = cache
        .conn
        .query_row(
            "SELECT subject, from_addr, reply_to_addr, to_addr, cc_addr, date, message_id
             FROM message_headers WHERE account_id=1 AND folder_path='INBOX' AND uid=7",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            },
        )
        .expect("load");

    assert_eq!(subject, "hi");

    assert_eq!(from, r#"[{"name":"Alice","email":"alice@example.com"}]"#);
    assert_eq!(
        reply_to,
        r#"[{"name":"Alice","email":"alice@work.example.com"}]"#
    );
    assert_eq!(to, r#"[{"name":"","email":"bob@example.com"}]"#);
    assert_eq!(cc, "[]");

    assert_eq!(date, "Mon, 5 Jan 2026 10:00:00 +0000");
    assert_eq!(message_id, "<m1@example.com>");

    let nulls: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM message_headers
             WHERE from_addr IS NULL OR to_addr IS NULL OR cc_addr IS NULL",
            [],
            |r| r.get(0),
        )
        .expect("count");
    assert_eq!(nulls, 0);
}

#[test]
fn the_address_json_stays_queryable() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("save");

    let hit: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM message_headers
             WHERE from_addr LIKE '%alice@example.com%'",
            [],
            |r| r.get(0),
        )
        .expect("substr");
    assert_eq!(hit, 1, "a substring match must reach inside the JSON");

    let (name, email): (String, String) = cache
        .conn
        .query_row(
            "SELECT json_extract(from_addr, '$[0].name'),
                    json_extract(from_addr, '$[0].email')
             FROM message_headers WHERE uid=7",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("json_extract");
    assert_eq!(name, "Alice");
    assert_eq!(email, "alice@example.com");
}

#[test]
fn resaving_headers_updates_in_place() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let mut h = sample_headers();
    cache
        .save_message_headers(1, "INBOX", 7, &h)
        .expect("first");
    h.subject = "edited".to_string();
    h.cc = r#"[{"name":"","email":"carol@example.com"}]"#.to_string();
    cache
        .save_message_headers(1, "INBOX", 7, &h)
        .expect("second");

    let (subject, cc): (String, String) = cache
        .conn
        .query_row(
            "SELECT subject, cc_addr FROM message_headers WHERE uid=7",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("load");
    assert_eq!(subject, "edited");
    assert_eq!(cc, r#"[{"name":"","email":"carol@example.com"}]"#);

    let rows: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM message_headers", [], |r| r.get(0))
        .expect("count");
    assert_eq!(rows, 1, "upsert must not duplicate the row");
}

#[test]
fn a_header_row_cannot_be_written_before_its_message() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let err = cache.save_message_headers(1, "INBOX", 99, &sample_headers());
    assert!(
        err.is_err(),
        "orphan header row must be rejected by the foreign key"
    );
}

#[test]
fn deleting_a_message_cascades_to_its_headers() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("save");

    cache.delete_message(1, "INBOX", 7).expect("delete_message");

    let rows: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM message_headers", [], |r| r.get(0))
        .expect("count");
    assert_eq!(rows, 0, "header rows must cascade with the message");
}

#[test]
fn empty_headers_are_not_stored() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    cache
        .save_message_headers(1, "INBOX", 7, &MessageHeaders::default())
        .expect("save is a no-op");

    let rows: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM message_headers", [], |r| r.get(0))
        .expect("count");
    assert_eq!(rows, 0);
}

fn fts_hits(cache: &Cache, term: &str) -> i64 {
    cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH ?1",
            [term],
            |r| r.get(0),
        )
        .unwrap_or(-1)
}

#[test]
fn headers_are_searchable_even_though_they_no_longer_live_in_messages() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("save");

    assert_eq!(fts_hits(&cache, "hi"), 1, "subject must be indexed");
    assert_eq!(fts_hits(&cache, "alice"), 1, "sender must be indexed");

    assert_eq!(
        fts_hits(&cache, "\"m1@example.com\""),
        1,
        "message-id must be indexed"
    );
}

#[test]
fn saving_a_body_indexes_it_instead_of_rolling_the_insert_back() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");

    cache
        .save_body(1, "INBOX", 7, "<p>quarterly snow forecast</p>")
        .expect("save_body must succeed");

    let stored: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM bodies", [], |r| r.get(0))
        .expect("count");
    assert_eq!(stored, 1, "the body row must survive indexing");

    assert_eq!(
        fts_hits(&cache, "snow"),
        1,
        "body text must be searchable — the whole point of indexing"
    );
    assert_eq!(
        fts_hits(&cache, "hi"),
        1,
        "indexing a body must not wipe the header columns"
    );
}

#[test]
fn re_saving_a_body_reindexes_it_rather_than_duplicating() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>original wording</p>")
        .expect("first");

    cache
        .save_body(1, "INBOX", 7, "<p>replacement wording</p>")
        .expect("second");

    assert_eq!(fts_hits(&cache, "original"), 0, "stale tokens must be gone");
    assert_eq!(fts_hits(&cache, "replacement"), 1);
}

#[test]
fn deleting_a_message_removes_its_index_entry() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>temporary body</p>")
        .expect("body");

    cache.delete_message(1, "INBOX", 7).expect("delete_message");

    assert_eq!(
        fts_hits(&cache, "temporary"),
        0,
        "body must leave the index"
    );
    assert_eq!(fts_hits(&cache, "hi"), 0, "subject must leave the index");
}

#[test]
fn flag_updates_do_not_wipe_the_search_index() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");

    cache
        .update_message_flags(1, "INBOX", 7, Some(true), None)
        .expect("mark read");
    cache
        .update_message_flags(1, "INBOX", 7, None, Some(true))
        .expect("star");

    assert_eq!(
        fts_hits(&cache, "hi"),
        1,
        "subject must survive a flag flip"
    );
    assert_eq!(
        fts_hits(&cache, "alice"),
        1,
        "sender must survive a flag flip"
    );
}

#[test]
fn recency_sorts_on_date_ts_not_the_header_string() {
    let (cache, _dir) = scratch();
    add_account(&cache);

    for (uid, raw, ts) in [
        (1_i64, "Wed, 8 Jul 2026 09:30:09 +0000", 1_753_666_209_i64),
        (2_i64, "Wed, 9 Sep 2026 14:04:02 +0000", 1_788_922_642_i64),
    ] {
        cache
            .upsert_message(1, "INBOX", uid, false, false, false, "")
            .expect("message");
        let mut h = sample_headers();
        h.date = raw.to_string();
        h.date_ts = ts;
        cache
            .save_message_headers(1, "INBOX", uid, &h)
            .expect("headers");
    }

    let order: Vec<i64> = cache
        .load_messages(1, "INBOX", 0, 10)
        .expect("load")
        .into_iter()
        .map(|m| m.uid)
        .collect();
    assert_eq!(
        order,
        vec![2, 1],
        "newest first by timestamp, not by the weekday in the header"
    );
}

#[test]
fn a_message_without_headers_is_still_listed() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let rows = cache.load_messages(1, "INBOX", 0, 10).expect("load");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uid, 7);
    assert_eq!(rows[0].subject, None, "no header row means no subject");
}

#[test]
fn load_messages_ships_the_threading_fields_the_ui_threads_on() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let mut h = sample_headers();
    h.in_reply_to = "<root@example.com>".to_string();
    h.references = "<root@example.com> <mid@example.com>".to_string();
    cache
        .save_message_headers(1, "INBOX", 7, &h)
        .expect("headers");

    let rows = cache.load_messages(1, "INBOX", 0, 10).expect("load");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].message_id.as_deref(), Some("<m1@example.com>"));
    assert_eq!(rows[0].in_reply_to.as_deref(), Some("<root@example.com>"));
    assert_eq!(
        rows[0].references.as_deref(),
        Some("<root@example.com> <mid@example.com>")
    );

    let wire = serde_json::to_string(&rows).expect("serialize");
    for key in ["in_reply_to", "message_id", "references"] {
        assert!(
            wire.contains(&format!("\"{key}\"")),
            "payload lost the `{key}` threading field: {wire}"
        );
    }
}

#[test]
fn move_message_carries_the_headers_across() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>body text</p>")
        .expect("body");

    cache.move_message(1, "INBOX", 7, "Archive").expect("move");

    let src: i64 = cache
        .conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM messages WHERE folder_path='INBOX')
                  + (SELECT COUNT(*) FROM message_headers WHERE folder_path='INBOX')
                  + (SELECT COUNT(*) FROM bodies WHERE folder_path='INBOX')",
            [],
            |r| r.get(0),
        )
        .expect("count source");
    assert_eq!(src, 0, "source rows must be removed");

    let (subject, message_id, refs, date_ts): (String, String, String, i64) = cache
        .conn
        .query_row(
            "SELECT subject, message_id, references_, date_ts FROM message_headers
             WHERE folder_path='Archive'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("load moved headers");
    assert_eq!(subject, "hi");
    assert_eq!(message_id, "<m1@example.com>");
    assert_eq!(refs, "");
    assert_eq!(date_ts, 1_767_602_400, "sort key must survive a move");
    assert_eq!(fts_hits(&cache, "hi"), 1, "indexed at the new location");
    assert_eq!(
        fts_hits(&cache, "body"),
        1,
        "body indexed at the new location"
    );
}

#[test]
fn folder_uidvalidity_defaults_to_zero_and_round_trips() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("upsert_folder");

    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 0);
    cache
        .set_folder_uidvalidity(1, "INBOX", 4_200)
        .expect("set");
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 4_200);

    assert_eq!(cache.folder_uidvalidity(1, "Nope").expect("read"), 0);
}

#[test]
fn migrate_adds_uidvalidity_to_a_pre_existing_folders_table() {
    let (cache, _dir) = scratch();
    cache
        .conn
        .execute_batch(
            "DROP TABLE folders;
             CREATE TABLE folders (
                 account_id INTEGER NOT NULL,
                 path TEXT NOT NULL,
                 unread_count INTEGER DEFAULT 0,
                 noselect INTEGER DEFAULT 0,
                 kind TEXT NOT NULL DEFAULT 'custom',
                 PRIMARY KEY (account_id, path),
                 FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
             );",
        )
        .expect("legacy folders table");

    cache.migrate().expect("re-run migrate");
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");

    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 0);
    cache.set_folder_uidvalidity(1, "INBOX", 99).expect("set");
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 99);
}

#[test]
fn rediscovering_a_folder_preserves_its_uidvalidity() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("upsert_folder");
    cache.set_folder_uidvalidity(1, "INBOX", 7).expect("set");
    cache
        .upsert_folder(1, "INBOX", 5, false, "inbox")
        .expect("re-upsert");
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 7);
}

#[test]
fn clearing_a_folder_after_a_uidvalidity_change_drops_only_that_folder() {
    let (cache, _dir) = scratch();
    add_account(&cache);

    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>quarterly snow forecast</p>")
        .expect("body");

    cache
        .upsert_message(1, "Archive", 7, false, false, false, "")
        .expect("archive message");
    let mut archived = sample_headers();
    archived.subject = "archived".to_string();
    cache
        .save_message_headers(1, "Archive", 7, &archived)
        .expect("archive headers");

    assert_eq!(fts_hits(&cache, "hi"), 1);
    assert_eq!(fts_hits(&cache, "quarterly"), 1);

    let removed = cache.clear_folder_messages(1, "INBOX").expect("clear");
    assert_eq!(removed, 1, "one message removed");

    assert_eq!(cache.count_messages(1, "INBOX").expect("count"), 0);
    assert_eq!(
        fts_hits(&cache, "hi"),
        0,
        "the cleared folder's subject left the index"
    );
    assert_eq!(
        fts_hits(&cache, "quarterly"),
        0,
        "the cleared folder's body left the index"
    );

    assert_eq!(cache.count_messages(1, "Archive").expect("count"), 1);
    assert_eq!(fts_hits(&cache, "archived"), 1);
}

#[test]
fn moving_a_message_rekeys_its_index_row_to_the_destination() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");

    cache.move_message(1, "INBOX", 7, "Archive").expect("move");

    let joined: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages_fts f JOIN messages m ON m.rowid = f.rowid
             WHERE m.account_id = 1 AND m.folder_path = 'Archive' AND m.uid = 7
               AND messages_fts MATCH 'hi'",
            [],
            |r| r.get(0),
        )
        .expect("join");
    assert_eq!(joined, 1, "the hit must resolve to the destination row");
    assert_eq!(
        fts_hits(&cache, "hi"),
        1,
        "the source hit must not linger alongside it"
    );
}

#[test]
fn fts_init_rebuilds_a_legacy_triggered_index() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>legacy wording</p>")
        .expect("body");

    cache
        .conn
        .execute_batch(
            "DROP TABLE messages_fts;
             CREATE VIRTUAL TABLE messages_fts USING fts5(
                 subject, sender, preview, message_id, body,
                 account_id UNINDEXED, folder_path UNINDEXED, uid UNINDEXED);
             CREATE TRIGGER message_headers_fts_ai AFTER INSERT ON message_headers BEGIN
                 DELETE FROM messages_fts WHERE rowid = new.rowid;
             END;",
        )
        .expect("legacy shape");

    cache.fts_init().expect("rebuild");

    let triggers: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE '%fts%'",
            [],
            |r| r.get(0),
        )
        .expect("count triggers");
    assert_eq!(triggers, 0, "legacy triggers must be dropped");

    let sql: String = cache
        .conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'",
            [],
            |r| r.get(0),
        )
        .expect("table sql");
    assert!(
        sql.replace(' ', "").contains("content=''"),
        "the rebuilt table must be contentless"
    );

    assert_eq!(fts_hits(&cache, "hi"), 1, "headers must be reindexed");
    assert_eq!(fts_hits(&cache, "legacy"), 1, "body must be reindexed");
}

#[test]
fn migrate_adds_sync_cursor_columns_to_a_pre_existing_folders_table() {
    let (cache, _dir) = scratch();
    cache
        .conn
        .execute_batch(
            "DROP TABLE folders;
             CREATE TABLE folders (
                 account_id INTEGER NOT NULL,
                 path TEXT NOT NULL,
                 unread_count INTEGER DEFAULT 0,
                 noselect INTEGER DEFAULT 0,
                 kind TEXT NOT NULL DEFAULT 'custom',
                 uidvalidity INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (account_id, path),
                 FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
             );",
        )
        .expect("pre-cursor folders table");

    cache.migrate().expect("re-run migrate");
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");

    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (0, None)
    );
    cache
        .set_folder_last_seen_uid(1, "INBOX", 5_000)
        .expect("set last seen");
    cache
        .set_folder_backfill_cursor(1, "INBOX", Some(4_999))
        .expect("set cursor");
    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (5_000, Some(4_999))
    );
}

#[test]
fn sync_cursors_default_and_round_trip() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");

    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (0, None)
    );

    assert_eq!(
        cache.folder_sync_state(1, "Nope").expect("state"),
        (0, None)
    );

    cache
        .set_folder_last_seen_uid(1, "INBOX", 900)
        .expect("set last seen");
    cache
        .set_folder_backfill_cursor(1, "INBOX", Some(500))
        .expect("set cursor");
    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (900, Some(500))
    );

    cache
        .set_folder_backfill_cursor(1, "INBOX", Some(0))
        .expect("complete");
    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (900, Some(0))
    );
}

#[test]
fn min_cached_uid_is_the_deepest_cached_message() {
    let (cache, _dir) = scratch();
    add_account(&cache);

    assert_eq!(cache.min_cached_uid(1, "INBOX").expect("empty"), None);
    add_message(&cache, 9);
    add_message(&cache, 5);
    add_message(&cache, 7);
    assert_eq!(cache.min_cached_uid(1, "INBOX").expect("min"), Some(5));
}

#[test]
fn clearing_a_folder_resets_its_sync_cursors() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 42);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");
    cache
        .set_folder_last_seen_uid(1, "INBOX", 100)
        .expect("set last seen");
    cache
        .set_folder_backfill_cursor(1, "INBOX", Some(50))
        .expect("set cursor");

    cache.clear_folder_messages(1, "INBOX").expect("clear");

    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (0, None)
    );
    assert_eq!(cache.min_cached_uid(1, "INBOX").expect("min"), None);
}

#[test]
fn a_claimed_action_is_not_claimed_again() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    let action = cache
        .enqueue_action(1, "INBOX", 7, ActionKind::SetFlags, "{}", 100)
        .expect("enqueue");

    let claimed = cache.claim_due_action(1, 100).expect("claim").expect("due");
    assert_eq!(claimed.id, action.id);
    assert_eq!(claimed.uid, 7);
    assert_eq!(claimed.attempts, 0);

    assert!(cache.claim_due_action(1, 100).expect("reclaim").is_none());

    cache.complete_action(action.id).expect("complete");
    assert!(cache.claim_due_action(1, 100).expect("after").is_none());
}

#[test]
fn an_action_is_not_due_before_not_before() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .enqueue_action(1, "INBOX", 1, ActionKind::Delete, "{}", 100)
        .expect("enqueue");

    assert!(cache.claim_due_action(1, 99).expect("early").is_none());
    assert!(cache.claim_due_action(1, 100).expect("due").is_some());
}

#[test]
fn rescheduling_returns_an_action_to_the_queue_with_a_bumped_count() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    let action = cache
        .enqueue_action(
            1,
            "INBOX",
            1,
            ActionKind::Move,
            r#"{"dest":"Trash","refresh_dest":true}"#,
            100,
        )
        .expect("enqueue");
    let claimed = cache.claim_due_action(1, 100).expect("claim").expect("due");
    assert_eq!(claimed.id, action.id);

    let attempts = cache
        .reschedule_action(action.id, 500, "offline")
        .expect("reschedule");
    assert_eq!(attempts, 1);

    assert!(cache.claim_due_action(1, 499).expect("early").is_none());

    let again = cache.claim_due_action(1, 500).expect("due").expect("due");
    assert_eq!(again.attempts, 1);
}

#[test]
fn resetting_in_flight_reclaims_stranded_actions() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .enqueue_action(1, "INBOX", 1, ActionKind::SetFlags, "{}", 100)
        .expect("enqueue");
    assert!(cache.claim_due_action(1, 100).expect("claim").is_some());
    assert!(cache.claim_due_action(1, 100).expect("in flight").is_none());

    assert_eq!(cache.reset_in_flight_actions(1).expect("reset"), 1);
    assert!(cache.claim_due_action(1, 100).expect("after").is_some());
}

#[test]
fn an_unknown_action_kind_is_rejected_not_guessed() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .conn
        .execute(
            "INSERT INTO pending_actions (account_id, folder_path, uid, kind, created_at)
             VALUES (1, 'INBOX', 1, 'explode', 100)",
            [],
        )
        .expect("insert bogus kind");

    assert!(cache.claim_due_action(1, 100).is_err());
}

#[test]
fn a_due_sync_failure_is_claimed_once_and_cleared_on_success() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    let attempts = cache
        .record_sync_failure(1, "INBOX", 7, SyncStage::Body, 100, "timeout")
        .expect("record");
    assert_eq!(attempts, 1);

    assert!(cache
        .claim_due_sync_failure(1, 99, 160)
        .expect("early")
        .is_none());

    let claimed = cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("claim")
        .expect("due");
    assert_eq!(claimed.folder_path, "INBOX");
    assert_eq!(claimed.uid, 7);
    assert_eq!(claimed.stage, SyncStage::Body);
    assert_eq!(claimed.attempts, 1);

    assert!(cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("reclaim")
        .is_none());

    cache
        .clear_sync_failure(1, "INBOX", 7, SyncStage::Body)
        .expect("clear");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 0);
    assert!(cache
        .claim_due_sync_failure(1, 200, 260)
        .expect("after")
        .is_none());
}

#[test]
fn re_recording_a_failure_accumulates_attempts_instead_of_duplicating() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    assert_eq!(
        cache
            .record_sync_failure(1, "INBOX", 7, SyncStage::Header, 100, "boom")
            .expect("first"),
        1
    );
    assert_eq!(
        cache
            .record_sync_failure(1, "INBOX", 7, SyncStage::Header, 400, "boom again")
            .expect("second"),
        2
    );

    assert_eq!(cache.count_sync_failures(1).expect("count"), 1);

    assert!(cache
        .claim_due_sync_failure(1, 399, 460)
        .expect("early")
        .is_none());
    let claimed = cache
        .claim_due_sync_failure(1, 400, 460)
        .expect("claim")
        .expect("due");
    assert_eq!(claimed.attempts, 2);
    assert_eq!(claimed.stage, SyncStage::Header);
}

#[test]
fn header_and_body_failures_for_one_message_are_separate_rows() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .record_sync_failure(1, "INBOX", 7, SyncStage::Header, 100, "h")
        .expect("header");
    cache
        .record_sync_failure(1, "INBOX", 7, SyncStage::Body, 100, "b")
        .expect("body");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 2);

    let a = cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("claim a")
        .expect("due a");
    let b = cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("claim b")
        .expect("due b");
    let mut stages = vec![a.stage.as_str(), b.stage.as_str()];
    stages.sort_unstable();
    assert_eq!(stages, vec!["body", "header"]);
    assert!(cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("third")
        .is_none());
}

#[test]
fn clearing_a_folder_drops_its_queued_fetch_failures() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .record_sync_failure(1, "INBOX", 7, SyncStage::Body, 100, "timeout")
        .expect("record");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 1);

    cache
        .clear_folder_messages(1, "INBOX")
        .expect("clear folder");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 0);
    assert!(cache
        .claim_due_sync_failure(1, 100, 160)
        .expect("claim")
        .is_none());
}

#[test]
fn removing_an_account_cascades_its_sync_failures() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .record_sync_failure(1, "INBOX", 1, SyncStage::Body, 100, "x")
        .expect("record");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 1);

    cache.remove_account(1).expect("remove account");
    assert_eq!(cache.count_sync_failures(1).expect("count"), 0);
}

#[test]
fn an_unknown_sync_stage_is_rejected_not_guessed() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .conn
        .execute(
            "INSERT INTO sync_failures (account_id, folder_path, uid, stage, retry_at)
             VALUES (1, 'INBOX', 1, 'sideways', 100)",
            [],
        )
        .expect("insert bogus stage");

    assert!(cache.claim_due_sync_failure(1, 100, 160).is_err());
}

fn unmigrated() -> (Cache, PathBuf) {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pelliper-migration-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let cache = Cache::open(&dir).expect("open");
    (cache, dir)
}

fn schema_version(cache: &Cache) -> i64 {
    cache
        .conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .expect("user_version")
}

#[test]
fn a_version_zero_database_migrates_to_latest() {
    let (cache, _dir) = unmigrated();
    assert_eq!(
        schema_version(&cache),
        0,
        "fresh database starts at version 0"
    );

    cache.migrate().expect("migrate");

    assert_eq!(schema_version(&cache), crate::migrations::latest_version());

    let found: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_headers'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(
        found, 1,
        "message_headers must be created by the baseline migration"
    );
}

#[test]
fn rerunning_migrations_on_a_current_database_changes_nothing() {
    let (cache, _dir) = scratch();
    let before = schema_version(&cache);
    assert_eq!(before, crate::migrations::latest_version());

    cache.migrate().expect("re-run");

    assert_eq!(schema_version(&cache), before, "idempotent across startups");
}

#[test]
fn a_database_newer_than_this_build_is_rejected_before_any_work() {
    let (cache, _dir) = unmigrated();
    let newer = crate::migrations::latest_version() + 1;
    cache
        .conn
        .pragma_update(None, "user_version", newer)
        .expect("stamp newer version");

    let err = cache
        .migrate()
        .expect_err("a newer schema must be rejected");
    assert!(
        err.to_string().contains("newer than this build"),
        "got: {err}"
    );

    assert_eq!(schema_version(&cache), newer);
    let found: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='accounts'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(found, 0, "nothing must be created for a rejected database");
}

#[test]
fn cached_uids_in_range_returns_uids_within_bounds_sorted() {
    let (cache, _dir) = scratch();
    add_account(&cache);

    for uid in [5, 10, 15, 20, 25] {
        add_message(&cache, uid);
    }

    cache
        .upsert_message(1, "Archive", 15, false, false, false, "")
        .expect("archive message");

    let uids = cache
        .cached_uids_in_range(1, "INBOX", 10, 20)
        .expect("cached uids in range");
    assert_eq!(uids, vec![10, 15, 20]);

    let below = cache
        .cached_uids_in_range(1, "INBOX", 0, 4)
        .expect("below range");
    assert!(below.is_empty());

    let above = cache
        .cached_uids_in_range(1, "INBOX", 30, 50)
        .expect("above range");
    assert!(above.is_empty());

    let archive = cache
        .cached_uids_in_range(1, "Archive", 1, 30)
        .expect("archive uids");
    assert_eq!(archive, vec![15]);
}

#[test]
fn pending_action_uids_returns_distinct_uids_scoped_to_account_and_folder() {
    let (cache, _dir) = scratch();
    add_account(&cache);
    cache
        .conn
        .execute(
            "INSERT INTO accounts (id, email) VALUES (2, 'b@example.com')",
            [],
        )
        .expect("second account");

    cache
        .enqueue_action(1, "INBOX", 10, crate::types::ActionKind::SetFlags, "{}", 0)
        .expect("enqueue 1");

    cache
        .enqueue_action(1, "INBOX", 10, crate::types::ActionKind::Move, "{}", 0)
        .expect("enqueue 2");
    cache
        .enqueue_action(1, "INBOX", 20, crate::types::ActionKind::Delete, "{}", 0)
        .expect("enqueue 3");

    cache
        .enqueue_action(1, "Drafts", 30, crate::types::ActionKind::SetFlags, "{}", 0)
        .expect("enqueue 4");

    cache
        .enqueue_action(2, "INBOX", 10, crate::types::ActionKind::SetFlags, "{}", 0)
        .expect("enqueue 5");

    let inbox_pending = cache.pending_action_uids(1, "INBOX").expect("pending uids");
    assert_eq!(inbox_pending.len(), 2);
    assert!(inbox_pending.contains(&10));
    assert!(inbox_pending.contains(&20));

    let drafts_pending = cache
        .pending_action_uids(1, "Drafts")
        .expect("drafts pending");
    assert_eq!(drafts_pending.len(), 1);
    assert!(drafts_pending.contains(&30));

    let acc2_pending = cache.pending_action_uids(2, "INBOX").expect("acc2 pending");
    assert_eq!(acc2_pending.len(), 1);
    assert!(acc2_pending.contains(&10));
}

#[test]
fn update_message_flags_bulk_modifies_flags_and_returns_number_of_changed_rows() {
    let (mut cache, _dir) = scratch();
    add_account(&cache);

    for uid in [1, 2, 3] {
        add_message(&cache, uid);
    }

    let updates = vec![(1, true, false), (2, false, true), (3, false, false)];
    let count = cache
        .update_message_flags_bulk(1, "INBOX", &updates)
        .expect("bulk update");
    assert_eq!(
        count.len(),
        2,
        "only messages with changed flags count as updated"
    );

    let msgs = cache.load_messages(1, "INBOX", 0, 10).expect("load");
    let m1 = msgs.iter().find(|m| m.uid == 1).unwrap();
    assert!(m1.is_read);
    assert!(!m1.is_starred);

    let m2 = msgs.iter().find(|m| m.uid == 2).unwrap();
    assert!(!m2.is_read);
    assert!(m2.is_starred);

    let m3 = msgs.iter().find(|m| m.uid == 3).unwrap();
    assert!(!m3.is_read);
    assert!(!m3.is_starred);

    assert_eq!(
        cache.message_flags(1, "INBOX", 1).expect("flags"),
        Some((true, false))
    );
    assert_eq!(
        cache.message_flags(1, "INBOX", 2).expect("flags"),
        Some((false, true))
    );
    assert_eq!(
        cache.message_flags(1, "INBOX", 3).expect("flags"),
        Some((false, false))
    );
    assert_eq!(cache.message_flags(1, "INBOX", 999).expect("flags"), None);

    let count2 = cache
        .update_message_flags_bulk(1, "INBOX", &updates)
        .expect("re-run");
    assert!(count2.is_empty(), "no-op updates touch zero rows");
}

#[test]
fn delete_messages_purges_messages_fts_attachments_and_sync_failures() {
    let (mut cache, _dir) = scratch();
    add_account(&cache);

    for uid in [1, 2] {
        add_message(&cache, uid);
        let mut h = sample_headers();
        h.subject = format!("subject {uid}");
        cache
            .save_message_headers(1, "INBOX", uid, &h)
            .expect("headers");
        cache
            .save_body(1, "INBOX", uid, &format!("<p>body content {uid}</p>"))
            .expect("body");
        cache
            .replace_attachments(
                1,
                "INBOX",
                uid,
                &[crate::types::AttachmentRecord {
                    name: format!("file{uid}.txt"),
                    file_path: format!("/tmp/file{uid}.txt"),
                    content_type: "text/plain".to_string(),
                    size: 100,
                }],
            )
            .expect("attachments");
        cache
            .record_sync_failure(
                1,
                "INBOX",
                uid,
                crate::types::SyncStage::Body,
                1000,
                "test failure",
            )
            .expect("failure");
    }

    assert_eq!(fts_hits(&cache, "subject"), 2);
    assert_eq!(fts_hits(&cache, "content"), 2);

    let deleted = cache.delete_messages(1, "INBOX", &[1]).expect("delete");
    assert_eq!(deleted, 1);

    assert_eq!(cache.count_messages(1, "INBOX").expect("count"), 1);
    assert_eq!(fts_hits(&cache, "\"subject 1\""), 0);
    assert_eq!(fts_hits(&cache, "\"body content 1\""), 0);
    assert!(cache
        .load_attachments(1, "INBOX", 1)
        .expect("att")
        .is_empty());

    let sf_count: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sync_failures WHERE account_id = 1 AND folder_path = 'INBOX' AND uid = 1",
            [],
            |r| r.get(0),
        )
        .expect("sf count");
    assert_eq!(sf_count, 0);

    assert_eq!(fts_hits(&cache, "\"subject 2\""), 1);
    assert_eq!(fts_hits(&cache, "\"body content 2\""), 1);
    assert_eq!(cache.load_attachments(1, "INBOX", 2).expect("att").len(), 1);
    let sf2_count: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sync_failures WHERE account_id = 1 AND folder_path = 'INBOX' AND uid = 2",
            [],
            |r| r.get(0),
        )
        .expect("sf2 count");
    assert_eq!(sf2_count, 1);
}
