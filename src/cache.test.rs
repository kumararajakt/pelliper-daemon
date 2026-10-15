//! Schema contract tests for the cache.
//!
//! `message_headers` is keyed to `messages` by a composite foreign key, which
//! makes write *ordering* and delete cascading load-bearing behaviour rather
//! than incidental details — both are asserted here against the real `Cache`,
//! not a copy of the DDL.

use super::*;
use crate::types::{ActionKind, MessageHeaders};
use std::sync::atomic::{AtomicU32, Ordering};

/// A fresh cache in a throwaway data dir, with the schema and the search
/// index created — both are needed to assert anything about FTS behaviour.
fn scratch() -> (Cache, PathBuf) {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pelliper-cache-test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let cache = Cache::open(&dir).expect("open");
    cache.create_tables().expect("create_tables");
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

/// Insert the parent `messages` row a header row depends on.
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
fn create_tables_adds_message_headers() {
    let (cache, _dir) = scratch();
    let found: i64 = cache
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_headers'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(found, 1, "message_headers must exist after create_tables");
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
    // The address columns hold exactly what the daemon serialized, JSON and
    // all — the cache is not a second place that reformats them.
    assert_eq!(from, r#"[{"name":"Alice","email":"alice@example.com"}]"#);
    assert_eq!(
        reply_to,
        r#"[{"name":"Alice","email":"alice@work.example.com"}]"#
    );
    assert_eq!(to, r#"[{"name":"","email":"bob@example.com"}]"#);
    assert_eq!(cc, "[]");
    // `date` stays a column name, unquoted, holding the raw Date header.
    assert_eq!(date, "Mon, 5 Jan 2026 10:00:00 +0000");
    assert_eq!(message_id, "<m1@example.com>");

    // An absent list is the empty array, not NULL, so consumers never have to
    // tell "no header" apart from "unparseable header".
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
    // The UI sorts and searches on the sender, and FTS indexes `from_addr` as
    // its `sender` column, so the JSON has to be queryable as-is.
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
    // This is why every write site calls upsert_message first.
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

    // A server that returned neither ENVELOPE nor a header block must not
    // leave an empty shell behind.
    cache
        .save_message_headers(1, "INBOX", 7, &MessageHeaders::default())
        .expect("save is a no-op");

    let rows: i64 = cache
        .conn
        .query_row("SELECT COUNT(*) FROM message_headers", [], |r| r.get(0))
        .expect("count");
    assert_eq!(rows, 0);
}

/// Count FTS rows matching `term`, i.e. what a search would return.
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
    // The indexed columns live in `message_headers`, so reindexing had to move
    // there too (there are no triggers now — the writers index explicitly).
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("save");

    assert_eq!(fts_hits(&cache, "hi"), 1, "subject must be indexed");
    assert_eq!(fts_hits(&cache, "alice"), 1, "sender must be indexed");
    // Quoted: a bare term with dots is FTS5 syntax, not a literal.
    assert_eq!(
        fts_hits(&cache, "\"m1@example.com\""),
        1,
        "message-id must be indexed"
    );
}

#[test]
fn saving_a_body_indexes_it_instead_of_rolling_the_insert_back() {
    // Regression test. The old bodies_fts_ai did
    // `INSERT INTO messages_fts(body, rowid) VALUES(...)`, and FTS5 rejects a
    // partial insert against an existing rowid ('constraint failed'). That
    // error rolled back the *enclosing* `INSERT INTO bodies`, so bodies were
    // never stored and never indexed. Every caller swallowed it with
    // `let _ = cache.save_body(...)`, so the failure was invisible.
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
    // Flag changes do not reindex: `update_message_flags` touches `messages`
    // only, and the index is rebuilt solely from `message_headers`/`bodies`
    // writes. A naive reindex here would rebuild the row from the (empty)
    // preview and destroy the subject/sender index, so this guards the
    // invariant that toggling a flag leaves search untouched.
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
    // `date` is the raw Date header, so ordering by it sorts on the weekday
    // name; `date_ts` is the parsed timestamp every ORDER BY must use.
    let (cache, _dir) = scratch();
    add_account(&cache);

    // Two messages whose header strings sort in the opposite order to their
    // timestamps: "Wed, 9 Sep" < "Wed, 8 Jul" lexically, but Sep is later.
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
    // The UI joins headers in with a LEFT JOIN; a message whose header row is
    // missing must not vanish from the list.
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);

    let rows = cache.load_messages(1, "INBOX", 0, 10).expect("load");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uid, 7);
    assert_eq!(rows[0].subject, None, "no header row means no subject");
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

    // The source rows are gone (deleted, and cascaded).
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

    // The destination kept every part, including the threading fields.
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

    // Never observed yet.
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 0);
    cache
        .set_folder_uidvalidity(1, "INBOX", 4_200)
        .expect("set");
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 4_200);
    // An unknown folder reads as "never observed", not an error.
    assert_eq!(cache.folder_uidvalidity(1, "Nope").expect("read"), 0);
}

#[test]
fn create_tables_adds_uidvalidity_to_a_pre_existing_folders_table() {
    // `CREATE TABLE IF NOT EXISTS` cannot evolve a table, so databases written
    // before UIDVALIDITY tracking get the column via a targeted ALTER during
    // create_tables. Recreate the old shape and re-run it.
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

    cache.create_tables().expect("re-run create_tables");
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");
    // The newly added column defaults to "never observed" and is writable.
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 0);
    cache.set_folder_uidvalidity(1, "INBOX", 99).expect("set");
    assert_eq!(cache.folder_uidvalidity(1, "INBOX").expect("read"), 99);
}

#[test]
fn rediscovering_a_folder_preserves_its_uidvalidity() {
    // `connect()` re-upserts every folder on startup with a fresh unread
    // count; that must not wipe the recorded UIDVALIDITY, or the next sync
    // would mistake the folder for brand new.
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

    // INBOX: message + header + body, so the delete has to cascade and leave
    // the search index clean.
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>quarterly snow forecast</p>")
        .expect("body");

    // A second folder that must survive untouched.
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

    // Other folders keep their mail.
    assert_eq!(cache.count_messages(1, "Archive").expect("count"), 1);
    assert_eq!(fts_hits(&cache, "archived"), 1);
}

#[test]
fn moving_a_message_rekeys_its_index_row_to_the_destination() {
    // The index row is keyed on `messages.rowid`, which changes on a move, so a
    // count alone is not enough: the surviving hit must join back to the
    // destination row (and not merely be a stale row under the old rowid).
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
    // An upgraded database still holds the old contentful table and its
    // triggers. `fts_init` must drop the triggers, convert the table to the
    // contentless shape, and reindex the existing mail.
    let (cache, _dir) = scratch();
    add_account(&cache);
    add_message(&cache, 7);
    cache
        .save_message_headers(1, "INBOX", 7, &sample_headers())
        .expect("headers");
    cache
        .save_body(1, "INBOX", 7, "<p>legacy wording</p>")
        .expect("body");

    // Recreate the pre-#7 shape: contentful, with a whole-row trigger.
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
fn create_tables_adds_sync_cursor_columns_to_a_pre_existing_folders_table() {
    // Same story as uidvalidity: an old-format DB has no cursor columns, and
    // `CREATE TABLE IF NOT EXISTS` won't evolve it. Recreate the pre-cursor
    // shape (with uidvalidity, as after the gap #1 migration) and re-run.
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

    cache.create_tables().expect("re-run create_tables");
    add_account(&cache);
    cache
        .upsert_folder(1, "INBOX", 0, false, "inbox")
        .expect("folder");

    // Defaults: high-water 0, backfill never planned.
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

    // A fresh folder has no high-water and no backfill plan.
    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (0, None)
    );
    // An unknown folder reads the same as a fresh one — sync seeds it.
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

    // Some(0) marks history complete.
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

    // A UIDVALIDITY change invalidates the UID space, so the cursors pointing
    // into it must go: the folder re-syncs cold and re-plans from scratch.
    assert_eq!(
        cache.folder_sync_state(1, "INBOX").expect("state"),
        (0, None)
    );
    assert_eq!(cache.min_cached_uid(1, "INBOX").expect("min"), None);
}

// ---------------------------------------------------------------------------
// Durable action ledger
// ---------------------------------------------------------------------------

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

    // While it is claimed (in-flight) it must not be handed out again.
    assert!(cache.claim_due_action(1, 100).expect("reclaim").is_none());

    // Once completed it is gone entirely.
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

    // Backed off until `not_before`...
    assert!(cache.claim_due_action(1, 499).expect("early").is_none());
    // ...then claimable again, carrying the bumped attempt count.
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

    // A worker restart recovers the stranded action.
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

    // Misreading a destructive intent is worse than surfacing the error.
    assert!(cache.claim_due_action(1, 100).is_err());
}
