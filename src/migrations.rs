//! Ordered, transactional schema migrations.
//!
//! Historically the schema was created by `Cache::create_tables`: a batch of
//! `CREATE TABLE IF NOT EXISTS` statements plus a handful of probe-guarded
//! `ALTER TABLE`s, while `PRAGMA user_version` was written but never read. That
//! meant a column added to a released schema silently failed to apply to an
//! existing database (gap #8). This module replaces that with explicit,
//! ordered, transactional steps.
//!
//! `PRAGMA user_version` holds the number of migrations applied so far, so a
//! database at version `v` needs `MIGRATIONS[v..]` run. Each migration is
//! applied in its own transaction together with the `user_version` bump, so a
//! failure leaves the database exactly at its previous version. A database
//! written by a newer build (a higher `user_version` than this build knows) is
//! rejected rather than opened and mangled.
//!
//! The search index is *not* here: `messages_fts` has its own lifecycle
//! (`Cache::fts_init`) because migrating it means detecting the old contentful
//! shape, dropping legacy triggers, and rebuilding from `messages` — logic a
//! plain SQL migration cannot express.

use rusqlite::{Connection, Result};

/// The schema as of the first tracked migration (version 1). Every statement is
/// `IF NOT EXISTS`, so replaying it is harmless — it exists so a *fresh*
/// database gets the whole baseline in one step.
///
/// Databases written before this module existed were stamped `user_version = 1`
/// unconditionally and kept at the current shape by startup probes, so they
/// skip this migration; [`ensure_legacy_columns`] covers the one case where
/// such a database can still lag (a column added by a historical probe).
const M001_INIT: &str = "
    CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        display_name TEXT,
        provider TEXT,
        auth_type TEXT NOT NULL DEFAULT 'password',
        imap_host TEXT,
        imap_port INTEGER,
        imap_security TEXT DEFAULT 'tls',
        smtp_host TEXT,
        smtp_port INTEGER,
        smtp_security TEXT DEFAULT 'starttls',
        username TEXT,
        refresh_token TEXT,
        enabled INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    -- `uidvalidity` is the server's UIDVALIDITY for this folder, or 0 when never
    -- observed. IMAP UIDs are only stable while a folder's UIDVALIDITY is
    -- unchanged; if it ever changes the server has renumbered every message, so
    -- the cached (folder, uid) rows now name different mail and must be
    -- discarded.
    --
    -- Sync cursors. `last_seen_uid` is the forward high-water mark: every UID at
    -- or below it has been accounted for, so incremental sync only asks the
    -- server for UIDs above it. `backfill_cursor` is history-backfill progress:
    -- the top of the next UID window to fetch (NULL = never planned, 0 =
    -- complete). Persisting both is what lets a restart resume instead of
    -- re-planning with `UID SEARCH ALL`.
    CREATE TABLE IF NOT EXISTS folders (
        account_id INTEGER NOT NULL,
        path TEXT NOT NULL,
        unread_count INTEGER DEFAULT 0,
        noselect INTEGER DEFAULT 0,
        kind TEXT NOT NULL DEFAULT 'custom',
        uidvalidity INTEGER NOT NULL DEFAULT 0,
        last_seen_uid INTEGER NOT NULL DEFAULT 0,
        backfill_cursor INTEGER,
        PRIMARY KEY (account_id, path),
        FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS messages (
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        has_attachments INTEGER DEFAULT 0,
        is_read INTEGER DEFAULT 0,
        is_starred INTEGER DEFAULT 0,
        preview TEXT,
        PRIMARY KEY (account_id, folder_path, uid)
    );

    -- Every header of every message, and the only place they live.
    --
    -- The six `*_addr` columns are JSON arrays of objects with `name` and
    -- `email` keys, e.g. a one-element list holding Alice's display name and
    -- address. An absent header is the two-character string `[]`, not NULL. They
    -- are structured rather than display strings so recipients stay addressable
    -- (a group entry has no addr-spec, and the UI wants the name and the address
    -- separately); the UI renders one back to text on read. FTS indexes
    -- `from_addr` as its `sender` column -- FTS5 tokenizes the JSON punctuation
    -- away, so both the name and the address stay searchable.
    --
    -- `date_ts` is the parsed unix timestamp of the `Date:` header and exists
    -- because `date` itself is a string: ordering by the raw header sorts
    -- lexically on the weekday name. Every `ORDER BY` on message recency uses
    -- this column, never `date`.
    --
    -- `ENVELOPE` has no `references` field, so `references_` is the one column
    -- that cannot come from it; the daemon fetches
    -- `HEADER.FIELDS (REFERENCES IN-REPLY-TO)` alongside the envelope purely to
    -- fill it. Threading reads it.
    --
    -- FK to `messages` is what forces write order: a header row cannot be
    -- inserted before its message, which is also why the search index is rebuilt
    -- explicitly after a header write rather than from a trigger (see
    -- `Cache::index_message`).
    CREATE TABLE IF NOT EXISTS message_headers (
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        subject TEXT,
        from_addr TEXT,
        sender_addr TEXT,
        reply_to_addr TEXT,
        to_addr TEXT,
        cc_addr TEXT,
        bcc_addr TEXT,
        date TEXT,
        date_ts INTEGER NOT NULL DEFAULT 0,
        in_reply_to TEXT,
        references_ TEXT,
        message_id TEXT,
        PRIMARY KEY (account_id, folder_path, uid),
        FOREIGN KEY (account_id, folder_path, uid) REFERENCES messages(account_id, folder_path, uid) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS bodies (
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        html TEXT,
        PRIMARY KEY (account_id, folder_path, uid),
        FOREIGN KEY (account_id, folder_path, uid) REFERENCES messages(account_id, folder_path, uid) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS attachments (
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        content_type TEXT,
        size INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS sender_policies (
        sender_email TEXT PRIMARY KEY,
        allow_remote INTEGER DEFAULT 0,
        block_sender INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS addresses (
        email TEXT PRIMARY KEY,
        display_name TEXT,
        frequency INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER NOT NULL,
        to_addr TEXT NOT NULL,
        cc_addr TEXT,
        bcc_addr TEXT,
        reply_to TEXT,
        subject TEXT,
        body TEXT,
        attempts INTEGER DEFAULT 0,
        created_at INTEGER NOT NULL
    );

    -- Durable ledger for non-send mutations (flags/move/delete). The user's
    -- intent is written here BEFORE the server is touched, so an offline or
    -- rejected operation survives a restart and is retried by the worker's drain
    -- loop instead of being lost. Sends keep their own `outbox` table above:
    -- different transport, different budget.
    --
    -- `state` is 'pending' (eligible for claiming) or 'in_flight' (a worker has
    -- claimed it). `not_before` gates retries/backoff. `payload` is kind-specific
    -- JSON. Unknown kinds are rejected on read rather than guessed at.
    CREATE TABLE IF NOT EXISTS pending_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL DEFAULT '{}',
        state TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        not_before INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        last_error TEXT,
        FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS pending_actions_due
        ON pending_actions (account_id, state, not_before);

    -- Durable ledger for per-message header/body fetch failures. A failed fetch
    -- is recorded here and retried by the worker's loop instead of being logged
    -- and forgotten: the prefetch queues live in memory, so an older message is
    -- otherwise never retried. The row is keyed so a message accumulates
    -- attempts rather than piling up duplicates.
    --
    -- `stage` is 'header' or 'body'. `retry_at` gates the retry (a due row has
    -- `retry_at <= now`). Unlike `pending_actions` there is no `state`: the
    -- retry runs synchronously in the single worker task, so a crash just leaves
    -- the row due rather than in flight.
    CREATE TABLE IF NOT EXISTS sync_failures (
        account_id INTEGER NOT NULL,
        folder_path TEXT NOT NULL,
        uid INTEGER NOT NULL,
        stage TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        retry_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        PRIMARY KEY (account_id, folder_path, uid, stage),
        FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS sync_failures_due
        ON sync_failures (account_id, retry_at);
";

/// Ordered migrations: index `i` upgrades `user_version = i` to `i + 1`.
pub const MIGRATIONS: &[&str] = &[M001_INIT];

/// Highest `user_version` this build understands.
pub fn latest_version() -> i64 {
    MIGRATIONS.len() as i64
}

/// Bring a connection up to [`latest_version`].
///
/// Applies every migration the database has not seen, each in its own
/// transaction alongside the `user_version` bump, then repairs the handful of
/// columns that pre-migration databases added via startup probes. Errors if the
/// database is newer than this build.
pub fn run(conn: &Connection) -> Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let latest = latest_version();

    if version > latest {
        // `SQLITE_NOTADB` is what SQLite itself reports for a file whose format
        // is newer than this version understands, which is exactly the situation:
        // the message is what the caller sees (rusqlite's `Display` prefers it).
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOTADB),
            Some(format!(
                "database schema version {version} is newer than this build supports \
                 (max {latest}); refusing to open it"
            )),
        ));
    }

    // `skip(version)` is what makes this safe: a database already at `version`
    // re-runs nothing, and a fresh one (0) runs everything in order.
    for (index, migration) in MIGRATIONS.iter().enumerate().skip(version as usize) {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(migration)?;
        tx.pragma_update(None, "user_version", index as i64 + 1)?;
        tx.commit()?;
    }

    ensure_legacy_columns(conn)?;
    Ok(())
}

/// Add the columns that pre-migration databases used to acquire through
/// probe-guarded `ALTER TABLE`s in `create_tables`.
///
/// Such a database carries the unreliable `user_version = 1` stamp and skips
/// [`M001_INIT`], so a column added by a historical probe would otherwise be
/// missing for good. This is idempotent (it checks `PRAGMA table_info` first) and
/// is a bridge for those databases only; new columns must go through
/// [`MIGRATIONS`] instead.
fn ensure_legacy_columns(conn: &Connection) -> Result<()> {
    let columns: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(folders)")?;
        let names = stmt.query_map([], |row| row.get::<_, String>(1))?;
        names.filter_map(Result::ok).collect()
    };
    let add = |name: &str, ddl: &str| -> Result<()> {
        if columns.iter().any(|c| c == name) {
            Ok(())
        } else {
            conn.execute_batch(ddl)
        }
    };
    add(
        "uidvalidity",
        "ALTER TABLE folders ADD COLUMN uidvalidity INTEGER NOT NULL DEFAULT 0;",
    )?;
    add(
        "last_seen_uid",
        "ALTER TABLE folders ADD COLUMN last_seen_uid INTEGER NOT NULL DEFAULT 0;",
    )?;
    add(
        "backfill_cursor",
        "ALTER TABLE folders ADD COLUMN backfill_cursor INTEGER;",
    )?;
    Ok(())
}
