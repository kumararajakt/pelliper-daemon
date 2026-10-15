use crate::types::{
    Account, ActionKind, AttachmentRecord, Folder, Message, MessageHeaders, OutboxItem,
    PendingAction, SyncFailure, SyncStage,
};
use rusqlite::{Connection, OptionalExtension, Result};
use std::path::PathBuf;

pub struct Cache {
    conn: Connection,
}

/// Build the search-index row for one message. `LEFT JOIN bodies` so a message
/// whose body has not been fetched yet is still searchable by its headers. The
/// rowid is `messages.rowid`, which is what a search hit joins back on.
const INDEX_FTS_SQL: &str = "\
INSERT INTO messages_fts(rowid, subject, sender, preview, message_id, body)
SELECT m.rowid, h.subject, h.from_addr, m.preview, h.message_id, b.html
FROM messages m
JOIN message_headers h
  ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
LEFT JOIN bodies b
  ON b.account_id = m.account_id AND b.folder_path = m.folder_path AND b.uid = m.uid
WHERE m.account_id = ?1 AND m.folder_path = ?2 AND m.uid = ?3";

/// Rebuild every index row: [`INDEX_FTS_SQL`] without the per-message filter.
/// Messages with no header row are skipped (nothing to index).
const BACKFILL_FTS_SQL: &str = "\
INSERT INTO messages_fts(rowid, subject, sender, preview, message_id, body)
SELECT m.rowid, h.subject, h.from_addr, m.preview, h.message_id, b.html
FROM messages m
JOIN message_headers h
  ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
LEFT JOIN bodies b
  ON b.account_id = m.account_id AND b.folder_path = m.folder_path AND b.uid = m.uid";

/// Delete the index row(s) for one message. Resolves the rowid through the live
/// `messages` row, so it must run before that row is deleted.
const DEINDEX_MESSAGE_SQL: &str = "\
DELETE FROM messages_fts WHERE rowid IN
    (SELECT rowid FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3)";

/// Delete the index rows for a whole folder; see [`DEINDEX_MESSAGE_SQL`] on
/// ordering.
const DEINDEX_FOLDER_SQL: &str = "\
DELETE FROM messages_fts WHERE rowid IN
    (SELECT rowid FROM messages WHERE account_id = ?1 AND folder_path = ?2)";

/// Delete the index rows for a whole account; see [`DEINDEX_MESSAGE_SQL`] on
/// ordering.
const DEINDEX_ACCOUNT_SQL: &str = "\
DELETE FROM messages_fts WHERE rowid IN
    (SELECT rowid FROM messages WHERE account_id = ?1)";

impl Cache {
    pub fn open(data_dir: &PathBuf) -> Result<Self> {
        std::fs::create_dir_all(data_dir).ok();
        let db_path = data_dir.join("cache.db");
        let conn = Connection::open(db_path)?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;

        Ok(Self { conn })
    }

    /// Bring the schema up to the version this build expects by running any
    /// pending migrations (see [`crate::migrations`]).
    ///
    /// This replaces the old `create_tables`: schema changes now go through the
    /// migration runner, so a column added to a released schema actually applies
    /// to an existing database instead of being a `CREATE TABLE IF NOT EXISTS`
    /// no-op. The search index has its own initializer ([`Cache::fts_init`]),
    /// which keeps its create/migrate/reindex logic out of plain SQL.
    pub fn migrate(&self) -> Result<()> {
        crate::migrations::run(&self.conn)
    }

    /// Create the full-text search index (FTS5).
    ///
    /// The index is **contentless** (`content=''`): the text is tokenized but
    /// not stored a second time, and there are **no triggers**. Instead the
    /// writers that change searchable content call [`Cache::index_message`] /
    /// [`Cache::deindex_message`] explicitly. That is deliberate: the old
    /// whole-row-rebuild triggers were fragile — a partial-column INSERT
    /// against an existing FTS5 rowid fails and rolls back the *enclosing*
    /// statement, which once silently discarded every cached body (see
    /// `saving_a_body_indexes_it_instead_of_rolling_the_insert_back`).
    ///
    /// A contentless table cannot be updated in place, so `index_message`
    /// deletes then re-inserts; `contentless_delete=1` (SQLite 3.43+) is what
    /// makes that DELETE legal. Rows are keyed on `messages.rowid`, so a search
    /// hit joins straight back to the message.
    ///
    /// Databases written before this change still hold the contentful table and
    /// its triggers. `fts_init` drops the triggers and, when it finds the old
    /// shape, rebuilds the table and reindexes from `messages`.
    pub fn fts_init(&self) -> Result<()> {
        // The triggers are the thing being removed; drop them unconditionally
        // so an upgraded database cannot keep firing them.
        self.conn.execute_batch(
            "DROP TRIGGER IF EXISTS message_headers_fts_ai;
             DROP TRIGGER IF EXISTS message_headers_fts_au;
             DROP TRIGGER IF EXISTS message_headers_fts_ad;
             DROP TRIGGER IF EXISTS bodies_fts_ai;
             DROP TRIGGER IF EXISTS bodies_fts_au;
             DROP TRIGGER IF EXISTS bodies_fts_ad;
             DROP TRIGGER IF EXISTS messages_fts_ad;",
        )?;

        if !self.fts_is_current()? {
            // Old contentful table (or none): drop and recreate. The index is
            // rebuilt from `messages` by the backfill below, so nothing is lost.
            self.conn
                .execute_batch("DROP TABLE IF EXISTS messages_fts;")?;
        }
        self.conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                 subject, sender, preview, message_id, body,
                 content='', contentless_delete=1
             );",
        )?;

        self.backfill_fts()
    }

    /// Whether `messages_fts` already exists in the current (contentless)
    /// shape. A table written by an older build stores its content and must be
    /// rebuilt.
    fn fts_is_current(&self) -> Result<bool> {
        let sql: Option<String> = self
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(sql
            .map(|s| s.replace(' ', "").contains("content=''"))
            .unwrap_or(false))
    }

    /// Rebuild the whole index from `messages` + `message_headers` + `bodies`,
    /// but only when it is empty — so an existing cache is indexed on the first
    /// run (or after a shape rebuild) without doing work every startup.
    fn backfill_fts(&self) -> Result<()> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM messages_fts", [], |r| r.get(0))?;
        if count == 0 {
            self.conn.execute_batch(BACKFILL_FTS_SQL)?;
        }
        Ok(())
    }

    /// (Re)build one message's search-index row. Contentless FTS5 cannot update
    /// in place, so the row is deleted first (a no-op if it was never indexed).
    /// A message with no header row is left out — there is nothing to index.
    fn index_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.deindex_message(account_id, folder_path, uid)?;
        self.conn.execute(
            INDEX_FTS_SQL,
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    /// Drop one message's search-index row. The rowid is resolved through the
    /// live `messages` row, so this must run before the message is deleted.
    fn deindex_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.conn.execute(
            DEINDEX_MESSAGE_SQL,
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    /// Drop the index rows for every message in a folder. Must run before the
    /// folder's `messages` rows are deleted.
    fn deindex_folder(&self, account_id: i64, folder_path: &str) -> Result<()> {
        self.conn.execute(
            DEINDEX_FOLDER_SQL,
            rusqlite::params![account_id, folder_path],
        )?;
        Ok(())
    }

    /// Drop the index rows for every message in an account. Must run before the
    /// account's `messages` rows are deleted.
    fn deindex_account(&self, account_id: i64) -> Result<()> {
        self.conn
            .execute(DEINDEX_ACCOUNT_SQL, rusqlite::params![account_id])?;
        Ok(())
    }

    pub fn load_accounts(&self) -> Result<Vec<Account>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, email, display_name, provider, auth_type, imap_host, imap_port, imap_security, smtp_host, smtp_port, smtp_security, username, refresh_token, enabled FROM accounts",
        )?;

        let accounts = stmt
            .query_map([], |row| {
                Ok(Account {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    display_name: row.get(2)?,
                    provider: row.get(3)?,
                    auth_type: row.get(4)?,
                    imap_host: row.get(5)?,
                    imap_port: row.get(6)?,
                    imap_security: row.get(7)?,
                    smtp_host: row.get(8)?,
                    smtp_port: row.get(9)?,
                    smtp_security: row.get(10)?,
                    username: row.get(11)?,
                    refresh_token: row.get(12)?,
                    enabled: row.get(13)?,
                })
            })?
            .collect::<Result<Vec<_>>>()?;

        Ok(accounts)
    }

    pub fn insert_account(&mut self, account: &Account) -> Result<i64> {
        self.conn.execute(
            "INSERT OR REPLACE INTO accounts (email, display_name, provider, auth_type, imap_host, imap_port, imap_security, smtp_host, smtp_port, smtp_security, username, refresh_token, enabled) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                account.email,
                account.display_name,
                account.provider,
                account.auth_type,
                account.imap_host,
                account.imap_port,
                account.imap_security,
                account.smtp_host,
                account.smtp_port,
                account.smtp_security,
                account.username,
                account.refresh_token,
                account.enabled,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn account_count(&self) -> Result<i64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM accounts", [], |row| row.get(0))?;
        Ok(count)
    }

    pub fn account_exists(&self, email: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts WHERE email = ?1",
            rusqlite::params![email],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Delete an account and all its folders, messages, bodies, and attachments.
    pub fn remove_account(&self, account_id: i64) -> Result<()> {
        // Foreign keys with ON DELETE CASCADE handle messages/bodies/folders.
        // The search index has no FK, so clear it before the messages go.
        self.deindex_account(account_id)?;
        self.conn.execute(
            "DELETE FROM attachments WHERE account_id = ?1",
            rusqlite::params![account_id],
        )?;
        self.conn.execute(
            "DELETE FROM bodies WHERE account_id = ?1",
            rusqlite::params![account_id],
        )?;
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1",
            rusqlite::params![account_id],
        )?;
        self.conn.execute(
            "DELETE FROM folders WHERE account_id = ?1",
            rusqlite::params![account_id],
        )?;
        self.conn.execute(
            "DELETE FROM accounts WHERE id = ?1",
            rusqlite::params![account_id],
        )?;
        tracing::info!("Removed account {} and all associated data", account_id);
        Ok(())
    }

    pub fn upsert_folder(
        &self,
        account_id: i64,
        path: &str,
        unread_count: i32,
        noselect: bool,
        kind: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO folders (account_id, path, unread_count, noselect, kind) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(account_id, path) DO UPDATE SET unread_count = ?3, noselect = ?4, kind = ?5",
            rusqlite::params![account_id, path, unread_count, noselect as i32, kind],
        )?;
        Ok(())
    }

    pub fn upsert_message(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        is_read: bool,
        is_starred: bool,
        has_attachments: bool,
        preview: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (account_id, folder_path, uid, is_read, is_starred, has_attachments, preview)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET
                is_read = ?4, is_starred = ?5, has_attachments = ?6, preview = ?7",
            rusqlite::params![account_id, folder_path, uid, is_read as i32, is_starred as i32, has_attachments as i32, preview],
        )?;
        Ok(())
    }

    pub fn update_folder_unread(&self, account_id: i64, path: &str, unread: i32) -> Result<()> {
        self.conn.execute(
            "UPDATE folders SET unread_count = ?1 WHERE account_id = ?2 AND path = ?3",
            rusqlite::params![unread, account_id, path],
        )?;
        Ok(())
    }

    /// Update the read/starred flags of one message in place.
    pub fn update_message_flags(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        is_read: Option<bool>,
        is_starred: Option<bool>,
    ) -> Result<()> {
        if let Some(read) = is_read {
            self.conn.execute(
                "UPDATE messages SET is_read = ?1
                 WHERE account_id = ?2 AND folder_path = ?3 AND uid = ?4",
                rusqlite::params![read as i32, account_id, folder_path, uid],
            )?;
        }
        if let Some(starred) = is_starred {
            self.conn.execute(
                "UPDATE messages SET is_starred = ?1
                 WHERE account_id = ?2 AND folder_path = ?3 AND uid = ?4",
                rusqlite::params![starred as i32, account_id, folder_path, uid],
            )?;
        }
        Ok(())
    }

    /// Move a message (and its cached headers and body) from one folder to
    /// another.  All three tables are keyed by the same composite key, so each
    /// is copied before the source row is deleted (which cascades the rest).
    /// The search-index row is re-keyed from the source rowid to the
    /// destination's while both rows exist.
    pub fn move_message(
        &self,
        account_id: i64,
        from_folder: &str,
        uid: i64,
        to_folder: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO messages
                (account_id, folder_path, uid, has_attachments, is_read, is_starred, preview)
             SELECT account_id, ?2, uid, has_attachments, is_read, is_starred, preview
             FROM messages
             WHERE account_id = ?1 AND folder_path = ?3 AND uid = ?4",
            rusqlite::params![account_id, to_folder, from_folder, uid],
        )?;
        self.conn.execute(
            "INSERT OR REPLACE INTO message_headers
                (account_id, folder_path, uid, subject, from_addr, sender_addr, reply_to_addr,
                 to_addr, cc_addr, bcc_addr, date, date_ts, in_reply_to, references_, message_id)
             SELECT account_id, ?2, uid, subject, from_addr, sender_addr, reply_to_addr,
                    to_addr, cc_addr, bcc_addr, date, date_ts, in_reply_to, references_, message_id
             FROM message_headers
             WHERE account_id = ?1 AND folder_path = ?3 AND uid = ?4",
            rusqlite::params![account_id, to_folder, from_folder, uid],
        )?;
        self.conn.execute(
            "INSERT OR REPLACE INTO bodies (account_id, folder_path, uid, html)
             SELECT account_id, ?2, uid, html
             FROM bodies
             WHERE account_id = ?1 AND folder_path = ?3 AND uid = ?4",
            rusqlite::params![account_id, to_folder, from_folder, uid],
        )?;
        // Reindex under the destination before the source row disappears; the
        // source's index row is keyed on its old `messages.rowid`.
        self.index_message(account_id, to_folder, uid)?;
        self.deindex_message(account_id, from_folder, uid)?;
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, from_folder, uid],
        )?;
        Ok(())
    }

    /// Remove a message row (and its cascade-deleted body), and its search-index
    /// row.
    pub fn delete_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.deindex_message(account_id, folder_path, uid)?;
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    /// Remove a folder (and all its messages via CASCADE) from the cache.
    pub fn delete_folder(&self, account_id: i64, path: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM folders WHERE account_id = ?1 AND path = ?2",
            rusqlite::params![account_id, path],
        )?;
        Ok(())
    }

    /// The stored UIDVALIDITY for a folder, or 0 when it has never been
    /// observed (a missing folder row also reads as 0).
    pub fn folder_uidvalidity(&self, account_id: i64, folder_path: &str) -> Result<i64> {
        let result = self.conn.query_row(
            "SELECT uidvalidity FROM folders WHERE account_id = ?1 AND path = ?2",
            rusqlite::params![account_id, folder_path],
            |row| row.get::<_, i64>(0),
        );
        match result {
            Ok(value) => Ok(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Record the UIDVALIDITY the server currently reports for a folder.
    pub fn set_folder_uidvalidity(
        &self,
        account_id: i64,
        folder_path: &str,
        uidvalidity: i64,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE folders SET uidvalidity = ?1 WHERE account_id = ?2 AND path = ?3",
            rusqlite::params![uidvalidity, account_id, folder_path],
        )?;
        Ok(())
    }

    /// Drop every cached message in a folder, used when the server reassigns
    /// UIDs (a UIDVALIDITY change) and `(folder, uid)` can no longer be
    /// trusted. Message headers and bodies cascade; attachment rows are keyed
    /// without a foreign key, so they are removed explicitly. Search-index rows
    /// are removed first (there are no triggers). The sync cursors are reset
    /// too — they are meaningless once the UID space is invalidated, as are any
    /// queued fetch retries for the old UIDs.
    /// Returns the number of messages removed.
    pub fn clear_folder_messages(&self, account_id: i64, folder_path: &str) -> Result<usize> {
        self.deindex_folder(account_id, folder_path)?;
        let removed = self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, folder_path],
        )?;
        self.conn.execute(
            "DELETE FROM attachments WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, folder_path],
        )?;
        self.conn.execute(
            "DELETE FROM sync_failures WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, folder_path],
        )?;
        self.conn.execute(
            "UPDATE folders SET last_seen_uid = 0, backfill_cursor = NULL
             WHERE account_id = ?1 AND path = ?2",
            rusqlite::params![account_id, folder_path],
        )?;
        Ok(removed)
    }

    /// The folder's persisted sync cursors: `(last_seen_uid, backfill_cursor)`.
    /// `last_seen_uid` is the forward high-water mark (0 = never synced);
    /// `backfill_cursor` is the top of the next history window (None = never
    /// planned, Some(0) = complete). An unknown folder reads as a fresh one.
    pub fn folder_sync_state(
        &self,
        account_id: i64,
        folder_path: &str,
    ) -> Result<(i64, Option<i64>)> {
        let result = self.conn.query_row(
            "SELECT last_seen_uid, backfill_cursor FROM folders WHERE account_id = ?1 AND path = ?2",
            rusqlite::params![account_id, folder_path],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        );
        match result {
            Ok(state) => Ok(state),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((0, None)),
            Err(e) => Err(e),
        }
    }

    /// Advance (or initialize) a folder's forward high-water UID. Only ever
    /// grows: incremental sync asks for UIDs strictly above this.
    pub fn set_folder_last_seen_uid(
        &self,
        account_id: i64,
        folder_path: &str,
        last_seen_uid: i64,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE folders SET last_seen_uid = ?1 WHERE account_id = ?2 AND path = ?3",
            rusqlite::params![last_seen_uid, account_id, folder_path],
        )?;
        Ok(())
    }

    /// Persist history-backfill progress: the top of the next UID window to
    /// fetch. `Some(0)` marks the folder fully backfilled.
    pub fn set_folder_backfill_cursor(
        &self,
        account_id: i64,
        folder_path: &str,
        cursor: Option<i64>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE folders SET backfill_cursor = ?1 WHERE account_id = ?2 AND path = ?3",
            rusqlite::params![cursor, account_id, folder_path],
        )?;
        Ok(())
    }

    /// UIDs of messages cached in a given range [lo, hi] (inclusive), ascending.
    pub fn delete_messages(
        &mut self,
        account_id: i64,
        folder_path: &str,
        uids: &[i64],
    ) -> Result<usize> {
        if uids.is_empty() {
            return Ok(0);
        }
        let mut removed = 0;
        let tx = self.conn.transaction()?;
        for uid in uids {
            // Deindex before the message row goes, so the rowid subquery
            // resolves; the transaction keeps the two in step.
            tx.execute(
                DEINDEX_MESSAGE_SQL,
                rusqlite::params![account_id, folder_path, uid],
            )?;
            removed += tx.execute(
                "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
                rusqlite::params![account_id, folder_path, uid],
            )?;
        }
        tx.commit()?;
        Ok(removed)
    }

    /// The lowest UID already cached in a folder, if any. Used to seed the
    /// history cursor so an existing cache is not re-downloaded.
    pub fn min_cached_uid(&self, account_id: i64, folder_path: &str) -> Result<Option<i64>> {
        let min: Option<i64> = self.conn.query_row(
            "SELECT MIN(uid) FROM messages WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, folder_path],
            |row| row.get(0),
        )?;
        Ok(min)
    }

    pub fn load_folders(&self, account_id: i64) -> Result<Vec<Folder>> {
        let mut stmt = self.conn.prepare(
            "SELECT path, unread_count, noselect, kind FROM folders WHERE account_id = ?1 ORDER BY path",
        )?;
        let folders = stmt
            .query_map(rusqlite::params![account_id], |row| {
                Ok(Folder {
                    path: row.get(0)?,
                    unread_count: row.get(1)?,
                    noselect: row.get::<_, i32>(2)? != 0,
                    kind: row
                        .get::<_, Option<String>>(3)?
                        .unwrap_or_else(|| "custom".to_string()),
                })
            })?
            .collect::<Result<Vec<_>>>()?;
        Ok(folders)
    }

    /// Page through a folder's messages, newest first.
    ///
    /// Header columns are joined in from `message_headers`; recency sorts on
    /// `h.date_ts` (the parsed timestamp), never `h.date` (the raw header
    /// string, which sorts lexically). A LEFT JOIN keeps a message visible
    /// even if its header row is missing.
    pub fn load_messages(
        &self,
        account_id: i64,
        folder_path: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.uid, h.subject, h.from_addr, h.date_ts, h.in_reply_to, h.message_id,
                    h.references_, m.has_attachments, m.is_read, m.is_starred, m.preview
             FROM messages m
             LEFT JOIN message_headers h
               ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
             WHERE m.account_id = ?1 AND m.folder_path = ?2
             ORDER BY h.date_ts DESC LIMIT ?3 OFFSET ?4",
        )?;
        let messages = stmt
            .query_map(
                rusqlite::params![account_id, folder_path, limit, offset],
                |row| {
                    Ok(Message {
                        uid: row.get(0)?,
                        subject: row.get(1)?,
                        sender: row.get(2)?,
                        // NULL when the LEFT JOIN found no header row; a
                        // headerless message sorts as epoch rather than
                        // failing the whole page load.
                        date: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
                        in_reply_to: row.get(4)?,
                        message_id: row.get(5)?,
                        references: row.get(6)?,
                        has_attachments: row.get::<_, i32>(7)? != 0,
                        is_read: row.get::<_, i32>(8)? != 0,
                        is_starred: row.get::<_, i32>(9)? != 0,
                        preview: row.get(10)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>>>()?;
        Ok(messages)
    }

    pub fn count_messages(&self, account_id: i64, folder_path: &str) -> Result<i64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, folder_path],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// UIDs of messages flagged as having attachments but with no attachment
    /// files stored yet, newest first. Used by the attachment prefetch queue.
    pub fn messages_missing_attachments(
        &self,
        account_id: i64,
        folder_path: &str,
        limit: usize,
    ) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.uid FROM messages m
             JOIN message_headers h
               ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
             WHERE m.account_id = ?1 AND m.folder_path = ?2 AND m.has_attachments = 1
               AND NOT EXISTS (
                   SELECT 1 FROM attachments a
                   WHERE a.account_id = m.account_id
                     AND a.folder_path = m.folder_path
                     AND a.uid = m.uid
               )
             ORDER BY h.date_ts DESC
             LIMIT ?3",
        )?;
        let uids = stmt
            .query_map(
                rusqlite::params![account_id, folder_path, limit as i64],
                |row| row.get::<_, i64>(0),
            )?
            .collect::<Result<Vec<_>>>()?;
        Ok(uids)
    }

    /// Count unread messages for a folder (from DB, not IMAP).
    pub fn count_unread(&self, account_id: i64, folder_path: &str) -> Result<i32> {
        let count: i32 = self.conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND is_read = 0",
            rusqlite::params![account_id, folder_path],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Load cached body HTML for a message. Returns None if not cached.
    pub fn load_body(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<Option<String>> {
        let result = self.conn.query_row(
            "SELECT html FROM bodies WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| row.get::<_, Option<String>>(0),
        );
        match result {
            Ok(html) => Ok(html),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Load the threading headers needed to reply: (message_id, references).
    /// The threading headers of one message, for building a reply.
    ///
    /// Reads from `message_headers`, which is where they live. Returns `None`
    /// when the message has no header row at all, and `None` fields when the
    /// message predates header caching.
    pub fn message_refs(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        let result = self.conn.query_row(
            "SELECT message_id, references_ FROM message_headers
             WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        );
        match result {
            Ok(orm) => Ok(Some(orm)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Save a message's complete header block plus its decoded ENVELOPE
    /// fields.  The row is keyed to `messages` by (account_id, folder_path,
    /// uid) and cascades on delete, so callers must have upserted the message
    /// itself first — a header row with no parent would fail the foreign key.
    /// Empty input is skipped rather than stored as an empty shell.
    ///
    /// This write also (re)builds the message's search-index row, because
    /// `message_headers` owns the indexed header columns (there are no FTS
    /// triggers now — the index is maintained explicitly).
    pub fn save_message_headers(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        headers: &MessageHeaders,
    ) -> Result<()> {
        if headers.is_empty() {
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO message_headers (
                 account_id, folder_path, uid,
                 subject, from_addr, sender_addr, reply_to_addr,
                 to_addr, cc_addr, bcc_addr, date, date_ts,
                 in_reply_to, references_, message_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET
                 subject = ?4, from_addr = ?5, sender_addr = ?6,
                 reply_to_addr = ?7, to_addr = ?8, cc_addr = ?9, bcc_addr = ?10,
                 date = ?11, date_ts = ?12, in_reply_to = ?13, references_ = ?14,
                 message_id = ?15",
            rusqlite::params![
                account_id,
                folder_path,
                uid,
                headers.subject,
                headers.from,
                headers.sender,
                headers.reply_to,
                headers.to,
                headers.cc,
                headers.bcc,
                headers.date,
                headers.date_ts,
                headers.in_reply_to,
                headers.references,
                headers.message_id,
            ],
        )?;
        self.index_message(account_id, folder_path, uid)?;
        Ok(())
    }

    /// Save body HTML to cache.
    pub fn save_body(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        html: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO bodies (account_id, folder_path, uid, html) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET html = ?4",
            rusqlite::params![account_id, folder_path, uid, html],
        )?;
        // The body is the last indexed column; reindex so the new text is
        // searchable without dropping the header columns (contentless FTS5
        // cannot update in place, so `index_message` rebuilds the whole row).
        self.index_message(account_id, folder_path, uid)?;
        Ok(())
    }

    /// Update has_attachments flag from actual body parse (corrects BODYSTRUCTURE guesses).
    pub fn update_has_attachment(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        has: bool,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE messages SET has_attachments = ?1 WHERE account_id = ?2 AND folder_path = ?3 AND uid = ?4",
            rusqlite::params![has as i32, account_id, folder_path, uid],
        )?;
        Ok(())
    }

    /// Check if a body is already cached.
    pub fn has_body(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM bodies WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Replace the stored attachments for a message (delete + insert). Called
    /// whenever a body is re-parsed, so attachment metadata stays in sync.
    pub fn replace_attachments(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        attachments: &[AttachmentRecord],
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM attachments WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
        )?;
        for att in attachments {
            self.conn.execute(
                "INSERT INTO attachments (account_id, folder_path, uid, name, file_path, content_type, size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    account_id,
                    folder_path,
                    uid,
                    att.name,
                    att.file_path,
                    att.content_type,
                    att.size
                ],
            )?;
        }
        Ok(())
    }

    /// Load the attachment metadata for a message.
    pub fn load_attachments(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<Vec<AttachmentRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, file_path, content_type, size FROM attachments
             WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3
             ORDER BY rowid ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![account_id, folder_path, uid], |row| {
            Ok(AttachmentRecord {
                name: row.get(0)?,
                file_path: row.get(1)?,
                content_type: row.get(2)?,
                size: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>>>()
    }

    /// Whether the message's parsed body flagged it as having attachments.
    pub fn message_has_attachments(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<bool> {
        let has: i32 = self.conn.query_row(
            "SELECT has_attachments FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| row.get(0),
        )?;
        Ok(has != 0)
    }

    /// Update the refresh token for an account (when Google rotates it).
    pub fn update_refresh_token(&self, account_id: i64, new_refresh_token: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE accounts SET refresh_token = ?1 WHERE id = ?2",
            rusqlite::params![new_refresh_token, account_id],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Outbox
    // -----------------------------------------------------------------------

    /// Store an unsent message so it can be retried when the daemon reconnects.
    pub fn enqueue_outbox(
        &self,
        account_id: i64,
        to_addr: &str,
        cc_addr: &str,
        bcc_addr: &str,
        reply_to: &str,
        subject: &str,
        body: &str,
    ) -> Result<i64> {
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.conn.execute(
            "INSERT INTO outbox (account_id, to_addr, cc_addr, bcc_addr, reply_to, subject, body, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                account_id,
                to_addr,
                cc_addr,
                bcc_addr,
                reply_to,
                subject,
                body,
                created
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Load every unsent message (oldest first) for the background flusher.
    pub fn load_pending_outbox(&self) -> Result<Vec<OutboxItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, account_id, to_addr, cc_addr, bcc_addr, reply_to, subject, body, attempts
             FROM outbox ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(OutboxItem {
                id: row.get(0)?,
                account_id: row.get(1)?,
                to_addr: row.get(2)?,
                cc_addr: row.get(3)?,
                bcc_addr: row.get(4)?,
                reply_to: row.get(5)?,
                subject: row.get(6)?,
                body: row.get(7)?,
                attempts: row.get(8)?,
            })
        })?;
        rows.collect::<Result<Vec<_>>>()
    }

    /// Increment a queued message's retry counter, returning the new value.
    pub fn increment_outbox_attempts(&self, id: i64) -> Result<i32> {
        self.conn.execute(
            "UPDATE outbox SET attempts = attempts + 1 WHERE id = ?1",
            rusqlite::params![id],
        )?;
        let attempts: i32 = self.conn.query_row(
            "SELECT attempts FROM outbox WHERE id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )?;
        Ok(attempts)
    }

    /// Remove a queued message once it's sent or permanently dropped.
    pub fn delete_outbox(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM outbox WHERE id = ?1", rusqlite::params![id])?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Durable non-send action ledger (flags / move / delete)
    // -----------------------------------------------------------------------

    /// Record a non-send mutation as a durable intent **before** the server is
    /// touched, returning the stored row so the caller can try it immediately.
    ///
    /// `now` is the `created_at`/initial `not_before`: a freshly enqueued action
    /// is due at once. If the process dies before the attempt, the row stays
    /// `pending` and the worker's drain loop picks it up on the next start.
    pub fn enqueue_action(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        kind: ActionKind,
        payload: &str,
        now: i64,
    ) -> Result<PendingAction> {
        self.conn.execute(
            "INSERT INTO pending_actions
                (account_id, folder_path, uid, kind, payload, state, attempts, not_before, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6)",
            rusqlite::params![account_id, folder_path, uid, kind.as_str(), payload, now],
        )?;
        Ok(PendingAction {
            id: self.conn.last_insert_rowid(),
            folder_path: folder_path.to_string(),
            uid,
            kind,
            payload: payload.to_string(),
            attempts: 0,
        })
    }

    /// Atomically claim the oldest due action for an account, marking it
    /// `in_flight` so a later claim cannot return it again. Returns `None` when
    /// nothing is due.
    ///
    /// `Cache` is serialized behind a single mutex, so the select-then-update is
    /// atomic with respect to other claims; the state column still matters for
    /// crash recovery (see [`Cache::reset_in_flight_actions`]).
    pub fn claim_due_action(&self, account_id: i64, now: i64) -> Result<Option<PendingAction>> {
        let action = {
            let mut stmt = self.conn.prepare(
                "SELECT id, account_id, folder_path, uid, kind, payload, attempts, not_before, created_at, last_error
                 FROM pending_actions
                 WHERE account_id = ?1 AND state = 'pending' AND not_before <= ?2
                 ORDER BY id ASC
                 LIMIT 1",
            )?;
            let mut rows = stmt.query_map(rusqlite::params![account_id, now], row_to_action)?;
            match rows.next() {
                Some(row) => Some(row?),
                None => None,
            }
        };
        if let Some(ref action) = action {
            self.conn.execute(
                "UPDATE pending_actions SET state = 'in_flight' WHERE id = ?1",
                rusqlite::params![action.id],
            )?;
        }
        Ok(action)
    }

    /// Drop an action: it either succeeded or is being abandoned.
    pub fn complete_action(&self, id: i64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM pending_actions WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    /// Record a failed attempt: bump the counter, schedule the retry, and return
    /// the action to the `pending` pool. Returns the new attempt count.
    pub fn reschedule_action(&self, id: i64, not_before: i64, last_error: &str) -> Result<i32> {
        self.conn.execute(
            "UPDATE pending_actions
             SET state = 'pending', attempts = attempts + 1, not_before = ?2, last_error = ?3
             WHERE id = ?1",
            rusqlite::params![id, not_before, last_error],
        )?;
        let attempts: i32 = self.conn.query_row(
            "SELECT attempts FROM pending_actions WHERE id = ?1",
            rusqlite::params![id],
            |row| row.get(0),
        )?;
        Ok(attempts)
    }

    /// Return an account's `in_flight` actions to the `pending` pool. Called
    /// when a worker (re)starts so actions interrupted by a crash or restart are
    /// retried instead of being stranded as `in_flight` forever. Returns the
    /// number recovered.
    pub fn reset_in_flight_actions(&self, account_id: i64) -> Result<usize> {
        let recovered = self.conn.execute(
            "UPDATE pending_actions SET state = 'pending'
             WHERE account_id = ?1 AND state = 'in_flight'",
            rusqlite::params![account_id],
        )?;
        Ok(recovered)
    }

    // -----------------------------------------------------------------------
    // Per-message sync-failure ledger (header / body fetch retries)
    // -----------------------------------------------------------------------

    /// Record a failed header/body fetch for one message, bumping its attempt
    /// count and scheduling the next retry. Upserts on the
    /// `(account, folder, uid, stage)` key, so a message that keeps failing
    /// accumulates attempts instead of creating duplicate rows. `retry_at` is
    /// the next eligible time, computed by the caller from the retry policy.
    /// Returns the new attempt count.
    pub fn record_sync_failure(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        stage: SyncStage,
        retry_at: i64,
        last_error: &str,
    ) -> Result<i32> {
        self.conn.execute(
            "INSERT INTO sync_failures
                (account_id, folder_path, uid, stage, attempts, retry_at, last_error)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
             ON CONFLICT(account_id, folder_path, uid, stage)
             DO UPDATE SET attempts = attempts + 1, retry_at = ?5, last_error = ?6",
            rusqlite::params![
                account_id,
                folder_path,
                uid,
                stage.as_str(),
                retry_at,
                last_error
            ],
        )?;
        let attempts: i32 = self.conn.query_row(
            "SELECT attempts FROM sync_failures
             WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3 AND stage = ?4",
            rusqlite::params![account_id, folder_path, uid, stage.as_str()],
            |row| row.get(0),
        )?;
        Ok(attempts)
    }

    /// Claim the most overdue due sync failure for an account, pushing its retry
    /// time out to `push_to` so a retry that fails before it can be rescheduled
    /// is not picked up again immediately. Unlike the action ledger there is no
    /// `in_flight` state: the retry is synchronous in the single worker task, so
    /// a crash mid-retry simply leaves the row to be retried after `push_to`.
    pub fn claim_due_sync_failure(
        &self,
        account_id: i64,
        now: i64,
        push_to: i64,
    ) -> Result<Option<SyncFailure>> {
        let failure = {
            let mut stmt = self.conn.prepare(
                "SELECT folder_path, uid, stage, attempts
                 FROM sync_failures
                 WHERE account_id = ?1 AND retry_at <= ?2
                 ORDER BY retry_at ASC
                 LIMIT 1",
            )?;
            let mut rows =
                stmt.query_map(rusqlite::params![account_id, now], row_to_sync_failure)?;
            match rows.next() {
                Some(row) => Some(row?),
                None => None,
            }
        };
        if let Some(ref failure) = failure {
            self.conn.execute(
                "UPDATE sync_failures SET retry_at = ?5
                 WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3 AND stage = ?4",
                rusqlite::params![
                    account_id,
                    failure.folder_path,
                    failure.uid,
                    failure.stage.as_str(),
                    push_to
                ],
            )?;
        }
        Ok(failure)
    }

    /// Drop a sync-failure row: the retry succeeded, or it is being abandoned.
    pub fn clear_sync_failure(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
        stage: SyncStage,
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sync_failures
             WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3 AND stage = ?4",
            rusqlite::params![account_id, folder_path, uid, stage.as_str()],
        )?;
        Ok(())
    }

    /// Outstanding sync failures for an account (for status and tests).
    pub fn count_sync_failures(&self, account_id: i64) -> Result<i64> {
        self.conn.query_row(
            "SELECT count(*) FROM sync_failures WHERE account_id = ?1",
            rusqlite::params![account_id],
            |row| row.get(0),
        )
    }

    /// Load a single account by id (None if it no longer exists).
    pub fn load_account(&self, account_id: i64) -> Result<Option<Account>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, email, display_name, provider, auth_type, imap_host, imap_port, imap_security, smtp_host, smtp_port, smtp_security, username, refresh_token, enabled FROM accounts WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![account_id], |row| {
            Ok(Account {
                id: row.get(0)?,
                email: row.get(1)?,
                display_name: row.get(2)?,
                provider: row.get(3)?,
                auth_type: row.get(4)?,
                imap_host: row.get(5)?,
                imap_port: row.get(6)?,
                imap_security: row.get(7)?,
                smtp_host: row.get(8)?,
                smtp_port: row.get(9)?,
                smtp_security: row.get(10)?,
                username: row.get(11)?,
                refresh_token: row.get(12)?,
                enabled: row.get(13)?,
            })
        })?;
        match rows.next() {
            Some(Ok(acc)) => Ok(Some(acc)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Address book (autocomplete)
    // -----------------------------------------------------------------------

    /// Upsert a sender address (increment frequency on conflict).
    pub fn upsert_address(&self, email: &str, display_name: &str) -> Result<()> {
        if email.is_empty() {
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO addresses (email, display_name, frequency) VALUES (?1, ?2, 1)
             ON CONFLICT(email) DO UPDATE SET
               display_name = CASE WHEN ?2 != '' THEN ?2 ELSE display_name END,
               frequency = frequency + 1",
            rusqlite::params![email.to_lowercase(), display_name],
        )?;
        Ok(())
    }

    /// Search addresses by prefix (email or display name), most used first.
    pub fn search_addresses(&self, query: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let pattern = format!("%{}%", query.to_lowercase());
        let mut stmt = self.conn.prepare(
            "SELECT email, display_name FROM addresses
             WHERE email LIKE ?1 OR display_name LIKE ?1
             ORDER BY frequency DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![pattern, limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>>>()
    }

    /// Check if an email address is in the address book.
    pub fn is_known_address(&self, email: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM addresses WHERE email = ?1",
            rusqlite::params![email.to_lowercase()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // -----------------------------------------------------------------------
    // Sender policies (allow/block)
    // -----------------------------------------------------------------------

    /// Set a sender policy: block or allow remote content for this sender.
    pub fn set_sender_policy(
        &self,
        sender_email: &str,
        block_sender: bool,
        allow_remote: bool,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sender_policies (sender_email, block_sender, allow_remote) VALUES (?1, ?2, ?3)
             ON CONFLICT(sender_email) DO UPDATE SET block_sender = ?2, allow_remote = ?3",
            rusqlite::params![sender_email.to_lowercase(), block_sender as i32, allow_remote as i32],
        )?;
        Ok(())
    }

    /// Remove a sender policy entry.
    pub fn remove_sender_policy(&self, sender_email: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sender_policies WHERE sender_email = ?1",
            rusqlite::params![sender_email.to_lowercase()],
        )?;
        Ok(())
    }

    /// Check if a sender is blocked.
    pub fn is_sender_blocked(&self, sender_email: &str) -> Result<bool> {
        let blocked: i32 = self
            .conn
            .query_row(
                "SELECT block_sender FROM sender_policies WHERE sender_email = ?1",
                rusqlite::params![sender_email.to_lowercase()],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(blocked != 0)
    }

    /// Load all sender policies.
    pub fn load_sender_policies(&self) -> Result<Vec<(String, bool, bool)>> {
        let mut stmt = self.conn.prepare(
            "SELECT sender_email, block_sender, allow_remote FROM sender_policies ORDER BY sender_email"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)? != 0,
                row.get::<_, i32>(2)? != 0,
            ))
        })?;
        rows.collect::<Result<Vec<_>>>()
    }
}

/// Map a `pending_actions` row to a [`PendingAction`]. An unknown `kind` is an
/// error rather than a guess: silently misreading a destructive intent is worse
/// than surfacing it.
fn row_to_action(row: &rusqlite::Row) -> Result<PendingAction> {
    let kind_raw: String = row.get(4)?;
    let kind = ActionKind::from_str(&kind_raw).ok_or_else(|| {
        rusqlite::Error::InvalidColumnName(format!("unknown action kind: {kind_raw}"))
    })?;
    Ok(PendingAction {
        id: row.get(0)?,
        folder_path: row.get(2)?,
        uid: row.get(3)?,
        kind,
        payload: row.get(5)?,
        attempts: row.get(6)?,
    })
}

/// Map a `sync_failures` row to a [`SyncFailure`]. An unknown `stage` is an
/// error rather than a guess, matching [`row_to_action`].
fn row_to_sync_failure(row: &rusqlite::Row) -> Result<SyncFailure> {
    let stage_raw: String = row.get(2)?;
    let stage = SyncStage::from_str(&stage_raw).ok_or_else(|| {
        rusqlite::Error::InvalidColumnName(format!("unknown sync stage: {stage_raw}"))
    })?;
    Ok(SyncFailure {
        folder_path: row.get(0)?,
        uid: row.get(1)?,
        stage,
        attempts: row.get(3)?,
    })
}

#[cfg(test)]
#[path = "cache.test.rs"]
mod tests;
