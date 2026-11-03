use crate::types::{
    Account, ActionKind, AttachmentRecord, Folder, Message, MessageHeaders, OutboxItem,
    PendingAction, SearchResult, SyncFailure, SyncStage,
};
use rusqlite::{Connection, OptionalExtension, Result};
use std::collections::HashSet;
use std::path::PathBuf;

pub struct Cache {
    conn: Connection,
}

const INDEX_FTS_SQL: &str = "\
INSERT INTO messages_fts(rowid, subject, sender, preview, message_id, body)
SELECT m.rowid, h.subject, h.from_addr, m.preview, h.message_id, b.html
FROM messages m
JOIN message_headers h
  ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
LEFT JOIN bodies b
  ON b.account_id = m.account_id AND b.folder_path = m.folder_path AND b.uid = m.uid
WHERE m.account_id = ?1 AND m.folder_path = ?2 AND m.uid = ?3";

const BACKFILL_FTS_SQL: &str = "\
INSERT INTO messages_fts(rowid, subject, sender, preview, message_id, body)
SELECT m.rowid, h.subject, h.from_addr, m.preview, h.message_id, b.html
FROM messages m
JOIN message_headers h
  ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
LEFT JOIN bodies b
  ON b.account_id = m.account_id AND b.folder_path = m.folder_path AND b.uid = m.uid";

const DEINDEX_MESSAGE_SQL: &str = "\
DELETE FROM messages_fts WHERE rowid IN
    (SELECT rowid FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3)";

const DEINDEX_FOLDER_SQL: &str = "\
DELETE FROM messages_fts WHERE rowid IN
    (SELECT rowid FROM messages WHERE account_id = ?1 AND folder_path = ?2)";

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

    pub fn migrate(&self) -> Result<()> {
        crate::migrations::run(&self.conn)
    }

    pub fn fts_init(&self) -> Result<()> {
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

    fn backfill_fts(&self) -> Result<()> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM messages_fts", [], |r| r.get(0))?;
        if count == 0 {
            self.conn.execute_batch(BACKFILL_FTS_SQL)?;
        }
        Ok(())
    }

    fn index_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.deindex_message(account_id, folder_path, uid)?;
        self.conn.execute(
            INDEX_FTS_SQL,
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    fn deindex_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.conn.execute(
            DEINDEX_MESSAGE_SQL,
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    fn deindex_folder(&self, account_id: i64, folder_path: &str) -> Result<()> {
        self.conn.execute(
            DEINDEX_FOLDER_SQL,
            rusqlite::params![account_id, folder_path],
        )?;
        Ok(())
    }

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

    pub fn remove_account(&self, account_id: i64) -> Result<()> {
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

    pub fn update_message_flags_bulk(
        &mut self,
        account_id: i64,
        folder_path: &str,
        flags: &[(i64, bool, bool)],
    ) -> Result<Vec<(i64, bool, bool)>> {
        if flags.is_empty() {
            return Ok(Vec::new());
        }
        let tx = self.conn.transaction()?;
        let mut updated = Vec::new();
        {
            let mut stmt = tx.prepare(
                "UPDATE messages SET is_read = ?1, is_starred = ?2
                 WHERE account_id = ?3 AND folder_path = ?4 AND uid = ?5
                   AND (is_read != ?1 OR is_starred != ?2)",
            )?;
            for &(uid, is_read, is_starred) in flags {
                let count = stmt.execute(rusqlite::params![
                    is_read as i32,
                    is_starred as i32,
                    account_id,
                    folder_path,
                    uid,
                ])?;
                if count > 0 {
                    updated.push((uid, is_read, is_starred));
                }
            }
        }
        tx.commit()?;
        Ok(updated)
    }

    pub fn message_flags(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<Option<(bool, bool)>> {
        let result = self
            .conn
            .query_row(
                "SELECT is_read, is_starred FROM messages
                 WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
                rusqlite::params![account_id, folder_path, uid],
                |row| Ok((row.get::<_, i32>(0)? != 0, row.get::<_, i32>(1)? != 0)),
            )
            .optional()?;
        Ok(result)
    }

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

        self.index_message(account_id, to_folder, uid)?;
        self.deindex_message(account_id, from_folder, uid)?;
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, from_folder, uid],
        )?;
        Ok(())
    }

    pub fn delete_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
        self.deindex_message(account_id, folder_path, uid)?;
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
        )?;
        Ok(())
    }

    pub fn delete_folder(&self, account_id: i64, path: &str) -> Result<()> {
        self.clear_folder_messages(account_id, path)?;
        self.conn.execute(
            "DELETE FROM pending_actions WHERE account_id = ?1 AND folder_path = ?2",
            rusqlite::params![account_id, path],
        )?;
        self.conn.execute(
            "DELETE FROM folders WHERE account_id = ?1 AND path = ?2",
            rusqlite::params![account_id, path],
        )?;
        Ok(())
    }

    pub fn rename_folder(&mut self, account_id: i64, old_path: &str, new_path: &str) -> Result<()> {
        let (kind, _) = crate::imap::classify_with_source(new_path, &[]);
        let tx = self.conn.transaction()?;
        tx.execute_batch("PRAGMA defer_foreign_keys = ON;")?;
        let updated = tx.execute(
            "UPDATE folders SET path = ?1, kind = ?2 WHERE account_id = ?3 AND path = ?4",
            rusqlite::params![new_path, kind.as_str(), account_id, old_path],
        )?;
        if updated == 0 {
            tx.execute(
                "INSERT INTO folders (account_id, path, unread_count, noselect, kind) VALUES (?1, ?2, 0, 0, ?3)
                 ON CONFLICT(account_id, path) DO UPDATE SET kind = ?3",
                rusqlite::params![account_id, new_path, kind.as_str()],
            )?;
        }
        tx.execute(
            "UPDATE messages SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE message_headers SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE bodies SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE attachments SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE sync_failures SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE pending_actions SET folder_path = ?1 WHERE account_id = ?2 AND folder_path = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.execute(
            "UPDATE pending_actions
             SET payload = json_set(payload, '$.dest', ?1)
             WHERE account_id = ?2 AND kind = 'move' AND json_extract(payload, '$.dest') = ?3",
            rusqlite::params![new_path, account_id, old_path],
        )?;
        tx.commit()?;
        Ok(())
    }

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

    pub fn cached_uids_in_range(
        &self,
        account_id: i64,
        folder_path: &str,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT uid FROM messages
             WHERE account_id = ?1 AND folder_path = ?2 AND uid >= ?3 AND uid <= ?4
             ORDER BY uid ASC",
        )?;
        let uids = stmt
            .query_map(rusqlite::params![account_id, folder_path, lo, hi], |row| {
                row.get(0)
            })?
            .collect::<Result<Vec<i64>, _>>()?;
        Ok(uids)
    }

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
            tx.execute(
                DEINDEX_MESSAGE_SQL,
                rusqlite::params![account_id, folder_path, uid],
            )?;
            tx.execute(
                "DELETE FROM attachments WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
                rusqlite::params![account_id, folder_path, uid],
            )?;
            tx.execute(
                "DELETE FROM sync_failures WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
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

    pub fn search_messages(&self, account_id: i64, query: &str) -> Result<Vec<SearchResult>> {
        let sanitized = sanitize_fts_query(query);
        if sanitized.is_empty() {
            return Ok(Vec::new());
        }

        let map_row = |row: &rusqlite::Row| {
            Ok(SearchResult {
                account_id: row.get(0)?,
                folder_path: row.get(1)?,
                uid: row.get(2)?,
                subject: row.get(3)?,
                sender: row.get(4)?,
                date: row.get::<_, Option<i64>>(5)?.unwrap_or(0),
                in_reply_to: row.get(6)?,
                message_id: row.get(7)?,
                references: row.get(8)?,
                has_attachments: row.get::<_, i32>(9)? != 0,
                is_read: row.get::<_, i32>(10)? != 0,
                is_starred: row.get::<_, i32>(11)? != 0,
                preview: row.get(12)?,
            })
        };

        if account_id > 0 {
            let mut stmt = self.conn.prepare(
                "SELECT m.account_id, m.folder_path, m.uid, h.subject, h.from_addr, h.date_ts,
                        h.in_reply_to, h.message_id, h.references_, m.has_attachments,
                        m.is_read, m.is_starred, m.preview
                 FROM messages_fts f
                 JOIN messages m ON m.rowid = f.rowid
                 LEFT JOIN message_headers h
                   ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
                 WHERE messages_fts MATCH ?1 AND m.account_id = ?2
                 ORDER BY bm25(messages_fts), h.date_ts DESC
                 LIMIT 100",
            )?;
            let results = stmt
                .query_map(rusqlite::params![sanitized, account_id], map_row)?
                .collect::<Result<Vec<_>>>()?;
            Ok(results)
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT m.account_id, m.folder_path, m.uid, h.subject, h.from_addr, h.date_ts,
                        h.in_reply_to, h.message_id, h.references_, m.has_attachments,
                        m.is_read, m.is_starred, m.preview
                 FROM messages_fts f
                 JOIN messages m ON m.rowid = f.rowid
                 LEFT JOIN message_headers h
                   ON h.account_id = m.account_id AND h.folder_path = m.folder_path AND h.uid = m.uid
                 WHERE messages_fts MATCH ?1
                 ORDER BY bm25(messages_fts), h.date_ts DESC
                 LIMIT 100",
            )?;
            let results = stmt
                .query_map(rusqlite::params![sanitized], map_row)?
                .collect::<Result<Vec<_>>>()?;
            Ok(results)
        }
    }

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

    pub fn count_unread(&self, account_id: i64, folder_path: &str) -> Result<i32> {
        let count: i32 = self.conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND is_read = 0",
            rusqlite::params![account_id, folder_path],
            |row| row.get(0),
        )?;
        Ok(count)
    }

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

        self.index_message(account_id, folder_path, uid)?;
        Ok(())
    }

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

    pub fn has_body(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM bodies WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

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

    pub fn update_refresh_token(&self, account_id: i64, new_refresh_token: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE accounts SET refresh_token = ?1 WHERE id = ?2",
            rusqlite::params![new_refresh_token, account_id],
        )?;
        Ok(())
    }

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

    pub fn delete_outbox(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM outbox WHERE id = ?1", rusqlite::params![id])?;
        Ok(())
    }

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

    pub fn complete_action(&self, id: i64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM pending_actions WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

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

    pub fn reset_in_flight_actions(&self, account_id: i64) -> Result<usize> {
        let recovered = self.conn.execute(
            "UPDATE pending_actions SET state = 'pending'
             WHERE account_id = ?1 AND state = 'in_flight'",
            rusqlite::params![account_id],
        )?;
        Ok(recovered)
    }

    pub fn pending_action_uids(&self, account_id: i64, folder_path: &str) -> Result<HashSet<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT uid FROM pending_actions
             WHERE account_id = ?1 AND folder_path = ?2",
        )?;
        let uids = stmt
            .query_map(rusqlite::params![account_id, folder_path], |row| {
                row.get::<_, i64>(0)
            })?
            .collect::<Result<HashSet<i64>, _>>()?;
        Ok(uids)
    }

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

    pub fn count_sync_failures(&self, account_id: i64) -> Result<i64> {
        self.conn.query_row(
            "SELECT count(*) FROM sync_failures WHERE account_id = ?1",
            rusqlite::params![account_id],
            |row| row.get(0),
        )
    }

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

    pub fn is_known_address(&self, email: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM addresses WHERE email = ?1",
            rusqlite::params![email.to_lowercase()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

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

    pub fn remove_sender_policy(&self, sender_email: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sender_policies WHERE sender_email = ?1",
            rusqlite::params![sender_email.to_lowercase()],
        )?;
        Ok(())
    }

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

pub fn sanitize_fts_query(query: &str) -> String {
    let tokens: Vec<String> = query
        .split_whitespace()
        .filter_map(|t| {
            let stripped = t.trim_matches(|c: char| c == '"' || c == '*');
            if stripped.is_empty() {
                None
            } else {
                let escaped = stripped.replace('"', "\"\"");
                Some(format!("\"{escaped}\"*"))
            }
        })
        .collect();
    tokens.join(" AND ")
}

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
