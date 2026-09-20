use crate::types::{Account, AttachmentRecord, Folder, Message, OutboxItem};
use rusqlite::{Connection, Result};
use std::path::PathBuf;

pub struct Cache {
    conn: Connection,
}

impl Cache {
    pub fn open(data_dir: &PathBuf) -> Result<Self> {
        std::fs::create_dir_all(data_dir).ok();
        let db_path = data_dir.join("cache.db");
        let conn = Connection::open(db_path)?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        conn.execute_batch("PRAGMA user_version=1;")?;

        Ok(Self { conn })
    }

    pub fn create_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            "
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

            CREATE TABLE IF NOT EXISTS folders (
                account_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                unread_count INTEGER DEFAULT 0,
                noselect INTEGER DEFAULT 0,
                kind TEXT NOT NULL DEFAULT 'custom',
                PRIMARY KEY (account_id, path),
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS messages (
                account_id INTEGER NOT NULL,
                folder_path TEXT NOT NULL,
                uid INTEGER NOT NULL,
                subject TEXT,
                sender TEXT,
                date INTEGER,
                in_reply_to TEXT,
                message_id TEXT,
                references_ TEXT,
                has_attachments INTEGER DEFAULT 0,
                is_read INTEGER DEFAULT 0,
                is_starred INTEGER DEFAULT 0,
                preview TEXT,
                PRIMARY KEY (account_id, folder_path, uid)
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
            ",
        )?;

        Ok(())
    }

    /// Create the full-text search index (FTS5) and keep it in sync with the
    /// messages/bodies tables via triggers. Backfills existing rows on first run.
    pub fn fts_init(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                subject, sender, preview, message_id, body,
                account_id UNINDEXED, folder_path UNINDEXED, uid UNINDEXED
            );

            CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, subject, sender, preview, message_id,
                                         account_id, folder_path, uid)
                VALUES (new.rowid, new.subject, new.sender, new.preview, new.message_id,
                        new.account_id, new.folder_path, new.uid);
            END;

            CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
                DELETE FROM messages_fts WHERE rowid = old.rowid;
            END;

            CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
                DELETE FROM messages_fts WHERE rowid = old.rowid;
                INSERT INTO messages_fts(rowid, subject, sender, preview, message_id,
                                         account_id, folder_path, uid)
                VALUES (new.rowid, new.subject, new.sender, new.preview, new.message_id,
                        new.account_id, new.folder_path, new.uid);
            END;

            CREATE TRIGGER IF NOT EXISTS bodies_fts_ai AFTER INSERT ON bodies BEGIN
                INSERT INTO messages_fts(body, rowid)
                VALUES (new.html, (SELECT rowid FROM messages
                        WHERE account_id = new.account_id
                          AND folder_path = new.folder_path
                          AND uid = new.uid));
            END;

            CREATE TRIGGER IF NOT EXISTS bodies_fts_ad AFTER DELETE ON bodies BEGIN
                UPDATE messages_fts
                SET body = NULL
                WHERE rowid = (SELECT rowid FROM messages
                        WHERE account_id = old.account_id
                          AND folder_path = old.folder_path
                          AND uid = old.uid);
            END;
            ",
        )?;

        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM messages_fts", [], |r| r.get(0))?;
        if count == 0 {
            self.conn.execute_batch(
                "
                INSERT INTO messages_fts(rowid, subject, sender, preview, message_id,
                                         account_id, folder_path, uid)
                SELECT rowid, subject, sender, preview, message_id, account_id, folder_path, uid
                FROM messages;

                UPDATE messages_fts
                SET body = (
                    SELECT b.html FROM bodies b
                    JOIN messages m ON m.rowid = messages_fts.rowid
                    WHERE b.account_id = m.account_id
                      AND b.folder_path = m.folder_path
                      AND b.uid = m.uid
                );
                ",
            )?;
        }

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

    pub fn has_cached_messages(&self, account_id: i64) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE account_id = ?1",
            rusqlite::params![account_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
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
        // Foreign keys with ON DELETE CASCADE handle messages/bodies/folders
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
        subject: &str,
        sender: &str,
        date: i64,
        is_read: bool,
        is_starred: bool,
        has_attachments: bool,
        preview: &str,
        message_id: &str,
        references: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (account_id, folder_path, uid, subject, sender, date, is_read, is_starred, has_attachments, preview, message_id, references_)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET
                subject = ?4, sender = ?5, date = ?6, is_read = ?7, is_starred = ?8, has_attachments = ?9, preview = ?10, message_id = ?11, references_ = ?12",
            rusqlite::params![account_id, folder_path, uid, subject, sender, date, is_read as i32, is_starred as i32, has_attachments as i32, preview, message_id, references],
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

    /// Move a message (and its cached body) from one folder to another.
    pub fn move_message(
        &self,
        account_id: i64,
        from_folder: &str,
        uid: i64,
        to_folder: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO messages
                (account_id, folder_path, uid, subject, sender, date, in_reply_to,
                 message_id, references_, has_attachments, is_read, is_starred, preview)
             SELECT account_id, ?2, uid, subject, sender, date, in_reply_to,
                    message_id, references_, has_attachments, is_read, is_starred, preview
             FROM messages
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
        self.conn.execute(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, from_folder, uid],
        )?;
        Ok(())
    }

    /// Remove a message row (and its cascade-deleted body).
    pub fn delete_message(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<()> {
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
            "SELECT uid, subject, sender, date, in_reply_to, message_id, references_, has_attachments, is_read, is_starred, preview
             FROM messages WHERE account_id = ?1 AND folder_path = ?2
             ORDER BY date DESC LIMIT ?3 OFFSET ?4",
        )?;
        let messages = stmt
            .query_map(
                rusqlite::params![account_id, folder_path, limit, offset],
                |row| {
                    Ok(Message {
                        uid: row.get(0)?,
                        subject: row.get(1)?,
                        sender: row.get(2)?,
                        date: row.get(3)?,
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

    /// Load all cached UIDs for a folder (for backfill comparison).
    pub fn load_cached_uids(&self, account_id: i64, folder_path: &str) -> Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT uid FROM messages WHERE account_id = ?1 AND folder_path = ?2")?;
        let uids = stmt
            .query_map(rusqlite::params![account_id, folder_path], |row| {
                row.get::<_, i64>(0)
            })?
            .collect::<Result<Vec<_>>>()?;
        Ok(uids)
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
             WHERE m.account_id = ?1 AND m.folder_path = ?2 AND m.has_attachments = 1
               AND NOT EXISTS (
                   SELECT 1 FROM attachments a
                   WHERE a.account_id = m.account_id
                     AND a.folder_path = m.folder_path
                     AND a.uid = m.uid
               )
             ORDER BY m.date DESC
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
    pub fn message_refs(
        &self,
        account_id: i64,
        folder_path: &str,
        uid: i64,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        let result = self.conn.query_row(
            "SELECT message_id, references_ FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid = ?3",
            rusqlite::params![account_id, folder_path, uid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );
        match result {
            Ok(orm) => Ok(Some(orm)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
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
