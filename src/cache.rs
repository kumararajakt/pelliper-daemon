use rusqlite::{Connection, Result};
use std::path::PathBuf;

pub struct Cache {
    conn: Connection,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Folder {
    pub path: String,
    pub unread_count: i32,
    pub noselect: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Message {
    pub uid: i64,
    pub subject: Option<String>,
    pub sender: Option<String>,
    pub date: i64,
    pub in_reply_to: Option<String>,
    pub message_id: Option<String>,
    pub references: Option<String>,
    pub has_attachments: bool,
    pub is_read: bool,
    pub is_starred: bool,
    pub preview: Option<String>,
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
                content_type TEXT
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

        // Migration: add noselect column if missing
        let _ = self.conn.execute(
            "ALTER TABLE folders ADD COLUMN noselect INTEGER DEFAULT 0",
            [],
        );

        // Migration: add threading columns if missing
        let _ = self.conn.execute(
            "ALTER TABLE messages ADD COLUMN message_id TEXT",
            [],
        );
        let _ = self.conn.execute(
            "ALTER TABLE messages ADD COLUMN references_ TEXT",
            [],
        );

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

        let count: i64 = self.conn.query_row("SELECT count(*) FROM messages_fts", [], |r| r.get(0))?;
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
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts",
            [],
            |row| row.get(0),
        )?;
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
        self.conn.execute("DELETE FROM attachments WHERE account_id = ?1", rusqlite::params![account_id])?;
        self.conn.execute("DELETE FROM bodies WHERE account_id = ?1", rusqlite::params![account_id])?;
        self.conn.execute("DELETE FROM messages WHERE account_id = ?1", rusqlite::params![account_id])?;
        self.conn.execute("DELETE FROM folders WHERE account_id = ?1", rusqlite::params![account_id])?;
        self.conn.execute("DELETE FROM accounts WHERE id = ?1", rusqlite::params![account_id])?;
        tracing::info!("Removed account {} and all associated data", account_id);
        Ok(())
    }

    pub fn upsert_folder(&self, account_id: i64, path: &str, unread_count: i32, noselect: bool) -> Result<()> {
        self.conn.execute(
            "INSERT INTO folders (account_id, path, unread_count, noselect) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id, path) DO UPDATE SET unread_count = ?3, noselect = ?4",
            rusqlite::params![account_id, path, unread_count, noselect as i32],
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

    pub fn delete_stale_messages(&self, account_id: i64, folder_path: &str, valid_uids: &[i64]) -> Result<()> {
        if valid_uids.is_empty() {
            return Ok(());
        }
        let placeholders: Vec<String> = valid_uids.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "DELETE FROM messages WHERE account_id = ?1 AND folder_path = ?2 AND uid NOT IN ({})",
            placeholders.join(", ")
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
            Box::new(account_id),
            Box::new(folder_path.to_string()),
        ];
        for uid in valid_uids {
            params.push(Box::new(*uid));
        }
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        self.conn.execute(&sql, param_refs.as_slice())?;
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

    pub fn load_folders(&self, account_id: i64) -> Result<Vec<Folder>> {
        let mut stmt = self.conn.prepare(
            "SELECT path, unread_count, noselect FROM folders WHERE account_id = ?1 ORDER BY path",
        )?;
        let folders = stmt
            .query_map(rusqlite::params![account_id], |row| {
                Ok(Folder {
                    path: row.get(0)?,
                    unread_count: row.get(1)?,
                    noselect: row.get::<_, i32>(2)? != 0,
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
            .query_map(rusqlite::params![account_id, folder_path, limit, offset], |row| {
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
            })?
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
        let mut stmt = self.conn.prepare(
            "SELECT uid FROM messages WHERE account_id = ?1 AND folder_path = ?2",
        )?;
        let uids = stmt
            .query_map(rusqlite::params![account_id, folder_path], |row| {
                row.get::<_, i64>(0)
            })?
            .collect::<Result<Vec<_>>>()?;
        Ok(uids)
    }

    /// Load the N most recent UIDs for a folder (sorted newest first).
    pub fn load_recent_uids(&self, account_id: i64, folder_path: &str, limit: usize) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT uid FROM messages WHERE account_id = ?1 AND folder_path = ?2 ORDER BY date DESC LIMIT ?3",
        )?;
        let uids = stmt
            .query_map(rusqlite::params![account_id, folder_path, limit as i64], |row| {
                row.get::<_, i64>(0)
            })?
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
    pub fn load_body(&self, account_id: i64, folder_path: &str, uid: i64) -> Result<Option<String>> {
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

    /// Save body HTML to cache.
    pub fn save_body(&self, account_id: i64, folder_path: &str, uid: i64, html: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO bodies (account_id, folder_path, uid, html) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET html = ?4",
            rusqlite::params![account_id, folder_path, uid, html],
        )?;
        Ok(())
    }

    /// Update has_attachments flag from actual body parse (corrects BODYSTRUCTURE guesses).
    pub fn update_has_attachment(&self, account_id: i64, folder_path: &str, uid: i64, has: bool) -> Result<()> {
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
}

#[derive(Debug, Clone)]
pub struct Account {
    pub id: i64,
    pub email: String,
    pub display_name: Option<String>,
    pub provider: Option<String>,
    pub auth_type: String,
    pub imap_host: Option<String>,
    pub imap_port: Option<i64>,
    pub imap_security: Option<String>,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<i64>,
    pub smtp_security: Option<String>,
    pub username: Option<String>,
    pub refresh_token: Option<String>,
    pub enabled: bool,
}
