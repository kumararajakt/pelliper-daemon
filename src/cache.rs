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

        Ok(())
    }

    pub fn load_accounts(&self) -> Result<Vec<Account>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, email, display_name, provider, auth_type, imap_host, imap_port, imap_security, smtp_host, smtp_port, smtp_security, username, access_token, refresh_token, enabled FROM accounts",
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
                    access_token: row.get(12)?,
                    refresh_token: row.get(13)?,
                    enabled: row.get(14)?,
                })
            })?
            .collect::<Result<Vec<_>>>()?;

        Ok(accounts)
    }

    pub fn insert_account(&mut self, account: &Account) -> Result<i64> {
        self.conn.execute(
            "INSERT OR REPLACE INTO accounts (email, display_name, provider, auth_type, imap_host, imap_port, imap_security, smtp_host, smtp_port, smtp_security, username, access_token, refresh_token, enabled) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
                account.access_token,
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

    pub fn account_exists(&self, email: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts WHERE email = ?1",
            rusqlite::params![email],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn update_access_token(&mut self, account_id: i64, token: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE accounts SET access_token = ?1 WHERE id = ?2",
            rusqlite::params![token, account_id],
        )?;
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
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (account_id, folder_path, uid, subject, sender, date, is_read, is_starred, has_attachments, preview)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(account_id, folder_path, uid) DO UPDATE SET
                subject = ?4, sender = ?5, date = ?6, is_read = ?7, is_starred = ?8, has_attachments = ?9, preview = ?10",
            rusqlite::params![account_id, folder_path, uid, subject, sender, date, is_read as i32, is_starred as i32, has_attachments as i32, preview],
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
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub enabled: bool,
}
