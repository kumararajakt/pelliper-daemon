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
                email TEXT NOT NULL,
                display_name TEXT,
                provider TEXT,
                imap_host TEXT,
                imap_port INTEGER,
                smtp_host TEXT,
                smtp_port INTEGER,
                enabled INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS folders (
                account_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                unread_count INTEGER DEFAULT 0,
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
        Ok(())
    }

    pub fn load_accounts(&self) -> Result<Vec<Account>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, email, display_name, provider, imap_host, imap_port, smtp_host, smtp_port, enabled FROM accounts",
        )?;

        let accounts = stmt
            .query_map([], |row| {
                Ok(Account {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    display_name: row.get(2)?,
                    provider: row.get(3)?,
                    imap_host: row.get(4)?,
                    imap_port: row.get(5)?,
                    smtp_host: row.get(6)?,
                    smtp_port: row.get(7)?,
                    enabled: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>>>()?;

        Ok(accounts)
    }

    pub fn insert_account(&self, account: &Account) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO accounts (email, display_name, provider, imap_host, imap_port, smtp_host, smtp_port, enabled) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                account.email,
                account.display_name,
                account.provider,
                account.imap_host,
                account.imap_port,
                account.smtp_host,
                account.smtp_port,
                account.enabled,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }
}

#[derive(Debug, Clone)]
pub struct Account {
    pub id: i64,
    pub email: String,
    pub display_name: Option<String>,
    pub provider: Option<String>,
    pub imap_host: Option<String>,
    pub imap_port: Option<i64>,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<i64>,
    pub enabled: bool,
}
