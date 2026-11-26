pub mod repo;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use rusqlite::Connection;

const MIGRATION_001: &str = include_str!("migrations/001_init.sql");

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Opens SQLite in WAL mode and applies initial schema migrations.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;
        conn.execute_batch(MIGRATION_001)?;
        let _ = conn.execute("ALTER TABLE accounts ADD COLUMN signature TEXT NOT NULL DEFAULT ''", []);
        Self::repair_rfc2047_headers(&conn);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn repair_rfc2047_headers(conn: &Connection) {
        if let Ok(mut stmt) = conn.prepare("SELECT id, subject, from_json FROM messages WHERE subject LIKE '%=?%' OR from_json LIKE '%=?%'") {
            let rows: Vec<(i64, String, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .ok()
                .into_iter()
                .flatten()
                .flatten()
                .collect();
            for (id, subj, from_j) in rows {
                let fixed_subj = crate::mime::decode_rfc2047(&subj);
                let fixed_from = crate::mime::decode_rfc2047(&from_j);
                let _ = conn.execute(
                    "UPDATE messages SET subject = ?1, from_json = ?2 WHERE id = ?3",
                    rusqlite::params![fixed_subj, fixed_from, id],
                );
            }
        }
    }

    pub fn account_count(&self) -> Result<i64> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM accounts", [], |row| row.get(0))?;
        Ok(count)
    }

    pub fn checkpoint_wal(&self) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_in_memory_and_runs_migrations() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(MIGRATION_001).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
