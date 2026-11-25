use anyhow::Result;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::db::Database;
use crate::search::ParsedSearchQuery;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountRow {
    pub id: i64,
    pub email: String,
    pub display_name: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub imap_tls: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_tls: String,
    pub auth_type: String,
    pub oauth_provider: Option<String>,
    pub sync_interval_secs: i64,
    pub enabled: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderRow {
    pub id: i64,
    pub account_id: i64,
    pub name: String,
    pub full_path: String,
    pub delimiter: String,
    pub role: String,
    pub priority: i32,
    pub uid_validity: Option<i64>,
    pub uid_next: i64,
    pub highest_modseq: Option<i64>,
    pub total_count: i64,
    pub unread_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSummary {
    pub id: i64,
    pub account_id: i64,
    pub folder_id: i64,
    pub uid: i64,
    pub thread_id: String,
    pub subject: String,
    pub from_json: String,
    pub date_ts: i64,
    pub flags_bitmask: i64,
    pub snippet: String,
    pub has_attachments: bool,
    pub body_synced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOpRow {
    pub id: i64,
    pub account_id: i64,
    pub folder_id: Option<i64>,
    pub message_id: Option<i64>,
    pub uid: Option<i64>,
    pub op_type: String,
    pub payload_json: String,
}

impl Database {
    pub fn list_accounts(&self) -> Result<Vec<AccountRow>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, email, display_name, imap_host, imap_port, imap_tls,
                    smtp_host, smtp_port, smtp_tls, auth_type, oauth_provider,
                    sync_interval_secs, enabled, last_error
             FROM accounts ORDER BY id ASC",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(AccountRow {
                    id: r.get(0)?,
                    email: r.get(1)?,
                    display_name: r.get(2)?,
                    imap_host: r.get(3)?,
                    imap_port: r.get::<_, i64>(4)? as u16,
                    imap_tls: r.get(5)?,
                    smtp_host: r.get(6)?,
                    smtp_port: r.get::<_, i64>(7)? as u16,
                    smtp_tls: r.get(8)?,
                    auth_type: r.get(9)?,
                    oauth_provider: r.get(10)?,
                    sync_interval_secs: r.get(11)?,
                    enabled: r.get::<_, i64>(12)? != 0,
                    last_error: r.get(13)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_account(&self, account_id: i64) -> Result<Option<AccountRow>> {
        let accounts = self.list_accounts()?;
        Ok(accounts.into_iter().find(|a| a.id == account_id))
    }

    pub fn insert_account(&self, acc: &AccountRow) -> Result<i64> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO accounts (email, display_name, imap_host, imap_port, imap_tls,
                                   smtp_host, smtp_port, smtp_tls, auth_type, oauth_provider,
                                   sync_interval_secs, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                acc.email,
                acc.display_name,
                acc.imap_host,
                acc.imap_port as i64,
                acc.imap_tls,
                acc.smtp_host,
                acc.smtp_port as i64,
                acc.smtp_tls,
                acc.auth_type,
                acc.oauth_provider,
                acc.sync_interval_secs,
                if acc.enabled { 1 } else { 0 },
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn delete_account(&self, account_id: i64) -> Result<Option<String>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let email: Option<String> = conn
            .query_row(
                "SELECT email FROM accounts WHERE id = ?1",
                params![account_id],
                |r| r.get(0),
            )
            .optional()?;
        conn.execute("DELETE FROM accounts WHERE id = ?1", params![account_id])?;
        Ok(email)
    }

    pub fn clear_account_cache(&self, account_id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM folders WHERE account_id = ?1", params![account_id])?;
        Ok(())
    }

    pub fn upsert_folder(
        &self,
        account_id: i64,
        name: &str,
        full_path: &str,
        delimiter: &str,
        role: &str,
        priority: i32,
    ) -> Result<i64> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO folders (account_id, name, full_path, delimiter, role, priority)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(account_id, full_path) DO UPDATE SET
                name = excluded.name,
                delimiter = excluded.delimiter,
                role = excluded.role,
                priority = excluded.priority",
            params![account_id, name, full_path, delimiter, role, priority],
        )?;
        let id: i64 = conn.query_row(
            "SELECT id FROM folders WHERE account_id = ?1 AND full_path = ?2",
            params![account_id, full_path],
            |r| r.get(0),
        )?;
        Ok(id)
    }

    pub fn list_folders(&self, account_id: Option<i64>) -> Result<Vec<FolderRow>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let sql = if account_id.is_some() {
            "SELECT id, account_id, name, full_path, delimiter, role, priority,
                    uid_validity, uid_next, highest_modseq, total_count, unread_count
             FROM folders WHERE account_id = ?1 ORDER BY priority ASC, name ASC"
        } else {
            "SELECT id, account_id, name, full_path, delimiter, role, priority,
                    uid_validity, uid_next, highest_modseq, total_count, unread_count
             FROM folders ORDER BY account_id ASC, priority ASC, name ASC"
        };
        let mut stmt = conn.prepare(sql)?;
        let map_row = |r: &rusqlite::Row| {
            Ok(FolderRow {
                id: r.get(0)?,
                account_id: r.get(1)?,
                name: r.get(2)?,
                full_path: r.get(3)?,
                delimiter: r.get(4)?,
                role: r.get(5)?,
                priority: r.get(6)?,
                uid_validity: r.get(7)?,
                uid_next: r.get(8)?,
                highest_modseq: r.get(9)?,
                total_count: r.get(10)?,
                unread_count: r.get(11)?,
            })
        };
        let rows = if let Some(aid) = account_id {
            stmt.query_map(params![aid], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], map_row)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        Ok(rows)
    }

    pub fn mark_folder_all_read_optimistic(&self, folder_id: i64) -> Result<i64> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let account_id: i64 = conn.query_row(
            "SELECT account_id FROM folders WHERE id = ?1",
            params![folder_id],
            |r| r.get(0),
        )?;
        conn.execute(
            "UPDATE messages SET flags_bitmask = flags_bitmask | 1 WHERE folder_id = ?1",
            params![folder_id],
        )?;
        conn.execute(
            "UPDATE folders SET unread_count = 0 WHERE id = ?1",
            params![folder_id],
        )?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        conn.execute(
            "INSERT INTO pending_ops (account_id, folder_id, op_type, payload_json, created_ts)
             VALUES (?1, ?2, 'mark_all_read', '{}', ?3)",
            params![account_id, folder_id, now],
        )?;
        Ok(account_id)
    }

    pub fn list_pending_ops(&self, account_id: i64) -> Result<Vec<PendingOpRow>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, account_id, folder_id, message_id, uid, op_type, payload_json
             FROM pending_ops WHERE account_id = ?1 ORDER BY id ASC",
        )?;
        let rows = stmt
            .query_map(params![account_id], |r| {
                Ok(PendingOpRow {
                    id: r.get(0)?,
                    account_id: r.get(1)?,
                    folder_id: r.get(2)?,
                    message_id: r.get(3)?,
                    uid: r.get(4)?,
                    op_type: r.get(5)?,
                    payload_json: r.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_pending_op(&self, op_id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM pending_ops WHERE id = ?1", params![op_id])?;
        Ok(())
    }

    pub fn search_messages_local(
        &self,
        folder_id: Option<i64>,
        q: &ParsedSearchQuery,
        limit: i64,
    ) -> Result<Vec<MessageSummary>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut sql = String::from(
            "SELECT m.id, m.account_id, m.folder_id, m.uid, m.thread_id, m.subject,
                    m.from_json, m.date_ts, m.flags_bitmask, m.snippet,
                    m.has_attachments, m.body_synced
             FROM messages m WHERE 1=1",
        );
        let mut bind_vals: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(fid) = folder_id {
            sql.push_str(" AND m.folder_id = ?");
            bind_vals.push(fid.into());
        }
        for f in &q.from {
            sql.push_str(" AND m.from_json LIKE ?");
            bind_vals.push(format!("%{f}%").into());
        }
        for t in &q.to {
            sql.push_str(" AND m.to_json LIKE ?");
            bind_vals.push(format!("%{t}%").into());
        }
        for s in &q.subject {
            sql.push_str(" AND m.subject LIKE ?");
            bind_vals.push(format!("%{s}%").into());
        }
        if let Some(unread) = q.is_unread {
            if unread {
                sql.push_str(" AND (m.flags_bitmask & 1) = 0");
            } else {
                sql.push_str(" AND (m.flags_bitmask & 1) = 1");
            }
        }
        if q.is_starred == Some(true) {
            sql.push_str(" AND (m.flags_bitmask & 2) = 2");
        }
        if q.has_attachment {
            sql.push_str(" AND m.has_attachments = 1");
        }
        if !q.free_text.is_empty() {
            let fts = q.free_text.join(" ");
            sql.push_str(
                " AND m.id IN (SELECT message_id FROM messages_fts WHERE messages_fts MATCH ?)",
            );
            bind_vals.push(fts.into());
        }

        sql.push_str(" ORDER BY m.date_ts DESC LIMIT ?");
        bind_vals.push(limit.into());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(bind_vals), |r| {
                Ok(MessageSummary {
                    id: r.get(0)?,
                    account_id: r.get(1)?,
                    folder_id: r.get(2)?,
                    uid: r.get(3)?,
                    thread_id: r.get(4)?,
                    subject: r.get(5)?,
                    from_json: r.get(6)?,
                    date_ts: r.get(7)?,
                    flags_bitmask: r.get(8)?,
                    snippet: r.get(9)?,
                    has_attachments: r.get::<_, i64>(10)? != 0,
                    body_synced: r.get::<_, i64>(11)? != 0,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn bodies_pending_90_days(&self, account_id: i64, cutoff_ts: i64, limit: i64) -> Result<Vec<(i64, i64, i64, i64)>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT m.id, m.folder_id, m.uid, m.size_bytes
             FROM messages m
             JOIN folders f ON f.id = m.folder_id
             WHERE m.account_id = ?1
               AND m.body_synced = 0
               AND m.date_ts >= ?2
             ORDER BY f.priority ASC, m.date_ts DESC
             LIMIT ?3",
        )?;
        let rows = stmt
            .query_map(params![account_id, cutoff_ts, limit], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
