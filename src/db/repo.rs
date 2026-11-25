use anyhow::Result;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::db::Database;
use crate::mime::ParsedMessageBody;
use crate::search::ParsedSearchQuery;

#[derive(Debug, Clone)]
pub struct NewHeaderRow {
    pub account_id: i64,
    pub folder_id: i64,
    pub uid: i64,
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references_header: Option<String>,
    pub thread_id: String,
    pub subject: String,
    pub from_json: String,
    pub to_json: String,
    pub cc_json: String,
    pub date_ts: i64,
    pub internal_date_ts: i64,
    pub size_bytes: i64,
    pub flags_bitmask: i64,
}

#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub id: i64,
    pub account_id: i64,
    pub payload_json: String,
    pub retry_count: i64,
}

#[derive(Debug, Clone)]
pub struct AttachmentDownloadInfo {
    pub id: i64,
    pub message_id: i64,
    pub account_id: i64,
    pub folder_id: i64,
    pub uid: i64,
    pub part_id: String,
    pub filename: String,
    pub mime_type: String,
    pub local_path: Option<String>,
}

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
    pub signature: String,
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
                    sync_interval_secs, COALESCE(signature, ''), enabled, last_error
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
                    signature: r.get(12)?,
                    enabled: r.get::<_, i64>(13)? != 0,
                    last_error: r.get(14)?,
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

    pub fn update_account(
        &self,
        account_id: i64,
        display_name: Option<&str>,
        sync_interval_secs: Option<i64>,
        signature: Option<&str>,
        enabled: Option<bool>,
    ) -> Result<Option<AccountRow>> {
        {
            let conn = self.conn.lock().expect("db mutex poisoned");
            if let Some(name) = display_name {
                conn.execute(
                    "UPDATE accounts SET display_name = ?1 WHERE id = ?2",
                    params![name, account_id],
                )?;
            }
            if let Some(interval) = sync_interval_secs {
                conn.execute(
                    "UPDATE accounts SET sync_interval_secs = ?1 WHERE id = ?2",
                    params![interval.max(60), account_id],
                )?;
            }
            if let Some(sig) = signature {
                conn.execute(
                    "UPDATE accounts SET signature = ?1 WHERE id = ?2",
                    params![sig, account_id],
                )?;
            }
            if let Some(en) = enabled {
                conn.execute(
                    "UPDATE accounts SET enabled = ?1 WHERE id = ?2",
                    params![if en { 1 } else { 0 }, account_id],
                )?;
            }
        }
        self.get_account(account_id)
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

    pub fn get_message_detail(&self, message_id: i64) -> Result<Option<serde_json::Value>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let row: Option<(i64, i64, i64, String, String, String, String, String, i64, i64, bool, Option<String>, Option<String>)> = conn
            .query_row(
                "SELECT m.id, m.account_id, m.folder_id, m.subject, m.from_json, m.to_json,
                        m.cc_json, COALESCE(m.message_id, ''), m.date_ts, m.flags_bitmask, m.body_synced,
                        b.text_plain, b.text_html
                 FROM messages m
                 LEFT JOIN message_bodies b ON b.message_id = m.id
                 WHERE m.id = ?1",
                params![message_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get::<_, i64>(10)? != 0,
                        r.get(11)?,
                        r.get(12)?,
                    ))
                },
            )
            .optional()?;

        let Some((id, account_id, folder_id, subject, from_json, to_json, cc_json, rfc_id, date_ts, flags, body_synced, text_plain, text_html)) = row else {
            return Ok(None);
        };

        let mut att_stmt = conn.prepare(
            "SELECT id, filename, mime_type, size_bytes, is_inline, local_path
             FROM attachments WHERE message_id = ?1 ORDER BY id ASC",
        )?;
        let attachments: Vec<serde_json::Value> = att_stmt
            .query_map(params![message_id], |r| {
                Ok(serde_json::json!({
                    "id": r.get::<_, i64>(0)?,
                    "filename": r.get::<_, String>(1)?,
                    "mime_type": r.get::<_, String>(2)?,
                    "size_bytes": r.get::<_, i64>(3)?,
                    "is_inline": r.get::<_, i64>(4)? != 0,
                    "local_path": r.get::<_, Option<String>>(5)?,
                }))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Some(serde_json::json!({
            "id": id,
            "account_id": account_id,
            "folder_id": folder_id,
            "subject": subject,
            "from_json": from_json,
            "to_json": to_json,
            "cc_json": cc_json,
            "message_id": rfc_id,
            "date_ts": date_ts,
            "flags_bitmask": flags,
            "body_synced": body_synced,
            "text_plain": text_plain,
            "text_html": text_html,
            "attachments": attachments,
        })))
    }

    pub fn set_flags_optimistic(&self, message_id: i64, add_mask: i64, remove_mask: i64) -> Result<Option<(i64, i64)>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let meta: Option<(i64, i64, i64)> = conn
            .query_row(
                "SELECT account_id, folder_id, uid FROM messages WHERE id = ?1",
                params![message_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((account_id, folder_id, uid)) = meta else {
            return Ok(None);
        };

        conn.execute(
            "UPDATE messages SET flags_bitmask = (flags_bitmask | ?1) & ~?2 WHERE id = ?3",
            params![add_mask, remove_mask, message_id],
        )?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let payload = serde_json::json!({ "add_mask": add_mask, "remove_mask": remove_mask }).to_string();
        conn.execute(
            "INSERT INTO pending_ops (account_id, folder_id, message_id, uid, op_type, payload_json, created_ts)
             VALUES (?1, ?2, ?3, ?4, 'set_flags', ?5, ?6)",
            params![account_id, folder_id, message_id, uid, payload, now],
        )?;
        Ok(Some((account_id, folder_id)))
    }

    pub fn delete_or_move_optimistic(&self, message_id: i64, target_role: &str) -> Result<Option<(i64, i64)>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let meta: Option<(i64, i64, i64)> = conn
            .query_row(
                "SELECT account_id, folder_id, uid FROM messages WHERE id = ?1",
                params![message_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((account_id, folder_id, uid)) = meta else {
            return Ok(None);
        };

        let target_folder: Option<i64> = conn
            .query_row(
                "SELECT id FROM folders WHERE account_id = ?1 AND role = ?2 LIMIT 1",
                params![account_id, target_role],
                |r| r.get(0),
            )
            .optional()?;

        if let Some(tfid) = target_folder {
            if tfid != folder_id {
                conn.execute("UPDATE messages SET folder_id = ?1 WHERE id = ?2", params![tfid, message_id])?;
            } else {
                conn.execute("DELETE FROM messages WHERE id = ?1", params![message_id])?;
            }
        } else {
            conn.execute("DELETE FROM messages WHERE id = ?1", params![message_id])?;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let payload = serde_json::json!({ "target_role": target_role }).to_string();
        conn.execute(
            "INSERT INTO pending_ops (account_id, folder_id, message_id, uid, op_type, payload_json, created_ts)
             VALUES (?1, ?2, ?3, ?4, 'move_role', ?5, ?6)",
            params![account_id, folder_id, message_id, uid, payload, now],
        )?;
        Ok(Some((account_id, folder_id)))
    }

    pub fn enqueue_outbox(&self, account_id: i64, payload_json: &str) -> Result<i64> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "INSERT INTO outbox (account_id, payload_json, status) VALUES (?1, ?2, 'queued')",
            params![account_id, payload_json],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn list_ready_outbox(&self, now_ts: i64) -> Result<Vec<OutboxRow>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, account_id, payload_json, retry_count
             FROM outbox
             WHERE status IN ('queued', 'retrying') AND next_retry_ts <= ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt
            .query_map(params![now_ts], |r| {
                Ok(OutboxRow {
                    id: r.get(0)?,
                    account_id: r.get(1)?,
                    payload_json: r.get(2)?,
                    retry_count: r.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_outbox(&self, outbox_id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM outbox WHERE id = ?1", params![outbox_id])?;
        Ok(())
    }

    pub fn mark_outbox_retry(&self, outbox_id: i64, retry_count: i64, next_retry_ts: i64, err: &str) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        let status = if retry_count >= 5 { "failed" } else { "retrying" };
        conn.execute(
            "UPDATE outbox SET status = ?1, retry_count = ?2, next_retry_ts = ?3, last_error = ?4 WHERE id = ?5",
            params![status, retry_count, next_retry_ts, err, outbox_id],
        )?;
        Ok(())
    }

    pub fn get_folder_path(&self, folder_id: i64) -> Result<Option<String>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT full_path FROM folders WHERE id = ?1",
            params![folder_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn find_folder_path_by_role(&self, account_id: i64, role: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT full_path FROM folders WHERE account_id = ?1 AND role = ?2 LIMIT 1",
            params![account_id, role],
            |r| r.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn clear_folder_messages(&self, folder_id: i64) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute("DELETE FROM messages WHERE folder_id = ?1", params![folder_id])?;
        conn.execute("UPDATE folders SET uid_next = 1 WHERE id = ?1", params![folder_id])?;
        Ok(())
    }

    pub fn update_folder_uid_validity(&self, folder_id: i64, uid_validity: i64) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE folders SET uid_validity = ?1 WHERE id = ?2",
            params![uid_validity, folder_id],
        )?;
        Ok(())
    }

    pub fn upsert_message_headers_batch(&self, rows: &[NewHeaderRow], folder_id: i64, max_uid: i64) -> Result<Vec<MessageSummary>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        let mut inserted = Vec::new();

        for h in rows {
            // Anti-ghost guard: skip if a pending move/delete exists for this (folder_id, uid)
            let pending_move: i64 = tx.query_row(
                "SELECT COUNT(*) FROM pending_ops WHERE folder_id = ?1 AND uid = ?2 AND op_type = 'move_role'",
                params![h.folder_id, h.uid],
                |r| r.get(0),
            )?;
            if pending_move > 0 {
                continue;
            }

            tx.execute(
                "INSERT INTO messages (account_id, folder_id, uid, message_id, in_reply_to, references_header,
                                       thread_id, subject, from_json, to_json, cc_json,
                                       date_ts, internal_date_ts, size_bytes, flags_bitmask)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                 ON CONFLICT(folder_id, uid) DO UPDATE SET
                    flags_bitmask = excluded.flags_bitmask",
                params![
                    h.account_id,
                    h.folder_id,
                    h.uid,
                    h.message_id,
                    h.in_reply_to,
                    h.references_header,
                    h.thread_id,
                    h.subject,
                    h.from_json,
                    h.to_json,
                    h.cc_json,
                    h.date_ts,
                    h.internal_date_ts,
                    h.size_bytes,
                    h.flags_bitmask,
                ],
            )?;

            let id: i64 = tx.query_row(
                "SELECT id FROM messages WHERE folder_id = ?1 AND uid = ?2",
                params![h.folder_id, h.uid],
                |r| r.get(0),
            )?;
            inserted.push(MessageSummary {
                id,
                account_id: h.account_id,
                folder_id: h.folder_id,
                uid: h.uid,
                thread_id: h.thread_id.clone(),
                subject: h.subject.clone(),
                from_json: h.from_json.clone(),
                date_ts: h.date_ts,
                flags_bitmask: h.flags_bitmask,
                snippet: String::new(),
                has_attachments: false,
                body_synced: false,
            });
        }

        if max_uid > 0 {
            tx.execute(
                "UPDATE folders SET uid_next = MAX(uid_next, ?1) WHERE id = ?2",
                params![max_uid + 1, folder_id],
            )?;
        }
        tx.execute(
            "UPDATE folders SET
                total_count = (SELECT COUNT(*) FROM messages WHERE folder_id = ?1),
                unread_count = (SELECT COUNT(*) FROM messages WHERE folder_id = ?1 AND (flags_bitmask & 1) = 0)
             WHERE id = ?1",
            params![folder_id],
        )?;
        tx.commit()?;
        Ok(inserted)
    }

    pub fn reconcile_folder_flags(&self, folder_id: i64, server_flags: &[(i64, i64)]) -> Result<()> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        for &(uid, flags) in server_flags {
            tx.execute(
                "UPDATE messages SET flags_bitmask = ?1 WHERE folder_id = ?2 AND uid = ?3",
                params![flags, folder_id, uid],
            )?;
        }
        tx.execute(
            "UPDATE folders SET
                total_count = (SELECT COUNT(*) FROM messages WHERE folder_id = ?1),
                unread_count = (SELECT COUNT(*) FROM messages WHERE folder_id = ?1 AND (flags_bitmask & 1) = 0)
             WHERE id = ?1",
            params![folder_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_message_uid_folder(&self, message_id: i64) -> Result<Option<(i64, i64, i64)>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT folder_id, uid, size_bytes FROM messages WHERE id = ?1 AND body_synced = 0",
            params![message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn save_parsed_bodies_batch(&self, items: &[(i64, ParsedMessageBody)]) -> Result<Vec<(i64, String)>> {
        let mut conn = self.conn.lock().expect("db mutex poisoned");
        let tx = conn.transaction()?;
        let mut ready = Vec::with_capacity(items.len());

        for (message_id, body) in items {
            tx.execute(
                "INSERT INTO message_bodies (message_id, text_plain, text_html, raw_size)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(message_id) DO UPDATE SET
                    text_plain = excluded.text_plain,
                    text_html = excluded.text_html,
                    raw_size = excluded.raw_size",
                params![message_id, body.text_plain, body.text_html, body.raw_size],
            )?;

            let has_att = body.attachments.iter().any(|a| !a.is_inline);
            tx.execute(
                "UPDATE messages SET snippet = ?1, has_attachments = ?2, body_synced = 1 WHERE id = ?3",
                params![body.snippet, if has_att { 1 } else { 0 }, message_id],
            )?;

            tx.execute("DELETE FROM attachments WHERE message_id = ?1", params![message_id])?;
            for att in &body.attachments {
                tx.execute(
                    "INSERT INTO attachments (message_id, part_id, content_id, filename, mime_type, size_bytes, is_inline)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        message_id,
                        att.part_id,
                        att.content_id,
                        att.filename,
                        att.mime_type,
                        att.size_bytes,
                        if att.is_inline { 1 } else { 0 }
                    ],
                )?;
            }

            let meta: Option<(String, String, String)> = tx
                .query_row(
                    "SELECT subject, from_json, to_json FROM messages WHERE id = ?1",
                    params![message_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((subj, from_j, to_j)) = meta {
                tx.execute("DELETE FROM messages_fts WHERE message_id = ?1", params![message_id])?;
                tx.execute(
                    "INSERT INTO messages_fts (message_id, subject, from_text, to_text, body_text)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        message_id,
                        subj,
                        from_j,
                        to_j,
                        body.text_plain.as_deref().unwrap_or("")
                    ],
                )?;
            }

            ready.push((*message_id, body.snippet.clone()));
        }

        tx.commit()?;
        Ok(ready)
    }

    pub fn get_attachment_download_info(&self, attachment_id: i64) -> Result<Option<AttachmentDownloadInfo>> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.query_row(
            "SELECT a.id, a.message_id, m.account_id, m.folder_id, m.uid,
                    a.part_id, a.filename, a.mime_type, a.local_path
             FROM attachments a
             JOIN messages m ON m.id = a.message_id
             WHERE a.id = ?1",
            params![attachment_id],
            |r| {
                Ok(AttachmentDownloadInfo {
                    id: r.get(0)?,
                    message_id: r.get(1)?,
                    account_id: r.get(2)?,
                    folder_id: r.get(3)?,
                    uid: r.get(4)?,
                    part_id: r.get(5)?,
                    filename: r.get(6)?,
                    mime_type: r.get(7)?,
                    local_path: r.get(8)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn set_attachment_local_path(&self, attachment_id: i64, local_path: &str) -> Result<()> {
        let conn = self.conn.lock().expect("db mutex poisoned");
        conn.execute(
            "UPDATE attachments SET local_path = ?1 WHERE id = ?2",
            params![local_path, attachment_id],
        )?;
        Ok(())
    }
}
