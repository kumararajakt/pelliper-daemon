use super::Worker;
use crate::imap;
use crate::types::{ImapMessageSummary, SyncStage};

const PREFETCH_BODY_LIMIT: usize = 50;

const PREFETCH_ATTACHMENT_LIMIT: usize = 100;
impl Worker {
    pub(super) async fn fetch_and_store_body(
        &mut self,
        folder_path: &str,
        uid: i64,
    ) -> Result<(String, bool), String> {
        self.ensure_session().await?;
        let raw = {
            let session = self.session.as_mut().ok_or("no session")?;
            imap::fetch_body(session, folder_path, uid)
                .await
                .map_err(|e| e.to_string())?
        };

        let parsed = imap::parse_message_body(&raw);
        let html = if parsed.html.is_empty() {
            format!("<pre>{}</pre>", parsed.text)
        } else {
            parsed.html.clone()
        };

        self.persist_parsed(folder_path, uid, &html, &parsed, &raw)
            .await;

        Ok((html, parsed.has_attachments))
    }

    pub(super) async fn fetch_single_body(
        &mut self,
        folder_path: &str,
        uid: i64,
    ) -> Result<(String, bool), Box<dyn std::error::Error + Send + Sync>> {
        self.fetch_and_store_body(folder_path, uid)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }

    async fn persist_parsed(
        &mut self,
        folder_path: &str,
        uid: i64,
        html: &str,
        parsed: &imap::ParsedBody,
        raw: &[u8],
    ) {
        {
            let cache = self.cache.lock().await;

            if let Err(e) = cache.save_body(self.account.id, folder_path, uid, html) {
                tracing::error!(
                    "Worker[{}]: save_body {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
            if let Err(e) = cache.update_has_attachment(
                self.account.id,
                folder_path,
                uid,
                parsed.has_attachments,
            ) {
                tracing::warn!(
                    "Worker[{}]: update_has_attachment {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
            let headers = imap::headers_from_raw_message(raw);
            if let Err(e) = cache.save_message_headers(self.account.id, folder_path, uid, &headers)
            {
                tracing::error!(
                    "Worker[{}]: save_message_headers {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
        }

        if parsed.attachments.is_empty() {
            return;
        }

        let dir = self
            .attachments_dir
            .join(self.account.id.to_string())
            .join(sanitize_path_component(folder_path))
            .join(uid.to_string());
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!(
                "Worker[{}]: failed to create attachment dir {:?}: {}",
                self.account.id,
                dir,
                e
            );
            return;
        }

        let mut records = Vec::with_capacity(parsed.attachments.len());
        for (i, att) in parsed.attachments.iter().enumerate() {
            let fallback_name = attachment_fallback_name(&att.content_type, i);
            let safe_name = sanitize_filename(&att.name, &fallback_name);
            let full = dir.join(format!("{}_{}", i, safe_name));
            if let Err(e) = std::fs::write(&full, &att.bytes) {
                tracing::warn!(
                    "Worker[{}]: failed to write attachment {:?}: {}",
                    self.account.id,
                    full,
                    e
                );
                continue;
            }
            records.push(crate::types::AttachmentRecord {
                name: if att.name.is_empty() {
                    fallback_name.clone()
                } else {
                    att.name.clone()
                },
                file_path: full.to_string_lossy().to_string(),
                content_type: att.content_type.clone(),
                size: att.bytes.len() as i64,
            });
        }

        if !records.is_empty() {
            let cache = self.cache.lock().await;
            if let Err(e) = cache.replace_attachments(self.account.id, folder_path, uid, &records) {
                tracing::error!(
                    "Worker[{}]: replace_attachments {}/{} failed: {e}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
            tracing::debug!(
                "Worker[{}]: cached {} attachment(s) for {} uid={}",
                self.account.id,
                records.len(),
                folder_path,
                uid
            );
        }
    }

    pub(super) async fn queue_body_prefetch(
        &mut self,
        folder_path: &str,
        summaries: &[ImapMessageSummary],
    ) {
        let cache = self.cache.lock().await;
        for msg in summaries.iter().take(PREFETCH_BODY_LIMIT) {
            let key = (folder_path.to_string(), msg.uid);
            if self.body_prefetched.contains(&key) {
                continue;
            }
            match cache.has_body(self.account.id, folder_path, msg.uid) {
                Ok(true) => {
                    self.body_prefetched.insert(key);
                }
                _ => {
                    self.body_prefetch.push_back(key.clone());
                    self.body_prefetched.insert(key);
                }
            }
        }
    }

    pub(super) async fn run_one_body_prefetch(&mut self, folder_path: &str, uid: i64) {
        match self.fetch_and_store_body(folder_path, uid).await {
            Ok(_) => {
                tracing::debug!(
                    "Worker[{}]: prefetched body for {} uid={}",
                    self.account.id,
                    folder_path,
                    uid
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Worker[{}]: body prefetch failed for {} uid={}: {}",
                    self.account.id,
                    folder_path,
                    uid,
                    e
                );

                self.note_sync_failure(folder_path, uid, SyncStage::Body, &e)
                    .await;
            }
        }
    }

    pub(super) async fn queue_attachment_prefetch(&mut self, folder_path: &str) {
        let uids = {
            let cache = self.cache.lock().await;
            cache
                .messages_missing_attachments(
                    self.account.id,
                    folder_path,
                    PREFETCH_ATTACHMENT_LIMIT,
                )
                .unwrap_or_default()
        };

        for uid in uids {
            let key = (folder_path.to_string(), uid);
            if self.attachment_prefetched.contains(&key) {
                continue;
            }
            if self.body_prefetch.iter().any(|k| k == &key) {
                self.attachment_prefetched.insert(key);
                continue;
            }
            self.attachment_prefetch.push_back(key.clone());
            self.attachment_prefetched.insert(key);
        }
    }

    pub(super) async fn run_one_attachment_prefetch(&mut self, folder_path: &str, uid: i64) {
        self.run_one_body_prefetch(folder_path, uid).await;
    }
}

fn sanitize_path_component(path: &str) -> String {
    let clean: String = path
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if clean.is_empty() {
        "folder".to_string()
    } else {
        clean
    }
}

fn sanitize_filename(name: &str, fallback: &str) -> String {
    let clean: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ' ' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();
    if clean.is_empty() || clean == "." {
        fallback.to_string()
    } else {
        clean
    }
}

fn attachment_fallback_name(content_type: &str, index: usize) -> String {
    let lower = content_type.to_lowercase();
    let ext = match lower.split('/').nth(1) {
        Some("pdf") => "pdf",
        Some("zip") => "zip",
        Some("html") => "html",
        Some("plain") => "txt",
        Some("jpeg") => "jpg",
        Some("png") => "png",
        Some("gif") => "gif",
        Some("octet-stream") | None => "bin",
        Some(other) => {
            if other.is_empty() {
                "bin"
            } else {
                other
            }
        }
    };
    format!("attachment_{}.{}", index, ext)
}
