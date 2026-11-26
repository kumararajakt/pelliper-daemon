use base64::Engine;
use mail_parser::{MessageParser, MimeHeaders};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedAttachmentMeta {
    pub part_id: String,
    pub content_id: Option<String>,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: i64,
    pub is_inline: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedMessageBody {
    pub text_plain: Option<String>,
    pub text_html: Option<String>,
    pub snippet: String,
    pub attachments: Vec<ParsedAttachmentMeta>,
    pub raw_size: i64,
}

/// Parses raw RFC 5322 bytes into body text/html, a clean 1-line snippet, and attachment metadata.
pub fn parse_message_body(raw: &[u8]) -> Option<ParsedMessageBody> {
    let msg = MessageParser::default().parse(raw)?;

    let text_plain = msg.body_text(0).map(|s| s.into_owned());
    let text_html = msg.body_html(0).map(|s| s.into_owned());

    let snippet_source = text_plain
        .as_deref()
        .or(text_html.as_deref())
        .unwrap_or("");
    let snippet = build_snippet(snippet_source, text_plain.is_none() && text_html.is_some());

    let mut attachments = Vec::new();
    for (idx, part) in msg.attachments().enumerate() {
        let filename = part
            .attachment_name()
            .unwrap_or("attachment")
            .to_string();
        let mime_type = part
            .content_type()
            .map(|ct| {
                if let Some(sub) = ct.subtype() {
                    format!("{}/{}", ct.ctype(), sub)
                } else {
                    ct.ctype().to_string()
                }
            })
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let content_id = part.content_id().map(|s| s.to_string());
        let is_inline = content_id.is_some();

        attachments.push(ParsedAttachmentMeta {
            part_id: (idx + 1).to_string(),
            content_id,
            filename,
            mime_type,
            size_bytes: part.contents().len() as i64,
            is_inline,
        });
    }

    Some(ParsedMessageBody {
        text_plain,
        text_html,
        snippet,
        attachments,
        raw_size: raw.len() as i64,
    })
}

/// Extracts raw attachment bytes by 1-based `part_id` (falling back to filename match).
pub fn extract_attachment_bytes(raw: &[u8], part_id: &str, filename: &str) -> Option<Vec<u8>> {
    let msg = MessageParser::default().parse(raw)?;
    let target_idx: Option<usize> = part_id.parse::<usize>().ok().and_then(|n| n.checked_sub(1));

    if let Some(idx) = target_idx {
        if let Some(part) = msg.attachments().nth(idx) {
            return Some(part.contents().to_vec());
        }
    }
    for part in msg.attachments() {
        if part.attachment_name() == Some(filename) {
            return Some(part.contents().to_vec());
        }
    }
    None
}

/// Decodes RFC 2047 encoded-words (`=?UTF-8?Q?...?=` and `=?UTF-8?B?...?=`) in headers.
pub fn decode_rfc2047(input: &str) -> String {
    if !input.contains("=?") {
        return input.to_string();
    }

    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    let mut prev_was_encoded = false;

    while let Some(start) = rest.find("=?") {
        let prefix = &rest[..start];
        if !(prev_was_encoded && prefix.chars().all(char::is_whitespace)) {
            out.push_str(prefix);
        }

        let after_start = &rest[start + 2..];
        let Some(q1) = after_start.find('?') else {
            out.push_str("=?");
            rest = after_start;
            prev_was_encoded = false;
            continue;
        };
        let after_charset = &after_start[q1 + 1..];
        let Some(q2) = after_charset.find('?') else {
            out.push_str(&rest[start..start + 2 + q1 + 1]);
            rest = after_charset;
            prev_was_encoded = false;
            continue;
        };
        let encoding = &after_charset[..q2];
        let after_enc = &after_charset[q2 + 1..];
        let Some(end) = after_enc.find("?=") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let payload = &after_enc[..end];

        let decoded_bytes: Option<Vec<u8>> = if encoding.eq_ignore_ascii_case("B") {
            base64::engine::general_purpose::STANDARD.decode(payload).ok()
        } else if encoding.eq_ignore_ascii_case("Q") {
            Some(decode_q_encoding(payload))
        } else {
            None
        };

        if let Some(bytes) = decoded_bytes {
            out.push_str(&String::from_utf8_lossy(&bytes));
            prev_was_encoded = true;
        } else {
            out.push_str(&rest[start..start + 2 + q1 + 1 + q2 + 1 + end + 2]);
            prev_was_encoded = false;
        }
        rest = &after_enc[end + 2..];
    }

    out.push_str(rest);
    out
}

fn decode_q_encoding(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'_' => {
                out.push(b' ');
                i += 1;
            }
            b'=' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    out.push((h << 4) | l);
                    i += 3;
                } else {
                    out.push(b'=');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Computes a deterministic `thread_id` from `References` / `In-Reply-To` or normalized `Subject`.
pub fn compute_thread_id(
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    references: Option<&str>,
    subject: &str,
) -> String {
    if let Some(refs) = references {
        if let Some(root) = refs.split_whitespace().next() {
            let trimmed = root.trim_matches(['<', '>']);
            if !trimmed.is_empty() {
                return format!("ref:{trimmed}");
            }
        }
    }
    if let Some(irt) = in_reply_to {
        let trimmed = irt.trim().trim_matches(['<', '>']);
        if !trimmed.is_empty() {
            return format!("ref:{trimmed}");
        }
    }
    let norm = normalize_subject(subject);
    if !norm.is_empty() {
        return format!("subj:{norm}");
    }
    let mid = message_id
        .unwrap_or("")
        .trim()
        .trim_matches(['<', '>']);
    format!("msg:{mid}")
}

pub fn normalize_subject(subject: &str) -> String {
    let mut s = subject.trim();
    loop {
        let lower = s.to_ascii_lowercase();
        if lower.starts_with("re:") || lower.starts_with("fw:") {
            s = s[3..].trim_start();
        } else if lower.starts_with("fwd:") {
            s = s[4..].trim_start();
        } else {
            break;
        }
    }
    s.to_ascii_lowercase()
}

fn build_snippet(input: &str, strip_html: bool) -> String {
    let cleaned = if strip_html {
        let mut out = String::with_capacity(input.len().min(512));
        let mut in_tag = false;
        for ch in input.chars() {
            match ch {
                '<' => in_tag = true,
                '>' => {
                    in_tag = false;
                    out.push(' ');
                }
                _ if !in_tag => out.push(ch),
                _ => {}
            }
            if out.len() > 400 {
                break;
            }
        }
        out
    } else {
        input.chars().take(400).collect()
    };

    let collapsed: String = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > 140 {
        let mut s: String = collapsed.chars().take(137).collect();
        s.push_str("...");
        s
    } else {
        collapsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_email_and_snippet() {
        let raw = b"From: Alice <alice@example.com>\r\n\
Subject: Hello Pelliper\r\n\
Message-ID: <abc@example.com>\r\n\
Content-Type: text/plain; charset=utf-8\r\n\r\n\
Hello   world,\r\nThis is a test body.\r\n";

        let parsed = parse_message_body(raw).unwrap();
        assert_eq!(parsed.snippet, "Hello world, This is a test body.");
        assert!(parsed.text_plain.as_deref().unwrap().contains("test body"));
    }

    #[test]
    fn thread_id_groups_replies_by_root_reference() {
        let t1 = compute_thread_id(Some("<m1@x>"), None, None, "Project Plan");
        let t2 = compute_thread_id(
            Some("<m2@x>"),
            Some("<m1@x>"),
            Some("<m0@x> <m1@x>"),
            "Re: Project Plan",
        );
        assert_eq!(t1, "subj:project plan");
        assert_eq!(t2, "ref:m0@x");
        assert_eq!(normalize_subject("Re: Fwd: Re: Budget"), "budget");
    }

    #[test]
    fn decodes_rfc2047_q_and_b_headers() {
        let raw = "=?UTF-8?Q?SBI_offers_=E2=82=B91.6_crore_for_Ch?= =?UTF-8?Q?ief_AI_Officer?=";
        assert_eq!(decode_rfc2047(raw), "SBI offers ₹1.6 crore for Chief AI Officer");
        let b64 = "=?UTF-8?B?SGVsbG8gUGVsbGlwZXI=?=";
        assert_eq!(decode_rfc2047(b64), "Hello Pelliper");
    }
}
