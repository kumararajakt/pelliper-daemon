use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedSearchQuery {
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub folder: Vec<String>,
    pub before: Option<String>,
    pub since: Option<String>,
    pub is_unread: Option<bool>,
    pub is_starred: Option<bool>,
    pub has_attachment: bool,
    pub subject: Vec<String>,
    pub free_text: Vec<String>,
}

/// Parses compound `AND` search queries such as `from: alice@x.com + in: inbox is:unread invoice`.
pub fn parse_search_query(input: &str) -> ParsedSearchQuery {
    let tokens = tokenize(input);
    let mut query = ParsedSearchQuery::default();
    let mut i = 0;

    while i < tokens.len() {
        let tok = &tokens[i];
        if tok == "+" || tok.eq_ignore_ascii_case("AND") {
            i += 1;
            continue;
        }

        if let Some((raw_key, raw_val)) = tok.split_once(':') {
            let key = raw_key.to_ascii_lowercase();
            let mut val = raw_val.to_string();

            // Support `from: alice@example.com` (space after colon)
            if val.is_empty() && i + 1 < tokens.len() {
                let next = &tokens[i + 1];
                if next != "+" && !next.eq_ignore_ascii_case("AND") && !next.contains(':') {
                    val = next.clone();
                    i += 1;
                }
            }

            if !val.is_empty() {
                match key.as_str() {
                    "from" => query.from.push(val),
                    "to" => query.to.push(val),
                    "in" | "folder" => query.folder.push(val),
                    "before" => query.before = Some(val),
                    "since" | "after" => query.since = Some(val),
                    "subject" => query.subject.push(val),
                    "is" => match val.to_ascii_lowercase().as_str() {
                        "unread" => query.is_unread = Some(true),
                        "read" => query.is_unread = Some(false),
                        "starred" | "flagged" => query.is_starred = Some(true),
                        _ => {}
                    },
                    "has" => {
                        if val.eq_ignore_ascii_case("attachment")
                            || val.eq_ignore_ascii_case("attachments")
                        {
                            query.has_attachment = true;
                        }
                    }
                    _ => query.free_text.push(tok.clone()),
                }
            }
        } else {
            query.free_text.push(tok.clone());
        }
        i += 1;
    }

    query
}

/// Builds an IMAP `SEARCH` criteria string for background server-side search.
pub fn to_imap_search_criteria(q: &ParsedSearchQuery) -> Vec<String> {
    let mut parts = Vec::new();
    for f in &q.from {
        parts.push(format!("FROM \"{}\"", escape_imap(f)));
    }
    for t in &q.to {
        parts.push(format!("TO \"{}\"", escape_imap(t)));
    }
    for s in &q.subject {
        parts.push(format!("SUBJECT \"{}\"", escape_imap(s)));
    }
    if let Some(u) = q.is_unread {
        parts.push(if u { "UNSEEN" } else { "SEEN" }.to_string());
    }
    if q.is_starred == Some(true) {
        parts.push("FLAGGED".to_string());
    }
    for term in &q.free_text {
        parts.push(format!("TEXT \"{}\"", escape_imap(term)));
    }
    if parts.is_empty() {
        parts.push("ALL".to_string());
    }
    parts
}

fn escape_imap(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn tokenize(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            '+' if !in_quotes => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                out.push("+".to_string());
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compound_and_with_plus_and_space_after_colon() {
        let q = parse_search_query("from: example@example.com + in: inbox is:unread subject:\"Q4 Report\" invoice");
        assert_eq!(q.from, vec!["example@example.com"]);
        assert_eq!(q.folder, vec!["inbox"]);
        assert_eq!(q.is_unread, Some(true));
        assert_eq!(q.subject, vec!["Q4 Report"]);
        assert_eq!(q.free_text, vec!["invoice"]);
    }

    #[test]
    fn builds_imap_criteria() {
        let q = parse_search_query("from:alice@x.com is:starred budget");
        let crit = to_imap_search_criteria(&q);
        assert_eq!(
            crit,
            vec!["FROM \"alice@x.com\"", "FLAGGED", "TEXT \"budget\""]
        );
    }
}
