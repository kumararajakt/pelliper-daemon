use super::*;

#[test]
fn normalize_message_id_strips_brackets_and_comments() {
    assert_eq!(normalize_message_id("<a@b.com>"), "a@b.com");
    assert_eq!(normalize_message_id("a@b.com"), "a@b.com");
    // RFC 5322 comment trailing the id
    assert_eq!(normalize_message_id("<a@b.com> (Comment)"), "a@b.com");
    // Comment first, then the id
    assert_eq!(normalize_message_id("(C) <a@b.com>"), "a@b.com");
    // Nested comments must not disrupt extraction
    assert_eq!(normalize_message_id("<a@b.com> (x (y))"), "a@b.com");
    assert_eq!(normalize_message_id(""), "");
    assert_eq!(normalize_message_id("   "), "");
}

#[test]
fn normalize_references_joins_and_dedups() {
    assert_eq!(
        normalize_references("<a@b.com> <c@d.com>"),
        "a@b.com c@d.com"
    );
    // Trailing comment is dropped
    assert_eq!(
        normalize_references("<a@b.com> <c@d.com> (thread topic)"),
        "a@b.com c@d.com"
    );
    // Bare ids without angle brackets are kept
    assert_eq!(normalize_references("a@b.com c@d.com"), "a@b.com c@d.com");
    // Duplicates are removed
    assert_eq!(normalize_references("<a@b.com> <a@b.com>"), "a@b.com");
    assert_eq!(normalize_references(""), "");
}

#[test]
fn threading_matches_after_normalization() {
    // A raw-path message stored `message_id` with brackets; the parent's
    // References (ENVELOPE source) had none. After normalization both are
    // the same bare id, so threadmodel.cpp's linkage finds the parent.
    let child_message_id = normalize_message_id("<reply@host>");
    let parent_refs = normalize_references("reply@host");
    assert_eq!(child_message_id, parent_refs);
}
