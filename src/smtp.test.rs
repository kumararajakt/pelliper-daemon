use super::*;
use std::io::Write;

#[test]
fn split_recipient_list_handles_commas_in_quoted_strings_and_angle_brackets() {
    let raw = r#""Doe, Jane" <jane@example.com>, "Smith, John; Jr" <john@example.com>; regular@example.com, <bare@example.com>"#;
    let parts = split_recipient_list(raw);
    assert_eq!(
        parts,
        vec![
            r#""Doe, Jane" <jane@example.com>"#,
            r#""Smith, John; Jr" <john@example.com>"#,
            "regular@example.com",
            "<bare@example.com>",
        ]
    );
}

#[test]
fn split_recipient_list_ignores_empty_or_whitespace_strings() {
    assert!(split_recipient_list("").is_empty());
    assert!(split_recipient_list("   ").is_empty());
    assert!(split_recipient_list(",, ;; , ").is_empty());
}

#[test]
fn build_message_without_attachments_creates_multipart_alternative() {
    let msg = build_message(
        "sender@example.com",
        r#""Doe, Jane" <jane@example.com>"#,
        "cc@example.com",
        Some("bcc@example.com"),
        Some("replyto@example.com"),
        "Hello World",
        "Plain text body",
        Some("<msg123@example.com>"),
        Some("<ref1@example.com> <ref2@example.com>"),
        &[],
    )
    .expect("build message");

    let raw = String::from_utf8_lossy(&msg.formatted()).to_string();
    assert!(raw.contains("Content-Type: multipart/alternative;"));
    assert!(!raw.contains("Content-Type: multipart/mixed;"));
    assert!(raw.contains("Subject: Hello World"));
    assert!(raw.contains("<jane@example.com>"));
    assert!(raw.contains("Cc: cc@example.com"));
    assert!(raw.contains("In-Reply-To: <msg123@example.com>"));
    assert!(raw.contains("References: <ref1@example.com> <ref2@example.com>"));
    assert!(raw.contains("Plain text body"));
    assert!(raw.contains("<pre>Plain text body</pre>"));

    let envelope_to: Vec<String> = msg.envelope().to().iter().map(|a| a.to_string()).collect();
    assert_eq!(
        envelope_to,
        vec!["jane@example.com", "cc@example.com", "bcc@example.com"]
    );
}

#[test]
fn build_message_with_attachments_creates_multipart_mixed_with_disposition() {
    let dir = std::env::temp_dir().join(format!("pelliper-smtp-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let file_path = dir.join("quarterly_report.pdf");
    {
        let mut file = std::fs::File::create(&file_path).expect("create test file");
        file.write_all(b"%PDF-1.4 mock content")
            .expect("write file");
    }

    let attachment = EmailAttachment {
        path: file_path.to_str().unwrap().to_string(),
        filename: "quarterly_report.pdf".to_string(),
    };

    let msg = build_message(
        "sender@example.com",
        "jane@example.com",
        "",
        None,
        None,
        "Report Attached",
        "<p>Please find attached report.</p>",
        None,
        None,
        &[attachment],
    )
    .expect("build message with attachment");

    let raw = String::from_utf8_lossy(&msg.formatted()).to_string();

    assert!(raw.contains("Content-Type: multipart/mixed;"));
    assert!(raw.contains("Content-Type: multipart/alternative;"));
    assert!(raw.contains("Content-Type: application/pdf"));
    assert!(raw.contains(r#"Content-Disposition: attachment; filename="quarterly_report.pdf""#));
    assert!(raw.contains("<p>Please find attached report.</p>"));

    let _ = std::fs::remove_dir_all(&dir);
}
