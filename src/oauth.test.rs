use super::{config_for_id, provider_for, provider_from_email};
use crate::types::Account;

fn account(email: &str, provider: Option<&str>) -> Account {
    Account {
        id: 1,
        email: email.to_string(),
        display_name: None,
        provider: provider.map(str::to_string),
        auth_type: "oauth".to_string(),
        imap_host: None,
        imap_port: None,
        imap_security: None,
        smtp_host: None,
        smtp_port: None,
        smtp_security: None,
        username: None,
        refresh_token: None,
        enabled: true,
    }
}

#[test]
fn infers_provider_from_known_domains() {
    assert_eq!(provider_from_email("a@gmail.com"), Some("google"));
    assert_eq!(provider_from_email("a@googlemail.com"), Some("google"));
    assert_eq!(provider_from_email("a@outlook.com"), Some("microsoft"));
    assert_eq!(provider_from_email("a@hotmail.com"), Some("microsoft"));
    assert_eq!(
        provider_from_email("a@contoso.onmicrosoft.com"),
        Some("microsoft")
    );
}

#[test]
fn domain_inference_is_case_insensitive() {
    assert_eq!(provider_from_email("A@Outlook.COM"), Some("microsoft"));
    assert_eq!(provider_from_email("A@Gmail.com"), Some("google"));
}

#[test]
fn unknown_domains_are_not_guessed() {
    assert_eq!(provider_from_email("a@contoso.com"), None);
    assert_eq!(provider_from_email("a@example.org"), None);
    assert_eq!(provider_from_email("not-an-email"), None);
}

#[test]
fn stored_provider_wins_over_domain() {
    let acc = account("user@contoso.com", Some("microsoft"));
    assert_eq!(provider_for(&acc).unwrap().id, "microsoft");

    // A gmail address explicitly marked as Microsoft must not be sent to Google.
    let acc = account("user@gmail.com", Some("microsoft"));
    assert_eq!(provider_for(&acc).unwrap().id, "microsoft");
}

#[test]
fn empty_provider_falls_back_to_domain() {
    for provider in [None, Some(""), Some("   ")] {
        let acc = account("user@outlook.com", provider);
        assert_eq!(provider_for(&acc).unwrap().id, "microsoft");
    }
}

#[test]
fn fails_loudly_for_unknown_provider() {
    let acc = account("user@contoso.com", Some("yahoo"));
    assert!(provider_for(&acc).is_err());

    let acc = account("user@contoso.com", None);
    assert!(provider_for(&acc).is_err());
}

#[test]
fn provider_ids_are_normalized() {
    assert_eq!(config_for_id("Google").unwrap().id, "google");
    assert_eq!(config_for_id("  MICROSOFT  ").unwrap().id, "microsoft");
    assert!(config_for_id("").is_err());
}

#[test]
fn each_provider_uses_its_own_token_endpoint_and_env_vars() {
    let google = config_for_id("google").unwrap();
    assert_eq!(google.token_endpoint, "https://oauth2.googleapis.com/token");
    assert_eq!(google.client_id_env, "PELLIPER_GOOGLE_CLIENT_ID");

    let microsoft = config_for_id("microsoft").unwrap();
    assert_eq!(
        microsoft.token_endpoint,
        "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    );
    assert_eq!(microsoft.client_id_env, "PELLIPER_MICROSOFT_CLIENT_ID");
}
