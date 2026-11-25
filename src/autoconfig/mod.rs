use anyhow::{Context, Result};
use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutoconfigResult {
    pub imap_host: String,
    pub imap_port: u16,
    pub imap_tls: String, // "tls" | "starttls"
    pub imap_username: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_tls: String, // "tls" | "starttls"
    pub smtp_username: String,
    pub auth_type: String, // "password" | "oauth2"
    pub oauth_provider: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "clientConfig")]
struct ClientConfig {
    #[serde(rename = "emailProvider")]
    email_provider: EmailProvider,
}

#[derive(Debug, Deserialize)]
struct EmailProvider {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "incomingServer", default)]
    incoming_servers: Vec<ServerEntry>,
    #[serde(rename = "outgoingServer", default)]
    outgoing_servers: Vec<ServerEntry>,
}

#[derive(Debug, Deserialize)]
struct ServerEntry {
    #[serde(rename = "@type")]
    server_type: String,
    hostname: String,
    port: u16,
    #[serde(rename = "socketType")]
    socket_type: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    authentication: Vec<String>,
}

/// Queries Mozilla Autoconfig endpoints for `email` and returns resolved IMAP/SMTP settings if found.
pub async fn lookup_autoconfig(email: &str) -> Result<Option<AutoconfigResult>> {
    let Some((_, domain)) = email.split_once('@') else {
        return Ok(None);
    };
    let domain = domain.trim().to_lowercase();
    if domain.is_empty() {
        return Ok(None);
    }

    let urls = [
        format!("https://autoconfig.{domain}/mail/config-v1.1.xml?emailaddress={email}"),
        format!("https://{domain}/.well-known/autoconfig/mail/config-v1.1.xml"),
        format!("https://autoconfig.thunderbird.net/v1.1/{domain}"),
    ];

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(6))
        .build()?;

    for url in &urls {
        if let Ok(resp) = client.get(url).send().await {
            if resp.status().is_success() {
                if let Ok(xml) = resp.text().await {
                    if let Ok(parsed) = parse_autoconfig_xml(&xml, email) {
                        return Ok(Some(parsed));
                    }
                }
            }
        }
    }

    Ok(None)
}

pub fn parse_autoconfig_xml(xml: &str, email: &str) -> Result<AutoconfigResult> {
    let cfg: ClientConfig = from_str(xml).context("invalid Mozilla autoconfig XML")?;
    let (local_part, domain) = email.split_once('@').unwrap_or((email, ""));

    let imap = cfg
        .email_provider
        .incoming_servers
        .iter()
        .find(|s| s.server_type.eq_ignore_ascii_case("imap"))
        .context("no IMAP server in autoconfig")?;

    let smtp = cfg
        .email_provider
        .outgoing_servers
        .iter()
        .find(|s| s.server_type.eq_ignore_ascii_case("smtp"))
        .context("no SMTP server in autoconfig")?;

    let imap_tls = normalize_socket(&imap.socket_type);
    let smtp_tls = normalize_socket(&smtp.socket_type);

    let (auth_type, oauth_provider) = detect_auth(&imap.authentication, &cfg.email_provider.id, domain);

    Ok(AutoconfigResult {
        imap_host: imap.hostname.clone(),
        imap_port: imap.port,
        imap_tls,
        imap_username: expand_username(&imap.username, email, local_part, domain),
        smtp_host: smtp.hostname.clone(),
        smtp_port: smtp.port,
        smtp_tls,
        smtp_username: expand_username(&smtp.username, email, local_part, domain),
        auth_type,
        oauth_provider,
    })
}

fn normalize_socket(socket_type: &str) -> String {
    if socket_type.eq_ignore_ascii_case("STARTTLS") {
        "starttls".to_string()
    } else {
        "tls".to_string()
    }
}

fn expand_username(tpl: &str, email: &str, local: &str, domain: &str) -> String {
    if tpl.is_empty() {
        return email.to_string();
    }
    tpl.replace("%EMAILADDRESS%", email)
        .replace("%EMAILLOCALPART%", local)
        .replace("%EMAILDOMAIN%", domain)
}

fn detect_auth(methods: &[String], provider_id: &str, domain: &str) -> (String, Option<String>) {
    let has_oauth = methods.iter().any(|m| m.eq_ignore_ascii_case("OAuth2"));
    if has_oauth {
        let provider = if provider_id.contains("google") || domain.contains("gmail") || domain.contains("googlemail") {
            Some("google".to_string())
        } else if provider_id.contains("office365") || provider_id.contains("outlook") || domain.contains("outlook") || domain.contains("hotmail") || domain.contains("live.com") {
            Some("microsoft".to_string())
        } else if provider_id.contains("fastmail") || domain.contains("fastmail") {
            Some("fastmail".to_string())
        } else {
            None
        };
        ("oauth2".to_string(), provider)
    } else {
        ("password".to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mozilla_client_config_v1_1() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<clientConfig version="1.1">
  <emailProvider id="googlemail.com">
    <incomingServer type="imap">
      <hostname>imap.gmail.com</hostname>
      <port>993</port>
      <socketType>SSL</socketType>
      <username>%EMAILADDRESS%</username>
      <authentication>OAuth2</authentication>
      <authentication>password-cleartext</authentication>
    </incomingServer>
    <outgoingServer type="smtp">
      <hostname>smtp.gmail.com</hostname>
      <port>465</port>
      <socketType>SSL</socketType>
      <username>%EMAILADDRESS%</username>
      <authentication>OAuth2</authentication>
    </outgoingServer>
  </emailProvider>
</clientConfig>"#;

        let res = parse_autoconfig_xml(xml, "alice@gmail.com").unwrap();
        assert_eq!(res.imap_host, "imap.gmail.com");
        assert_eq!(res.imap_port, 993);
        assert_eq!(res.imap_tls, "tls");
        assert_eq!(res.smtp_host, "smtp.gmail.com");
        assert_eq!(res.smtp_port, 465);
        assert_eq!(res.auth_type, "oauth2");
        assert_eq!(res.oauth_provider.as_deref(), Some("google"));
        assert_eq!(res.imap_username, "alice@gmail.com");
    }
}
