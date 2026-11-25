use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::ipc::protocol::RpcNotification;
use crate::secrets::{store_secret, AccountSecret};

#[derive(Debug, Clone)]
pub struct OAuthProviderConfig {
    pub auth_url: &'static str,
    pub token_url: &'static str,
    pub scopes: &'static str,
    pub client_id: String,
    pub client_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default = "default_expires")]
    expires_in: i64,
}

fn default_expires() -> i64 {
    3600
}

pub fn provider_config(provider: &str, custom_client_id: Option<&str>) -> Result<OAuthProviderConfig> {
    let p = provider.trim().to_ascii_lowercase();
    match p.as_str() {
        "google" | "gmail" => Ok(OAuthProviderConfig {
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            scopes: "https://mail.google.com/",
            client_id: custom_client_id
                .map(str::to_string)
                .or_else(|| std::env::var("PELLIPER_GOOGLE_CLIENT_ID").ok())
                .unwrap_or_default(),
            client_secret: std::env::var("PELLIPER_GOOGLE_CLIENT_SECRET").ok(),
        }),
        "microsoft" | "outlook" | "office365" => Ok(OAuthProviderConfig {
            auth_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
            token_url: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            scopes: "https://outlook.office.com/IMAP.AccessAsUser.All https://outlook.office.com/SMTP.Send offline_access",
            client_id: custom_client_id
                .map(str::to_string)
                .or_else(|| std::env::var("PELLIPER_MS_CLIENT_ID").ok())
                .unwrap_or_default(),
            client_secret: None,
        }),
        "fastmail" => Ok(OAuthProviderConfig {
            auth_url: "https://api.fastmail.com/oauth/authorize",
            token_url: "https://api.fastmail.com/oauth/refresh",
            scopes: "urn:ietf:params:oauth:scope:mail",
            client_id: custom_client_id
                .map(str::to_string)
                .or_else(|| std::env::var("PELLIPER_FASTMAIL_CLIENT_ID").ok())
                .unwrap_or_default(),
            client_secret: None,
        }),
        other => anyhow::bail!("unsupported OAuth2 provider: {other}"),
    }
}

/// Starts a localhost PKCE callback server and returns `(session_id, auth_url)`.
pub async fn start_pkce_flow(
    provider: &str,
    email: &str,
    custom_client_id: Option<&str>,
    events: broadcast::Sender<RpcNotification>,
) -> Result<(String, String)> {
    let cfg = provider_config(provider, custom_client_id)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    let verifier = random_urlsafe(48);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let state = random_urlsafe(16);
    let session_id = state.clone();

    let auth_url = format!(
        "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&code_challenge={}&code_challenge_method=S256&state={}&login_hint={}&access_type=offline&prompt=consent",
        cfg.auth_url,
        url_encode(&cfg.client_id),
        url_encode(&redirect_uri),
        url_encode(cfg.scopes),
        url_encode(&challenge),
        url_encode(&state),
        url_encode(email),
    );

    let sid = session_id.clone();
    tokio::spawn(async move {
        match wait_and_exchange(listener, &cfg, &redirect_uri, &verifier, &state).await {
            Ok((access, refresh, expires_at)) => {
                info!(session_id = %sid, "OAuth2 PKCE flow succeeded");
                let _ = events.send(RpcNotification::new(
                    "oauth.completed",
                    json!({
                        "session_id": sid,
                        "success": true,
                        "access_token": access,
                        "refresh_token": refresh,
                        "expires_at": expires_at,
                    }),
                ));
            }
            Err(e) => {
                warn!(session_id = %sid, "OAuth2 PKCE flow failed: {e}");
                let _ = events.send(RpcNotification::new(
                    "oauth.completed",
                    json!({
                        "session_id": sid,
                        "success": false,
                        "error": e.to_string(),
                    }),
                ));
            }
        }
    });

    Ok((session_id, auth_url))
}

async fn wait_and_exchange(
    listener: TcpListener,
    cfg: &OAuthProviderConfig,
    redirect_uri: &str,
    verifier: &str,
    expected_state: &str,
) -> Result<(String, String, i64)> {
    let (mut stream, _) = tokio::time::timeout(
        std::time::Duration::from_secs(300),
        listener.accept(),
    )
    .await
    .context("OAuth2 login timed out")??;

    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let path = first_line.split_whitespace().nth(1).unwrap_or("");
    let query = path.split_once('?').map(|x| x.1).unwrap_or("");
    let params = parse_query(query);

    if let Some(err) = params.get("error") {
        let _ = write_http_response(&mut stream, "Authorization failed. You may close this tab.").await;
        anyhow::bail!("OAuth2 provider error: {err}");
    }

    let state = params.get("state").map(String::as_str).unwrap_or("");
    anyhow::ensure!(state == expected_state, "OAuth2 state mismatch");
    let code = params.get("code").context("missing authorization code")?;

    let _ = write_http_response(&mut stream, "Sign-in complete! You can close this tab and return to Pelliper.").await;

    let mut form = vec![
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri),
        ("client_id", cfg.client_id.as_str()),
        ("code_verifier", verifier),
    ];
    if let Some(ref secret) = cfg.client_secret {
        form.push(("client_secret", secret.as_str()));
    }

    let client = reqwest::Client::new();
    let resp = client.post(cfg.token_url).form(&form).send().await?;
    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("token exchange failed: {body}");
    }

    let tok: TokenResponse = resp.json().await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    Ok((
        tok.access_token,
        tok.refresh_token.unwrap_or_default(),
        now + tok.expires_in,
    ))
}

/// Refreshes an OAuth2 access token if within 60s of expiry and updates `org.freedesktop.secrets`.
pub async fn refresh_if_needed(
    email: &str,
    provider: &str,
    secret: AccountSecret,
) -> Result<AccountSecret> {
    let AccountSecret::OAuth2 {
        access_token,
        refresh_token,
        expires_at,
    } = secret
    else {
        return Ok(secret);
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    if expires_at == 0 || expires_at > now + 60 || refresh_token.is_empty() {
        return Ok(AccountSecret::OAuth2 {
            access_token,
            refresh_token,
            expires_at,
        });
    }

    let cfg = provider_config(provider, None)?;
    let mut form = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token.as_str()),
        ("client_id", cfg.client_id.as_str()),
    ];
    if let Some(ref cs) = cfg.client_secret {
        form.push(("client_secret", cs.as_str()));
    }

    let client = reqwest::Client::new();
    let resp = client.post(cfg.token_url).form(&form).send().await?;
    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("OAuth2 refresh failed: {body}");
    }
    let tok: TokenResponse = resp.json().await?;
    let updated = AccountSecret::OAuth2 {
        access_token: tok.access_token,
        refresh_token: tok.refresh_token.unwrap_or(refresh_token),
        expires_at: now + tok.expires_in,
    };
    let _ = store_secret(email, &updated).await;
    Ok(updated)
}

async fn write_http_response(stream: &mut tokio::net::TcpStream, msg: &str) -> Result<()> {
    let body = format!("<html><body style='font-family:sans-serif;padding:2rem;'><h2>Pelliper</h2><p>{msg}</p></body></html>");
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    Ok(())
}

fn random_urlsafe(num_bytes: usize) -> String {
    let mut bytes = vec![0u8; num_bytes];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(&bytes)
}

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_google_and_microsoft_configs() {
        let g = provider_config("google", Some("test-cid")).unwrap();
        assert!(g.auth_url.contains("accounts.google.com"));
        assert_eq!(g.client_id, "test-cid");

        let m = provider_config("microsoft", Some("ms-cid")).unwrap();
        assert!(m.auth_url.contains("login.microsoftonline.com"));
        assert!(m.scopes.contains("offline_access"));
    }
}
