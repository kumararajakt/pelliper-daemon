use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[allow(dead_code)]
    pub token_type: String,
    /// Seconds until the access token expires (typically 3600).
    pub expires_in: Option<u64>,
    /// A new refresh token may be issued on refresh (Google rotates them).
    pub refresh_token: Option<String>,
}

impl TokenResponse {
    /// Compute the absolute Unix timestamp when this token expires.
    /// Falls back to 1 hour from now if the server didn't specify.
    pub fn expires_at(&self) -> i64 {
        let secs = self.expires_in.unwrap_or(3600);
        chrono::Utc::now().timestamp() + secs as i64
    }
}

/// Refresh an OAuth2 access token using a refresh token.
pub async fn refresh_access_token(
    token_endpoint: &str,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<TokenResponse, Box<dyn std::error::Error + Send + Sync>> {
    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("refresh_token", refresh_token),
    ];

    let client = reqwest::Client::new();
    let resp = client.post(token_endpoint).form(&params).send().await?;

    let status = resp.status();
    let body = resp.text().await?;

    if !status.is_success() {
        return Err(format!("Token refresh failed ({}): {}", status, body).into());
    }

    let token: TokenResponse = serde_json::from_str(&body)?;
    Ok(token)
}
