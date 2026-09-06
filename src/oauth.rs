use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[allow(dead_code)]
    pub token_type: String,
    #[allow(dead_code)]
    pub expires_in: Option<u64>,
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
    let resp = client
        .post(token_endpoint)
        .form(&params)
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await?;

    if !status.is_success() {
        return Err(format!("Token refresh failed ({}): {}", status, body).into());
    }

    let token: TokenResponse = serde_json::from_str(&body)?;
    Ok(token)
}
