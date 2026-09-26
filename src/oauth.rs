use crate::types::Account;
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
    let mut params = vec![
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("refresh_token", refresh_token),
    ];
    // Public clients (e.g. the Microsoft loopback flow) must not present a
    // client_secret at all — an empty one is rejected as AADSTS700025.
    if !client_secret.is_empty() {
        params.push(("client_secret", client_secret));
    }

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

/// Token endpoint and client-credential env var names for one OAuth provider.
pub struct ProviderConfig {
    /// Short provider id, matching the UI's `OAuth2` provider ids.
    pub id: &'static str,
    pub token_endpoint: &'static str,
    pub client_id_env: &'static str,
    pub client_secret_env: &'static str,
}

const GOOGLE: ProviderConfig = ProviderConfig {
    id: "google",
    token_endpoint: "https://oauth2.googleapis.com/token",
    client_id_env: "PELLIPER_GOOGLE_CLIENT_ID",
    client_secret_env: "PELLIPER_GOOGLE_CLIENT_SECRET",
};

const MICROSOFT: ProviderConfig = ProviderConfig {
    id: "microsoft",
    token_endpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
    client_id_env: "PELLIPER_MICROSOFT_CLIENT_ID",
    client_secret_env: "PELLIPER_MICROSOFT_CLIENT_SECRET",
};

/// Guess a provider id from an email domain, mirroring the UI's
/// `OAuth2::detectProvider`. Only used when `accounts.provider` is unset.
fn provider_from_email(email: &str) -> Option<&'static str> {
    let domain = email.rsplit('@').next()?.to_ascii_lowercase();
    match domain.as_str() {
        "gmail.com" | "googlemail.com" => Some("google"),
        "outlook.com" | "hotmail.com" => Some("microsoft"),
        d if d.ends_with(".onmicrosoft.com") => Some("microsoft"),
        _ => None,
    }
}

/// Map a provider id to its configuration.
pub fn config_for_id(id: &str) -> Result<&'static ProviderConfig, String> {
    match id.trim().to_ascii_lowercase().as_str() {
        "google" => Ok(&GOOGLE),
        "microsoft" => Ok(&MICROSOFT),
        other => Err(format!("Unsupported OAuth provider: {}", other)),
    }
}

/// Resolve the OAuth provider for an account: the stored `provider` column
/// when present, otherwise inferred from the email domain.
pub fn provider_for(account: &Account) -> Result<&'static ProviderConfig, String> {
    if let Some(provider) = account
        .provider
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        return config_for_id(provider);
    }

    provider_from_email(&account.email)
        .ok_or_else(|| {
            format!(
                "Cannot determine OAuth provider for {} — no provider set and email domain is not a known one",
                account.email
            )
        })
        .and_then(config_for_id)
}

/// Read the client credentials for a provider from the environment.
pub fn client_credentials(provider: &ProviderConfig) -> Result<(String, String), String> {
    let client_id = std::env::var(provider.client_id_env).map_err(|_| {
        format!(
            "{} not set (see ~/.local/share/pelliper/.env)",
            provider.client_id_env
        )
    })?;
    let client_secret = std::env::var(provider.client_secret_env).unwrap_or_default();
    Ok((client_id, client_secret))
}

#[cfg(test)]
#[path = "oauth.test.rs"]
mod tests;
