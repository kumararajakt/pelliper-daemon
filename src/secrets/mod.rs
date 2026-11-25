use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const SERVICE_APP_ID: &str = "com.pelliper.Pelliper";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountSecret {
    Password {
        password: String,
    },
    OAuth2 {
        access_token: String,
        refresh_token: String,
        expires_at: i64,
    },
}

/// Stores or updates credentials for `email` in `org.freedesktop.secrets` (KWallet / GNOME Keyring).
pub async fn store_secret(email: &str, secret: &AccountSecret) -> Result<()> {
    let keyring = oo7::Keyring::new()
        .await
        .context("failed to open org.freedesktop.secrets keyring")?;
    let payload = serde_json::to_vec(secret)?;
    let attrs = HashMap::from([("app", SERVICE_APP_ID), ("email", email)]);
    keyring
        .create_item(&format!("Pelliper ({email})"), &attrs, &payload, true)
        .await
        .context("failed to store secret in keyring")?;
    Ok(())
}

/// Retrieves credentials for `email` from `org.freedesktop.secrets`.
pub async fn load_secret(email: &str) -> Result<Option<AccountSecret>> {
    let keyring = oo7::Keyring::new()
        .await
        .context("failed to open org.freedesktop.secrets keyring")?;
    let attrs = HashMap::from([("app", SERVICE_APP_ID), ("email", email)]);
    let items = keyring.search_items(&attrs).await?;
    let Some(item) = items.into_iter().next() else {
        return Ok(None);
    };
    item.unlock().await?;
    let raw = item.secret().await?;
    let parsed: AccountSecret = serde_json::from_slice(raw.as_bytes())?;
    Ok(Some(parsed))
}

/// Deletes credentials for `email` from `org.freedesktop.secrets`.
pub async fn delete_secret(email: &str) -> Result<()> {
    let keyring = oo7::Keyring::new()
        .await
        .context("failed to open org.freedesktop.secrets keyring")?;
    let attrs = HashMap::from([("app", SERVICE_APP_ID), ("email", email)]);
    keyring.delete(&attrs).await?;
    Ok(())
}
