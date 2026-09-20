use keyring::Entry;

#[derive(Clone)]
pub struct Secrets {
    service_name: String,
}

impl Secrets {
    pub fn new() -> Self {
        Self {
            service_name: "pelliper".to_string(),
        }
    }

    pub async fn get_password(
        &self,
        account: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let service = self.service_name.clone();
        let account = account.to_string();
        tokio::task::spawn_blocking(move || {
            let entry = Entry::new(&service, &account)?;
            Ok(entry.get_password()?)
        })
        .await?
    }
}
