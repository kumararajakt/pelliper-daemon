use keyring::Entry;

pub struct Secrets {
    service_name: String,
}

impl Secrets {
    pub fn new() -> Self {
        Self {
            service_name: "pelliper".to_string(),
        }
    }

    pub fn store_password(&self, account: &str, password: &str) -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::new(&self.service_name, account)?;
        entry.set_password(password)?;
        Ok(())
    }

    pub fn get_password(&self, account: &str) -> Result<String, Box<dyn std::error::Error>> {
        let entry = Entry::new(&self.service_name, account)?;
        Ok(entry.get_password()?)
    }

    pub fn delete_password(&self, account: &str) -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::new(&self.service_name, account)?;
        entry.delete_credential()?;
        Ok(())
    }
}
