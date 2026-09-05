use crate::mail::MessageInfo;

pub struct ImapConnection {
    connected: bool,
}

impl ImapConnection {
    pub fn new() -> Self {
        Self { connected: false }
    }

    pub async fn connect(
        &mut self,
        _host: &str,
        _port: u16,
        _username: &str,
        _password: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: implement async-imap connection with TLS
        self.connected = true;
        Ok(())
    }

    pub async fn list_folders(&mut self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // TODO: IMAP LIST
        Ok(vec![])
    }

    pub async fn fetch_messages(
        &mut self,
        _folder: &str,
        _limit: u32,
    ) -> Result<Vec<MessageInfo>, Box<dyn std::error::Error>> {
        // TODO: IMAP FETCH
        Ok(vec![])
    }

    pub async fn idle(&mut self, _folder: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // TODO: IMAP IDLE
        Ok(false)
    }
}
