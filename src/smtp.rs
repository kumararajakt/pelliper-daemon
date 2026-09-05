use lettre::message::Message;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{SmtpTransport, Transport};

pub struct SmtpSender {
    transport: Option<SmtpTransport>,
}

impl SmtpSender {
    pub fn new() -> Self {
        Self { transport: None }
    }

    pub fn connect(
        &mut self,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let creds = Credentials::new(username.to_string(), password.to_string());

        let transport = SmtpTransport::relay(host)?
            .port(port)
            .credentials(creds)
            .build();

        self.transport = Some(transport);
        Ok(())
    }

    pub fn send(&self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(transport) = &self.transport {
            transport.send(&message)?;
        }
        Ok(())
    }
}
