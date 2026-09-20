use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;

pub struct Autodiscovery {
    cache: HashMap<String, ServerConfig>,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub imap_host: String,
    pub imap_port: u16,
    pub imap_security: Security,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_security: Security,
    pub username_template: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Security {
    Ssl,
    StartTls,
    None,
}

impl Autodiscovery {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn discover(&mut self, email: &str) -> Option<ServerConfig> {
        let domain = email.split('@').last()?;

        if let Some(cached) = self.cache.get(domain) {
            return Some(cached.clone());
        }

        // Try ISPDB first
        if let Some(config) = self.try_ispdb(domain).await {
            self.cache.insert(domain.to_string(), config.clone());
            return Some(config);
        }

        // Try well-known paths
        if let Some(config) = self.try_well_known(domain).await {
            self.cache.insert(domain.to_string(), config.clone());
            return Some(config);
        }

        None
    }

    async fn try_ispdb(&self, domain: &str) -> Option<ServerConfig> {
        let url = format!("https://autoconfig.thunderbird.net/v1.1/{}", domain);
        let response = self.client.get(&url).send().await.ok()?;
        let xml = response.text().await.ok()?;
        parse_autoconfig_xml(&xml)
    }

    async fn try_well_known(&self, domain: &str) -> Option<ServerConfig> {
        let urls = [
            format!("https://mail.{}/.well-known/autoconfig", domain),
            format!("https://autoconfig.{}/.well-known/autoconfig", domain),
            format!("https://{}.well-known.autoconfig", domain),
        ];

        for url in &urls {
            if let Ok(response) = self.client.get(url.as_str()).send().await {
                if let Ok(xml) = response.text().await {
                    if let Some(config) = parse_autoconfig_xml(&xml) {
                        return Some(config);
                    }
                }
            }
        }
        None
    }
}

fn parse_autoconfig_xml(xml: &str) -> Option<ServerConfig> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();

    let mut imap_host: Option<String> = None;
    let mut imap_port: Option<u16> = None;
    let mut imap_socket: Option<Security> = None;
    let mut _imap_username: Option<String> = None;

    let mut smtp_host: Option<String> = None;
    let mut smtp_port: Option<u16> = None;
    let mut smtp_socket: Option<Security> = None;
    let mut _smtp_username: Option<String> = None;

    let mut in_incoming = false;
    let mut in_outgoing = false;
    let mut in_hostname = false;
    let mut in_port = false;
    let mut in_socket = false;
    let mut in_username = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref()).unwrap_or("");

                match name_str {
                    "incomingServer" => {
                        if get_attr(e, "type") == "imap" {
                            in_incoming = true;
                        }
                    }
                    "outgoingServer" => {
                        if get_attr(e, "type") == "smtp" {
                            in_outgoing = true;
                        }
                    }
                    "hostname" if in_incoming || in_outgoing => in_hostname = true,
                    "port" if in_incoming || in_outgoing => in_port = true,
                    "socketType" if in_incoming || in_outgoing => in_socket = true,
                    "username" if in_incoming || in_outgoing => in_username = true,
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref()).unwrap_or("");

                match name_str {
                    "incomingServer" => in_incoming = false,
                    "outgoingServer" => in_outgoing = false,
                    "hostname" => in_hostname = false,
                    "port" => in_port = false,
                    "socketType" => in_socket = false,
                    "username" => in_username = false,
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().ok()?.to_string();
                if in_hostname {
                    if in_incoming {
                        imap_host = Some(text);
                    } else if in_outgoing {
                        smtp_host = Some(text);
                    }
                } else if in_port {
                    if in_incoming {
                        imap_port = text.parse().ok();
                    } else if in_outgoing {
                        smtp_port = text.parse().ok();
                    }
                } else if in_socket {
                    let sec = parse_socket_type(&text);
                    if in_incoming {
                        imap_socket = Some(sec);
                    } else if in_outgoing {
                        smtp_socket = Some(sec);
                    }
                } else if in_username {
                    if in_incoming {
                        _imap_username = Some(text);
                    } else if in_outgoing {
                        _smtp_username = Some(text);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    Some(ServerConfig {
        imap_host: imap_host?,
        imap_port: imap_port.unwrap_or(993),
        imap_security: imap_socket.unwrap_or(Security::Ssl),
        smtp_host: smtp_host?,
        smtp_port: smtp_port.unwrap_or(587),
        smtp_security: smtp_socket.unwrap_or(Security::StartTls),
        username_template: _imap_username.unwrap_or_else(|| "%EMAILLOCALPART%".to_string()),
    })
}

fn get_attr(e: &quick_xml::events::BytesStart, name: &str) -> String {
    for attr in e.attributes().flatten() {
        if attr.key.as_ref() == name.as_bytes() {
            return String::from_utf8_lossy(&attr.value).to_string();
        }
    }
    String::new()
}

fn parse_socket_type(s: &str) -> Security {
    match s.to_uppercase().as_str() {
        "SSL" | "TLS" => Security::Ssl,
        "STARTTLS" => Security::StartTls,
        _ => Security::None,
    }
}

impl Security {
    pub fn imap_default_port(&self) -> u16 {
        match self {
            Security::Ssl => 993,
            Security::StartTls | Security::None => 143,
        }
    }

    pub fn smtp_default_port(&self) -> u16 {
        match self {
            Security::Ssl => 465,
            Security::StartTls | Security::None => 587,
        }
    }
}
