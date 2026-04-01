use crate::config::{Config, DeviceLinkCredentials};
use crate::error::Result;
use crate::types::OAuthTokens;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct TokenManager {
    config: Config,
}

impl TokenManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn store_device_link_credentials(&self, creds: DeviceLinkCredentials) -> Result<()> {
        let path = self.config.device_link_credentials_path();
        let content = serde_json::to_string_pretty(&creds)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn load_device_link_credentials(&self) -> Result<Option<DeviceLinkCredentials>> {
        let path = self.config.device_link_credentials_path();
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            let creds: DeviceLinkCredentials = serde_json::from_str(&content)?;
            if let Some(expires_at) = creds.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                if expires_at > now {
                    return Ok(Some(creds));
                }
            }
            Ok(Some(creds))
        } else {
            Ok(None)
        }
    }

    pub fn clear_device_link_credentials(&self) -> Result<()> {
        let path = self.config.device_link_credentials_path();
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    pub fn store_tokens(&self, tokens: &OAuthTokens) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let expires_at = now + tokens.expires_in as i64;

        let creds = crate::config::StoredCredentials {
            access_token: tokens.access_token.clone(),
            refresh_token: tokens.refresh_token.clone(),
            user_id: None,
            expires_at: Some(expires_at),
            email: None,
        };

        let mut config = self.config.clone();
        config.store_credentials(creds)?;

        Ok(())
    }

    pub fn load_tokens(&self) -> Result<Option<OAuthTokens>> {
        let config = crate::config::Config::load()?;

        if let Some(creds) = config.credentials {
            if let Some(expires_at) = creds.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;

                if expires_at > now {
                    return Ok(Some(OAuthTokens {
                        token_type: "bearer".to_string(),
                        access_token: creds.access_token,
                        refresh_token: creds.refresh_token,
                        expires_in: (expires_at - now) as u64,
                    }));
                }
            }
        }

        Ok(None)
    }

    pub fn is_token_expired(&self) -> Result<bool> {
        if let Some(tokens) = self.load_tokens()? {
            Ok(tokens.expires_in == 0)
        } else {
            Ok(true)
        }
    }
}
