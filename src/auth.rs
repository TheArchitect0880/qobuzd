use crate::api::QobuzApi;
use crate::config::{Config, DeviceLinkCredentials};
use crate::error::{QobuzError, Result};
use crate::token::TokenManager;
use crate::types::{DeviceTokenResponse, LinkTokenResponse, User};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct QobuzAuth {
    api: QobuzApi,
    token_manager: TokenManager,
    access_token: Arc<Mutex<Option<String>>>,
}

impl QobuzAuth {
    pub fn new(config: Config) -> Self {
        let api = QobuzApi::new(&config);
        let token_manager = TokenManager::new(config.clone());

        Self {
            api,
            token_manager,
            access_token: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn login_with_credentials(&self, email: &str, password: &str) -> Result<User> {
        let response = self.api.login(email, password).await?;

        let tokens = response.oauth;
        self.token_manager.store_tokens(&tokens)?;

        {
            let mut token = self.access_token.lock().await;
            *token = Some(tokens.access_token.clone());
        }

        Ok(response.user)
    }

    pub async fn refresh_access_token(&self) -> Result<()> {
        let config =
            crate::config::Config::load().map_err(|e| QobuzError::ConfigError(e.to_string()))?;

        if let Some(creds) = config.credentials {
            let new_tokens = self.api.refresh_token(&creds.refresh_token).await?;
            self.token_manager.store_tokens(&new_tokens)?;

            let mut token = self.access_token.lock().await;
            *token = Some(new_tokens.access_token);
        } else {
            return Err(QobuzError::TokenError(
                "No refresh token available".to_string(),
            ));
        }

        Ok(())
    }

    pub async fn get_valid_token(&self) -> Result<String> {
        if let Some(token) = self.access_token.lock().await.clone() {
            return Ok(token);
        }

        if let Some(tokens) = self.token_manager.load_tokens()? {
            let mut token = self.access_token.lock().await;
            *token = Some(tokens.access_token.clone());
            return Ok(tokens.access_token);
        }

        Err(QobuzError::TokenError(
            "No valid token available".to_string(),
        ))
    }

    pub async fn logout(&self) -> Result<()> {
        let mut token = self.access_token.lock().await;
        *token = None;

        let mut config =
            crate::config::Config::load().map_err(|e| QobuzError::ConfigError(e.to_string()))?;
        config.clear_credentials()?;

        Ok(())
    }

    pub async fn start_device_linking(&self) -> Result<LinkTokenResponse> {
        let token = self.get_valid_token().await?;
        let response = self.api.get_link_token(&token, "signIn").await?;
        Ok(response)
    }

    pub async fn get_device_token(
        &self,
        link_token: &str,
        link_device_id: &str,
    ) -> Result<DeviceTokenResponse> {
        let token = self.get_valid_token().await?;
        let response = self
            .api
            .get_device_token(&token, link_token, link_device_id)
            .await?;

        if let Some(oauth) = &response.oauth {
            let creds = DeviceLinkCredentials {
                device_access_token: oauth.access_token.clone(),
                device_refresh_token: oauth.refresh_token.clone(),
                device_id: self.api.get_device_id().to_string(),
                link_device_id: link_device_id.to_string(),
                expires_at: None,
            };
            self.token_manager.store_device_link_credentials(creds)?;
        }

        Ok(response)
    }

    pub async fn check_device_link_status(
        &self,
        link_token: &str,
        link_device_id: &str,
    ) -> Result<DeviceTokenResponse> {
        self.get_device_token(link_token, link_device_id).await
    }

    pub fn get_device_id(&self) -> &str {
        self.api.get_device_id()
    }

    pub fn get_device_name(&self) -> &str {
        self.api.get_device_name()
    }

    pub async fn is_linked(&self) -> bool {
        self.token_manager
            .load_device_link_credentials()
            .map(|c| c.is_some())
            .unwrap_or(false)
    }

    pub async fn unlink_device(&self) -> Result<()> {
        self.token_manager.clear_device_link_credentials()
    }
}
