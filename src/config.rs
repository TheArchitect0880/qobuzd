use crate::crypto;
use crate::error::{QobuzError, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub app_id: String,
    pub device_id: String,
    pub session_id: String,
    pub device_name: String,
    pub cache_dir: PathBuf,
    pub config_dir: PathBuf,
    pub credentials: Option<StoredCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub user_id: Option<u64>,
    pub expires_at: Option<i64>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceLinkCredentials {
    pub device_access_token: String,
    pub device_refresh_token: String,
    pub device_id: String,
    pub link_device_id: String,
    pub expires_at: Option<i64>,
}

impl Config {
    pub fn new(device_name: String) -> Result<Self> {
        let proj_dirs = ProjectDirs::from("com", "qobuz", "qobuzd").ok_or_else(|| {
            QobuzError::ConfigError("Could not determine config directory".into())
        })?;

        let config_dir = proj_dirs.config_dir().to_path_buf();
        let cache_dir = proj_dirs.cache_dir().to_path_buf();

        std::fs::create_dir_all(&config_dir)?;
        std::fs::create_dir_all(&cache_dir)?;

        let device_id = crypto::generate_device_id();
        let session_id = crypto::generate_session_id();

        Ok(Self {
            app_id: "312369995".to_string(),
            device_id,
            session_id,
            device_name,
            cache_dir,
            config_dir,
            credentials: None,
        })
    }

    pub fn load() -> Result<Self> {
        let proj_dirs = ProjectDirs::from("com", "qobuz", "qobuzd").ok_or_else(|| {
            QobuzError::ConfigError("Could not determine config directory".into())
        })?;

        let config_path = proj_dirs.config_dir().join("config.json");

        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let config: Config = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            Self::new("qobuzd".to_string())
        }
    }

    pub fn save(&self) -> Result<()> {
        let config_path = self.config_dir.join("config.json");
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(config_path, content)?;
        Ok(())
    }

    pub fn store_credentials(&mut self, creds: StoredCredentials) -> Result<()> {
        self.credentials = Some(creds);
        self.save()
    }

    pub fn clear_credentials(&mut self) -> Result<()> {
        self.credentials = None;
        self.save()
    }

    pub fn credentials_path(&self) -> PathBuf {
        self.config_dir.join("credentials.enc")
    }

    pub fn device_link_credentials_path(&self) -> PathBuf {
        self.config_dir.join("device_link.json")
    }
}
