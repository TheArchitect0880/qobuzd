use thiserror::Error;

#[derive(Error, Debug)]
pub enum QobuzError {
    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("API request failed: {0}")]
    ApiError(String),

    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Token error: {0}")]
    TokenError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Device linking error: {0}")]
    LinkError(String),

    #[error("Crypto error: {0}")]
    CryptoError(String),
}

pub type Result<T> = std::result::Result<T, QobuzError>;
