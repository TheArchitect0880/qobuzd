use crate::config::Config;
use crate::crypto;
use crate::error::{QobuzError, Result};
use crate::types::*;
use aes::Aes128;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use cbc::cipher::{block_padding::NoPadding, BlockDecryptMut, KeyIvInit};
use hkdf::Hkdf;
use reqwest::Client;
use sha2::Sha256;
use std::time::Duration;
use tracing::warn;

type Aes128CbcDec = cbc::Decryptor<Aes128>;

#[derive(Debug, Clone)]
pub enum TrackStream {
    DirectUrl {
        url: String,
        sampling_rate_hz: Option<u32>,
        bit_depth: Option<u32>,
        channels: Option<u32>,
    },
    Segmented {
        url_template: String,
        n_segments: u32,
        encryption_key_hex: Option<String>,
        sampling_rate_hz: Option<u32>,
        bit_depth: Option<u32>,
        channels: Option<u32>,
    },
}

fn b64url_decode(s: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(s.trim_end_matches('='))
        .map_err(|e| QobuzError::CryptoError(format!("base64url decode failed: {}", e)))
}

fn derive_track_key_hex(
    session_infos: &str,
    app_secret_hex: &str,
    key_field: &str,
) -> Result<String> {
    let infos_parts: Vec<&str> = session_infos.splitn(2, '.').collect();
    if infos_parts.len() != 2 {
        return Err(QobuzError::CryptoError(format!(
            "invalid session infos format: {}",
            session_infos
        )));
    }

    let salt = b64url_decode(infos_parts[0])?;
    let info = b64url_decode(infos_parts[1])?;
    let ikm = hex::decode(app_secret_hex)
        .map_err(|e| QobuzError::CryptoError(format!("invalid app secret hex: {}", e)))?;

    let hk = Hkdf::<Sha256>::new(Some(&salt), &ikm);
    let mut kek = [0u8; 16];
    hk.expand(&info, &mut kek)
        .map_err(|e| QobuzError::CryptoError(format!("HKDF expand failed: {e:?}")))?;

    let key_parts: Vec<&str> = key_field.splitn(3, '.').collect();
    if key_parts.len() != 3 || key_parts[0] != "qbz-1" {
        return Err(QobuzError::CryptoError(format!(
            "unexpected key field format: {}",
            key_field
        )));
    }

    let ciphertext = b64url_decode(key_parts[1])?;
    let iv_bytes = b64url_decode(key_parts[2])?;
    if ciphertext.len() < 16 || iv_bytes.len() < 16 {
        return Err(QobuzError::CryptoError(format!(
            "invalid key field lengths: ciphertext={} iv={}",
            ciphertext.len(),
            iv_bytes.len()
        )));
    }

    let mut buf = ciphertext;
    let iv: [u8; 16] = iv_bytes[..16]
        .try_into()
        .map_err(|_| QobuzError::CryptoError("invalid IV length".to_string()))?;
    let decrypted = Aes128CbcDec::new(&kek.into(), &iv.into())
        .decrypt_padded_mut::<NoPadding>(&mut buf)
        .map_err(|e| QobuzError::CryptoError(format!("AES-CBC decrypt failed: {e:?}")))?;

    if decrypted.len() < 16 {
        return Err(QobuzError::CryptoError(
            "decrypted track key too short".to_string(),
        ));
    }

    Ok(hex::encode(&decrypted[..16]))
}

#[derive(Clone)]
pub struct QobuzApi {
    client: Client,
    base_url: String,
    app_id: String,
    device_id: String,
    device_name: String,
    session_id: String,
}

impl QobuzApi {
    pub fn new(config: &Config) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("Qobuzd/0.1.0")
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: "https://www.qobuz.com".to_string(),
            app_id: config.app_id.clone(),
            device_id: config.device_id.clone(),
            device_name: config.device_name.clone(),
            session_id: config.session_id.clone(),
        }
    }

    pub fn get_device_id(&self) -> &str {
        &self.device_id
    }

    pub fn get_device_name(&self) -> &str {
        &self.device_name
    }

    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    fn get_timestamp(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn build_auth_headers(&self, access_token: Option<&str>) -> reqwest::header::HeaderMap {
        use reqwest::header::*;

        let mut headers = HeaderMap::new();
        headers.insert("X-Device-Platform", "linux".parse().unwrap());
        headers.insert("X-Device-Model", self.device_name.parse().unwrap());
        headers.insert("X-Device-Manufacturer-Id", self.device_id.parse().unwrap());

        if let Some(token) = access_token {
            let auth_value = format!("Bearer {}", token);
            headers.insert(AUTHORIZATION, auth_value.parse().unwrap());
        }

        headers
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<LoginResponse> {
        let timestamp = self.get_timestamp();

        let signature = crypto::generate_login_signature(email, password, &self.app_id, timestamp);

        let url = format!(
            "{}/api.json/0.2/oauth2/login?username={}&password={}&app_id={}&request_ts={}&request_sig={}",
            self.base_url,
            urlencoding::encode(email),
            urlencoding::encode(password),
            self.app_id,
            timestamp,
            signature
        );

        let response = self.client
            .get(&url)
            .header("User-Agent", "Dalvik/2.1.0 (Linux; U; Android 9; Nexus 6P Build/PQ3A.190801.002) QobuzMobileAndroid/9.7.0.3-b26022717")
            .header("X-App-Id", &self.app_id)
            .header("X-App-Version", "9.7.0.3")
            .header("X-Device-Platform", "android")
            .header("X-Device-Model", "Nexus 6P")
            .header("X-Device-Os-Version", "9")
            .send()
            .await?;

        let status = response.status();

        if !status.is_success() {
            let error: ErrorResponse = response.json().await.unwrap_or_else(|_| ErrorResponse {
                message: Some("Login failed".to_string()),
                code: Some(status.as_u16() as u32),
                status: Some("error".to_string()),
                errors: None,
            });
            return Err(QobuzError::AuthError(
                error.message.unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        let login_response: LoginResponse = response.json().await?;
        Ok(login_response)
    }

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<OAuthTokens> {
        let timestamp = self.get_timestamp();

        let signature = crypto::generate_request_signature(
            "oauth2/token",
            &[
                ("refresh_token", refresh_token),
                ("grant_type", "refresh_token"),
            ],
            timestamp,
        );

        let url = format!(
            "{}/api.json/0.2/oauth2/token?refresh_token={}&grant_type=refresh_token&app_id={}&request_ts={}&request_sig={}",
            self.base_url,
            urlencoding::encode(refresh_token),
            self.app_id,
            timestamp,
            signature
        );

        let response = self.client
            .get(&url)
            .header("User-Agent", "Dalvik/2.1.0 (Linux; U; Android 9; Nexus 6P Build/PQ3A.190801.002) QobuzMobileAndroid/9.7.0.3-b26022717")
            .header("X-App-Id", &self.app_id)
            .header("X-App-Version", "9.7.0.3")
            .header("X-Device-Platform", "android")
            .header("X-Device-Model", "Nexus 6P")
            .header("X-Device-Os-Version", "9")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::AuthError("Token refresh failed".to_string()));
        }

        let tokens: OAuthTokens = response.json().await?;
        Ok(tokens)
    }

    pub async fn get_user(&self, access_token: &str) -> Result<User> {
        let url = format!(
            "{}/api.json/0.2/user/get?app_id={}",
            self.base_url, self.app_id
        );

        let response = self
            .client
            .get(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::ApiError("Failed to get user".to_string()));
        }

        let user: User = response.json().await?;
        Ok(user)
    }

    pub async fn get_link_token(
        &self,
        access_token: &str,
        action: &str,
    ) -> Result<LinkTokenResponse> {
        let timestamp = self.get_timestamp();

        let signature = crypto::generate_request_signature(
            "link/token",
            &[
                ("link_action", action),
                ("external_device_id", &self.device_id),
            ],
            timestamp,
        );

        let url = format!(
            "{}/api.json/0.2/link/token?app_id={}&request_ts={}&request_sig={}",
            self.base_url, self.app_id, timestamp, signature
        );

        let body = LinkTokenRequest {
            link_action: action.to_string(),
            external_device_id: self.device_id.clone(),
        };

        let response = self
            .client
            .post(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::LinkError(
                "Failed to get link token".to_string(),
            ));
        }

        let link_response: LinkTokenResponse = response.json().await?;
        Ok(link_response)
    }

    pub async fn get_device_token(
        &self,
        access_token: &str,
        link_token: &str,
        link_device_id: &str,
    ) -> Result<DeviceTokenResponse> {
        let timestamp = self.get_timestamp();

        let signature = crypto::generate_request_signature(
            "link/device/token",
            &[
                ("link_token", link_token),
                ("link_device_id", link_device_id),
                ("external_device_id", &self.device_id),
            ],
            timestamp,
        );

        let url = format!(
            "{}/api.json/0.2/link/device/token?app_id={}&request_ts={}&request_sig={}",
            self.base_url, self.app_id, timestamp, signature
        );

        let body = DeviceTokenRequest {
            link_token: link_token.to_string(),
            link_device_id: link_device_id.to_string(),
            external_device_id: self.device_id.clone(),
        };

        let response = self
            .client
            .post(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::LinkError(
                "Failed to get device token".to_string(),
            ));
        }

        let device_response: DeviceTokenResponse = response.json().await?;
        Ok(device_response)
    }

    pub async fn get_qws_token(&self, access_token: &str) -> Result<QwsTokenResponse> {
        let timestamp = self.get_timestamp();

        let signature = crypto::generate_request_signature("qws/createToken", &[], timestamp);

        let url = format!(
            "{}/api.json/0.2/qws/createToken?app_id={}&request_ts={}&request_sig={}",
            self.base_url, self.app_id, timestamp, signature
        );

        let response = self
            .client
            .post(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .form(&[("jwt", "jwt_qws")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::ApiError("Failed to get QWS token".to_string()));
        }

        let qws_response: QwsTokenResponse = response.json().await?;
        Ok(qws_response)
    }

    pub async fn get_album(&self, access_token: &str, album_id: &str) -> Result<Album> {
        let url = format!(
            "{}/api.json/0.2/album/get?app_id={}&album_id={}",
            self.base_url, self.app_id, album_id
        );

        let response = self
            .client
            .get(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::ApiError("Failed to get album".to_string()));
        }

        let album: Album = response.json().await?;
        Ok(album)
    }

    async fn start_playback_session(&self, access_token: &str) -> Result<PlaybackSession> {
        let timestamp = self.get_timestamp();
        let signature =
            crypto::generate_request_signature("session/start", &[("profile", "qbz-1")], timestamp);

        let url = format!(
            "{}/api.json/0.2/session/start?app_id={}&request_ts={}&request_sig={}",
            self.base_url, self.app_id, timestamp, signature
        );

        let response = self
            .client
            .post(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .form(&[("profile", "qbz-1")])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(QobuzError::ApiError(format!(
                "Failed to start playback session: {} - {}",
                status, body
            )));
        }

        let session: PlaybackSession = response.json().await?;
        Ok(session)
    }

    async fn get_track_stream_mobile(
        &self,
        access_token: &str,
        track_id: &str,
        format_id: u32,
    ) -> Result<TrackStream> {
        let session = self.start_playback_session(access_token).await?;

        let timestamp = self.get_timestamp();
        let format_id_str = format_id.to_string();
        let signature = crypto::generate_request_signature(
            "file/url",
            &[
                ("format_id", &format_id_str),
                ("intent", "stream"),
                ("track_id", track_id),
            ],
            timestamp,
        );

        let url = format!(
            "{}/api.json/0.2/file/url?app_id={}&track_id={}&format_id={}&intent=stream&request_ts={}&request_sig={}",
            self.base_url, self.app_id, track_id, format_id, timestamp, signature
        );

        let mut headers = self.build_auth_headers(Some(access_token));
        headers.insert(
            "X-Session-Id",
            session.session_id.parse().map_err(|e| {
                QobuzError::ApiError(format!("Invalid X-Session-Id header value: {}", e))
            })?,
        );

        let response = self.client.get(&url).headers(headers).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(QobuzError::ApiError(format!(
                "Failed to get mobile file URL: {} - {}",
                status, body
            )));
        }

        let mut file: FileUrlResponse = response.json().await?;

        if let (Some(key_field), Some(infos)) = (file.key.clone(), session.infos.as_deref()) {
            match derive_track_key_hex(infos, &crypto::APP_SECRET, &key_field) {
                Ok(unwrapped) => file.key = Some(unwrapped),
                Err(e) => {
                    warn!("Failed to unwrap track key for {}: {}", track_id, e);
                    file.key = None;
                }
            }
        }

        if let Some(url) = file.url {
            return Ok(TrackStream::DirectUrl {
                url,
                sampling_rate_hz: file.sampling_rate.map(|v| v.round() as u32),
                bit_depth: file.bit_depth.map(|v| v as u32),
                channels: Some(2),
            });
        }

        if let (Some(url_template), Some(n_segments)) = (file.url_template, file.n_segments) {
            return Ok(TrackStream::Segmented {
                url_template,
                n_segments,
                encryption_key_hex: file.key,
                sampling_rate_hz: file.sampling_rate.map(|v| v.round() as u32),
                bit_depth: file.bit_depth.map(|v| v as u32),
                channels: Some(2),
            });
        }

        Err(QobuzError::ApiError(
            "Mobile file/url response did not contain url or url_template".to_string(),
        ))
    }

    async fn get_track_url_legacy(
        &self,
        access_token: &str,
        track_id: &str,
        format_id: u32,
    ) -> Result<String> {
        let timestamp = self.get_timestamp();
        let format_id_str = format_id.to_string();

        let signature = crypto::generate_request_signature(
            "track/getFileUrl",
            &[
                ("format_id", &format_id_str),
                ("intent", "stream"),
                ("track_id", track_id),
            ],
            timestamp,
        );

        let url = format!(
            "{}/api.json/0.2/track/getFileUrl?app_id={}&track_id={}&format_id={}&intent=stream&request_ts={}&request_sig={}",
            self.base_url, self.app_id, track_id, format_id, timestamp, signature
        );

        let response = self
            .client
            .get(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(QobuzError::ApiError(format!(
                "Failed to get track URL: {} - {}",
                status, body
            )));
        }

        #[derive(serde::Deserialize)]
        struct TrackUrlResponse {
            url: String,
        }

        let url_response: TrackUrlResponse = response.json().await?;
        Ok(url_response.url)
    }

    pub async fn get_track_stream(
        &self,
        access_token: &str,
        track_id: &str,
        format_id: u32,
    ) -> Result<TrackStream> {
        match self
            .get_track_stream_mobile(access_token, track_id, format_id)
            .await
        {
            Ok(stream) => Ok(stream),
            Err(e) => {
                warn!(
                    "Mobile file/url failed for track {} (format {}), falling back to legacy endpoint: {}",
                    track_id, format_id, e
                );
                let url = self
                    .get_track_url_legacy(access_token, track_id, format_id)
                    .await?;
                Ok(TrackStream::DirectUrl {
                    url,
                    sampling_rate_hz: None,
                    bit_depth: None,
                    channels: None,
                })
            }
        }
    }

    pub async fn get_track_url(
        &self,
        access_token: &str,
        track_id: &str,
        format_id: u32,
    ) -> Result<String> {
        match self
            .get_track_stream(access_token, track_id, format_id)
            .await?
        {
            TrackStream::DirectUrl { url, .. } => Ok(url),
            TrackStream::Segmented { .. } => Err(QobuzError::ApiError(
                "Track uses segmented stream; use get_track_stream instead".to_string(),
            )),
        }
    }

    pub async fn get_track(&self, access_token: &str, track_id: &str) -> Result<Track> {
        let timestamp = self.get_timestamp();

        let signature =
            crypto::generate_request_signature("track/get", &[("track_id", track_id)], timestamp);

        let url = format!(
            "{}/api.json/0.2/track/get?app_id={}&track_id={}&request_ts={}&request_sig={}",
            self.base_url, self.app_id, track_id, timestamp, signature
        );

        let response = self
            .client
            .get(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(QobuzError::ApiError(format!(
                "Failed to get track: {} - {}",
                status, body
            )));
        }

        let track: Track = response.json().await?;
        Ok(track)
    }

    pub async fn search(
        &self,
        access_token: &str,
        query: &str,
        search_type: &str,
        limit: u32,
        offset: u32,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/api.json/0.2/search?app_id={}&query={}&type={}&limit={}&offset={}",
            self.base_url,
            self.app_id,
            urlencoding::encode(query),
            search_type,
            limit,
            offset
        );

        let response = self
            .client
            .get(&url)
            .headers(self.build_auth_headers(Some(access_token)))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QobuzError::ApiError("Search failed".to_string()));
        }

        let results: serde_json::Value = response.json().await?;
        Ok(results)
    }
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                    result.push(c);
                }
                _ => {
                    for b in c.to_string().as_bytes() {
                        result.push_str(&format!("%{:02X}", b));
                    }
                }
            }
        }
        result
    }
}
