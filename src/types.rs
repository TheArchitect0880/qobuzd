use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub token_type: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: u64,
    #[serde(rename = "publicId")]
    pub public_id: Option<String>,
    pub email: String,
    pub login: String,
    #[serde(rename = "firstname")]
    pub first_name: Option<String>,
    #[serde(rename = "lastname")]
    pub last_name: Option<String>,
    #[serde(rename = "display_name")]
    pub display_name: Option<String>,
    #[serde(rename = "country_code")]
    pub country_code: Option<String>,
    #[serde(rename = "language_code")]
    pub language_code: Option<String>,
    pub zone: Option<String>,
    pub store: Option<String>,
    pub country: Option<String>,
    pub avatar: Option<String>,
    pub subscription: Option<Subscription>,
    pub credential: Option<Credential>,
    #[serde(rename = "store_features")]
    pub store_features: Option<StoreFeatures>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub offer: String,
    pub periodicity: String,
    #[serde(rename = "start_date")]
    pub start_date: Option<String>,
    #[serde(rename = "end_date")]
    pub end_date: Option<String>,
    #[serde(rename = "is_canceled")]
    pub is_canceled: bool,
    #[serde(rename = "household_size_max")]
    pub household_size_max: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    pub id: u64,
    pub label: String,
    pub description: Option<String>,
    pub parameters: Option<CredentialParameters>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialParameters {
    #[serde(rename = "lossy_streaming")]
    pub lossy_streaming: Option<bool>,
    #[serde(rename = "lossless_streaming")]
    pub lossless_streaming: Option<bool>,
    #[serde(rename = "hires_streaming")]
    pub hires_streaming: Option<bool>,
    #[serde(rename = "hires_purchases_streaming")]
    pub hires_purchases_streaming: Option<bool>,
    #[serde(rename = "mobile_streaming")]
    pub mobile_streaming: Option<bool>,
    #[serde(rename = "offline_streaming")]
    pub offline_streaming: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreFeatures {
    pub download: bool,
    pub streaming: bool,
    pub editorial: bool,
    pub club: bool,
    pub wallet: bool,
    pub weeklyq: bool,
    pub autoplay: bool,
    #[serde(rename = "inapp_purchase_subscripton")]
    pub inapp_purchase_subscription: bool,
    pub opt_in: bool,
    #[serde(rename = "pre_register_opt_in")]
    pub pre_register_opt_in: bool,
    #[serde(rename = "pre_register_zipcode")]
    pub pre_register_zipcode: bool,
    #[serde(rename = "music_import")]
    pub music_import: bool,
    pub radio: bool,
    #[serde(rename = "stream_purchase")]
    pub stream_purchase: bool,
    pub lyrics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub user: User,
    #[serde(rename = "oauth2")]
    pub oauth: OAuthTokens,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkTokenRequest {
    #[serde(rename = "link_action")]
    pub link_action: String,
    #[serde(rename = "external_device_id")]
    pub external_device_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkTokenResponse {
    pub status: String,
    #[serde(rename = "link_token")]
    pub link_token: Option<String>,
    #[serde(rename = "link_device_id")]
    pub link_device_id: Option<String>,
    pub errors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceTokenRequest {
    #[serde(rename = "link_token")]
    pub link_token: String,
    #[serde(rename = "link_device_id")]
    pub link_device_id: String,
    #[serde(rename = "external_device_id")]
    pub external_device_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceTokenResponse {
    pub status: String,
    #[serde(rename = "oauth2")]
    pub oauth: Option<OAuthTokens>,
    #[serde(rename = "link_action")]
    pub link_action: Option<String>,
    pub errors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QwsTokenResponse {
    #[serde(rename = "jwt_qws")]
    pub jwt_qws: QwsToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QwsToken {
    pub exp: i64,
    pub jwt: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub status: Option<String>,
    pub data: Option<T>,
    pub message: Option<String>,
    pub code: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: Option<String>,
    pub code: Option<u32>,
    pub status: Option<String>,
    pub errors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Album {
    pub id: String,
    pub title: String,
    pub version: Option<String>,
    #[serde(rename = "track_count")]
    pub track_count: Option<u32>,
    pub duration: Option<u32>,
    pub image: Option<AlbumImage>,
    pub artists: Option<Vec<Artist>>,
    pub label: Option<Label>,
    pub genre: Option<Genre>,
    #[serde(rename = "audio_info")]
    pub audio_info: Option<AudioInfo>,
    pub rights: Option<Rights>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlbumImage {
    pub small: Option<String>,
    pub thumbnail: Option<String>,
    pub large: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artist {
    pub id: u64,
    pub name: String,
    pub roles: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Label {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Genre {
    pub id: u64,
    pub name: String,
    pub path: Option<Vec<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioInfo {
    #[serde(rename = "maximum_sampling_rate")]
    pub maximum_sampling_rate: Option<f64>,
    #[serde(rename = "maximum_bit_depth")]
    pub maximum_bit_depth: Option<u32>,
    #[serde(rename = "maximum_channel_count")]
    pub maximum_channel_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rights {
    pub purchasable: Option<bool>,
    pub streamable: Option<bool>,
    pub downloadable: Option<bool>,
    #[serde(rename = "hires_streamable")]
    pub hires_streamable: Option<bool>,
    #[serde(rename = "hires_purchasable")]
    pub hires_purchasable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub id: u64,
    pub title: String,
    pub performer: Option<Artist>,
    pub duration: Option<u32>,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub artists: Option<Vec<Artist>>,
    pub album: Option<Album>,
    #[serde(rename = "audio_info")]
    pub audio_info: Option<AudioInfo>,
    pub rights: Option<Rights>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackSession {
    pub session_id: String,
    pub expires_at: Option<u64>,
    pub infos: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUrlResponse {
    pub track_id: Option<i64>,
    pub duration: Option<f64>,
    pub url: Option<String>,
    pub url_template: Option<String>,
    pub n_segments: Option<u32>,
    pub format_id: Option<i32>,
    pub mime_type: Option<String>,
    pub sampling_rate: Option<f64>,
    pub bit_depth: Option<i32>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Playlist {
    pub id: u64,
    pub name: String,
    pub description: Option<String>,
    pub tracks_count: Option<u32>,
    pub image: Option<AlbumImage>,
    pub user: Option<User>,
}
