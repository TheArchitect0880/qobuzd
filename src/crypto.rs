use md5::{Digest, Md5};
use sha1::Sha1;

pub const APP_SECRET: &str = "e79f8b9be485692b0e5f9dd895826368";

pub fn md5_hash(input: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn sha1_hash(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_request_signature(
    endpoint: &str,
    params: &[(&str, &str)],
    timestamp: i64,
) -> String {
    let endpoint_clean = endpoint.replace("/", "");

    let mut param_str = params
        .iter()
        .map(|(k, v)| format!("{}{}", k, v))
        .collect::<Vec<_>>();
    param_str.sort();

    let data = format!(
        "{}{}{}{}",
        endpoint_clean,
        param_str.join(""),
        timestamp,
        APP_SECRET
    );
    md5_hash(&data)
}

pub fn generate_login_signature(
    username: &str,
    password: &str,
    _app_id: &str,
    timestamp: i64,
) -> String {
    generate_request_signature(
        "oauth2/login",
        &[("username", username), ("password", password)],
        timestamp,
    )
}

pub fn generate_device_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.gen();
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

pub fn generate_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn generate_client_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_hash() {
        let result = md5_hash("test");
        assert_eq!(result, "098f6bcd4621d373cade4e832627b4f6");
    }

    #[test]
    fn test_request_signature() {
        let sig = generate_request_signature("test", &[("a", "1"), ("b", "2")], 1234567890);
        assert_eq!(sig.len(), 32);
    }
}
