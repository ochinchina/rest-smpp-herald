use async_trait::async_trait;
use log::warn;
use serde_json::Value;

#[async_trait]
pub trait UserAuthentication: Send + Sync {
    async fn authenticate(&self, system_id: String, password: String) -> bool;
}

pub struct SimpleUserAuthentication {
    valid_users: Vec<(String, String)>,
}

impl SimpleUserAuthentication {
    pub fn new(valid_users: Vec<(String, String)>) -> Self {
        SimpleUserAuthentication { valid_users }
    }
}

#[async_trait]
impl UserAuthentication for SimpleUserAuthentication {
    async fn authenticate(&self, system_id: String, password: String) -> bool {
        self.valid_users
            .iter()
            .any(|(id, pw)| id == &system_id && pw == &password)
    }
}

/// HTTP-backed user authentication that delegates credential checks to an
/// external REST API.
///
/// Sends a POST request with JSON `{"system_id": "...", "password": "..."}`
/// to the configured URL. Authentication succeeds when the response status is
/// 2xx and the JSON body contains `"authenticated": true`.
pub struct HttpUserAuthentication {
    url: String,
}

impl HttpUserAuthentication {
    pub fn new(url: String) -> Self {
        HttpUserAuthentication { url }
    }
}

#[async_trait]
impl UserAuthentication for HttpUserAuthentication {
    async fn authenticate(&self, system_id: String, password: String) -> bool {
        let client = reqwest::Client::new();
        let payload = serde_json::json!({
            "system_id": system_id,
            "password": password,
        });
        match client.post(&self.url).json(&payload).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!(
                        "Auth HTTP request to {} returned status {}",
                        self.url,
                        resp.status()
                    );
                    return false;
                }
                match resp.json::<Value>().await {
                    Ok(body) => body
                        .get("authenticated")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    Err(e) => {
                        warn!("Failed to parse auth response from {}: {}", self.url, e);
                        false
                    }
                }
            }
            Err(e) => {
                warn!("Auth HTTP request to {} failed: {}", self.url, e);
                false
            }
        }
    }
}
