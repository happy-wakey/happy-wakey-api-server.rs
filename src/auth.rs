use axum::http::HeaderMap;
use shared_auth_service_client::{ClientError, SharedAuthClient};

use crate::error::ApiFailure;

#[derive(Clone)]
pub struct SharedAuth {
    client: SharedAuthClient,
    audience: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identity {
    pub subject: String,
    pub roles: Vec<String>,
}

impl SharedAuth {
    pub fn new(
        base: String,
        audience: String,
        service_secret: String,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: SharedAuthClient::try_new(base)?
                .with_service_credential(service_secret)
                .with_max_response_bytes(64 * 1024),
            audience,
        })
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<Identity, ApiFailure> {
        let token = bearer(headers).ok_or_else(ApiFailure::unauthorized)?;
        self.authenticate_token(token).await
    }

    /// Re-authenticate a bearer for every non-HTTP transport operation.
    ///
    /// Persistent connections and durable message delivery are transport
    /// optimizations only; they never cache or confer identity.
    pub async fn authenticate_token(&self, token: &str) -> Result<Identity, ApiFailure> {
        let result = self
            .client
            .introspect_with_requirements(token, &self.audience, &[])
            .await
            .map_err(map_client_error)?;
        let subject = result
            .sub
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(ApiFailure::unauthorized)?;
        if !result.active {
            return Err(ApiFailure::unauthorized());
        }
        Ok(Identity {
            subject,
            roles: result.roles,
        })
    }
}

fn map_client_error(error: ClientError) -> ApiFailure {
    match error {
        ClientError::Unauthorized | ClientError::InvalidInput(_) => ApiFailure::unauthorized(),
        ClientError::MissingServiceCredential
        | ClientError::InvalidBaseUrl
        | ClientError::RequestTooLarge { .. }
        | ClientError::ResponseTooLarge { .. }
        | ClientError::Encode { .. }
        | ClientError::Decode { .. }
        | ClientError::Transport(_)
        | ClientError::Status(_)
        | ClientError::InsecureTransport(_) => ApiFailure::auth_unavailable(),
    }
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && token.len() <= 16 * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_is_strict_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic x".parse().unwrap());
        assert_eq!(bearer(&headers), None);
        headers.insert("authorization", "Bearer abc".parse().unwrap());
        assert_eq!(bearer(&headers), Some("abc"));
        let oversized = format!("Bearer {}", "x".repeat(16 * 1024 + 1));
        headers.insert("authorization", oversized.parse().unwrap());
        assert_eq!(bearer(&headers), None);
    }
}
