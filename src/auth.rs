use axum::http::HeaderMap;
use futures_util::StreamExt;
use reqwest::{redirect::Policy, Client, StatusCode};
use serde::{Deserialize, Serialize};

use crate::error::ApiFailure;

#[derive(Clone)]
pub struct SharedAuth {
    http: Client,
    base: String,
    audience: String,
    service_secret: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identity {
    pub subject: String,
    pub roles: Vec<String>,
}

#[derive(Serialize)]
struct Envelope<'a> {
    contract: &'static str,
    payload: IntrospectionRequest<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IntrospectionRequest<'a> {
    token: &'a str,
    audience: &'a str,
    required_scopes: [String; 0],
}

#[derive(Deserialize)]
struct IntrospectionResponse {
    active: bool,
    #[serde(default)]
    sub: String,
    #[serde(default)]
    roles: Vec<String>,
}

const MAX_INTROSPECTION_RESPONSE_BYTES: usize = 64 * 1024;

impl SharedAuth {
    pub fn new(
        base: String,
        audience: String,
        service_secret: String,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: Client::builder().redirect(Policy::none()).build()?,
            base,
            audience,
            service_secret,
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
        let response = self
            .http
            .post(format!(
                "{}/auth/introspect",
                self.base.trim_end_matches('/')
            ))
            .bearer_auth(&self.service_secret)
            .json(&Envelope {
                contract: "IntrospectionRequest",
                payload: IntrospectionRequest {
                    token,
                    audience: &self.audience,
                    required_scopes: [],
                },
            })
            .send()
            .await
            .map_err(|_| ApiFailure::auth_unavailable())?;
        if !response.status().is_success() {
            return Err(if response.status() == StatusCode::UNAUTHORIZED {
                ApiFailure::unauthorized()
            } else {
                ApiFailure::auth_unavailable()
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_INTROSPECTION_RESPONSE_BYTES as u64)
        {
            return Err(ApiFailure::auth_unavailable());
        }
        let mut body = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| ApiFailure::auth_unavailable())?;
            if body.len().saturating_add(chunk.len()) > MAX_INTROSPECTION_RESPONSE_BYTES {
                return Err(ApiFailure::auth_unavailable());
            }
            body.extend_from_slice(&chunk);
        }
        let result: IntrospectionResponse =
            serde_json::from_slice(&body).map_err(|_| ApiFailure::auth_unavailable())?;
        if !result.active || result.sub.trim().is_empty() {
            return Err(ApiFailure::unauthorized());
        }
        Ok(Identity {
            subject: result.sub,
            roles: result.roles,
        })
    }
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| {
            !token.is_empty()
                && token.len() <= 16 * 1024
                && !token
                    .bytes()
                    .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        })
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
        headers.insert("authorization", "Bearer abc def".parse().unwrap());
        assert_eq!(bearer(&headers), None);
        let oversized = format!("Bearer {}", "x".repeat(16 * 1024 + 1));
        headers.insert("authorization", oversized.parse().unwrap());
        assert_eq!(bearer(&headers), None);
    }
}
