use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::error::ApiFailure;

#[derive(Clone)]
pub struct SharedAuth {
    http: reqwest::Client,
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

impl SharedAuth {
    pub fn new(
        http: reqwest::Client,
        base: String,
        audience: String,
        service_secret: String,
    ) -> Self {
        Self {
            http,
            base,
            audience,
            service_secret,
        }
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<Identity, ApiFailure> {
        let token = bearer(headers).ok_or_else(ApiFailure::unauthorized)?;
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
            return Err(if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                ApiFailure::unauthorized()
            } else {
                ApiFailure::auth_unavailable()
            });
        }
        let result: IntrospectionResponse = response
            .json()
            .await
            .map_err(|_| ApiFailure::auth_unavailable())?;
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
    }
}
