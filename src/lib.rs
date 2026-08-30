pub mod async_operations;
pub mod auth;
pub mod entity;
pub mod error;
pub mod flags;
pub mod handlers;
pub mod nats;
pub mod operation;
pub mod reducer;
pub mod scheduler;
pub mod tcp;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use next_loggers::{Logger, Options};
use sea_orm::{Database, DatabaseConnection};
use tower_http::trace::TraceLayer;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub bind: String,
    pub shared_auth_base: String,
    pub shared_auth_audience: String,
    pub introspect_secret: String,
    pub async_operations_enabled: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: flags::var("DATABASE_URL").context("DATABASE_URL is required")?,
            bind: flags::var("HAPPY_WAKEY_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into()),
            shared_auth_base: flags::var("HAPPY_WAKEY_SHARED_AUTH_BASE")
                .context("HAPPY_WAKEY_SHARED_AUTH_BASE is required")?,
            shared_auth_audience: flags::var("HAPPY_WAKEY_SHARED_AUTH_AUDIENCE")
                .unwrap_or_else(|_| "happy-wakey".into()),
            introspect_secret: flags::var("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET")
                .context("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET is required")?
                .trim()
                .to_owned(),
            async_operations_enabled: flags::var("HAPPY_WAKEY_NATS_URL")
                .ok()
                .is_some_and(|value| !value.trim().is_empty()),
        })
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.introspect_secret.is_empty(),
            "introspection secret is empty"
        );
        let shared_auth = reqwest::Url::parse(&self.shared_auth_base)
            .context("Shared Auth base URL is invalid")?;
        anyhow::ensure!(
            is_safe_https_service_url(&self.shared_auth_base),
            "Shared Auth must use HTTPS to a DNS name, not a public IP"
        );
        anyhow::ensure!(
            shared_auth.host_str().is_some(),
            "Shared Auth host is missing"
        );
        anyhow::ensure!(
            shared_auth.username().is_empty() && shared_auth.password().is_none(),
            "Shared Auth base URL must not contain credentials"
        );
        anyhow::ensure!(
            shared_auth.query().is_none() && shared_auth.fragment().is_none(),
            "Shared Auth base URL must not contain a query or fragment"
        );
        Ok(())
    }
}

fn is_safe_https_service_url(raw: &str) -> bool {
    let Ok(url) = url::Url::parse(raw) else {
        return false;
    };
    if url.scheme() != "https" || url.username() != "" || url.password().is_some() {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    if matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return true;
    }
    host.parse::<std::net::IpAddr>().is_err() && !host.contains(':')
}

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseConnection,
    pub auth: auth::SharedAuth,
    pub telemetry: Arc<Logger>,
    pub async_operations_enabled: bool,
}

impl AppState {
    pub async fn connect(config: &Config) -> Result<Self> {
        config.validate()?;
        let db = Database::connect(&config.database_url)
            .await
            .context("connect to PostgreSQL/CockroachDB")?;
        Ok(Self {
            db,
            auth: auth::SharedAuth::new(
                config.shared_auth_base.clone(),
                config.shared_auth_audience.clone(),
                config.introspect_secret.clone(),
            )
            .context("build fail-closed Shared Auth client")?,
            telemetry: Arc::new(Logger::new(Options {
                app_name: "happy-wakey-api-server".into(),
                ..Options::default()
            })),
            async_operations_enabled: config.async_operations_enabled,
        })
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health))
        .route(
            "/v1/alarms",
            get(handlers::list_alarms).post(handlers::create_alarm),
        )
        .route(
            "/v1/occurrences/{occurrence_id}/transitions",
            post(handlers::transition_occurrence),
        )
        .route("/v1/async-operations", post(async_operations::register))
        .route("/v1/sync/pull", get(handlers::pull_changes))
        .route("/v1/sync/push", post(handlers::push_changes))
        .layer(request_body_limit())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn request_body_limit() -> DefaultBodyLimit {
    DefaultBodyLimit::max(operation::MAX_REQUEST_BYTES)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        extract::Json,
        http::{Request, StatusCode},
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    fn config(shared_auth_base: &str) -> Config {
        Config {
            database_url: "postgres://localhost/happy_wakey".into(),
            bind: "127.0.0.1:0".into(),
            shared_auth_base: shared_auth_base.into(),
            shared_auth_audience: "happy-wakey".into(),
            introspect_secret: "test-only-service-secret".into(),
            async_operations_enabled: false,
        }
    }

    fn test_router() -> Router {
        Router::new()
            .route("/", post(|Json(_): Json<Value>| async {}))
            .layer(request_body_limit())
    }

    #[tokio::test]
    async fn request_body_limit_rejects_oversized_json_before_handlers() {
        let body = format!(
            "{{\"payload\":\"{}\"}}",
            "x".repeat(operation::MAX_REQUEST_BYTES)
        );
        let response = test_router()
            .oneshot(
                Request::post("/")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn request_body_limit_allows_small_json() {
        let response = test_router()
            .oneshot(
                Request::post("/")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload":"ok"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn shared_auth_urls_fail_closed_without_public_ips() {
        assert!(is_safe_https_service_url("https://auth.oresoftware.dev"));
        assert!(!is_safe_https_service_url("http://auth.oresoftware.dev"));
        assert!(!is_safe_https_service_url("https://98.90.186.114"));
        assert!(!is_safe_https_service_url("https://[2001:db8::1]/"));
        assert!(!is_safe_https_service_url(
            "https://user:pass@auth.oresoftware.dev"
        ));
        assert!(is_safe_https_service_url("https://127.0.0.1/"));
    }

    #[test]
    fn shared_auth_base_is_https_and_credential_free() {
        assert!(config("https://auth.example.test").validate().is_ok());
        assert!(config("http://auth.example.test").validate().is_err());
        assert!(config("https://user:password@auth.example.test")
            .validate()
            .is_err());
        assert!(config("https://auth.example.test?redirect=elsewhere")
            .validate()
            .is_err());
        assert!(config("https://").validate().is_err());
        assert!(config("https://98.90.186.114").validate().is_err());
        assert!(config("https://[2001:db8::1]/").validate().is_err());
    }
}
